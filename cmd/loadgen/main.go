package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

type createResponse struct {
	StreamID    string `json:"stream_id"`
	WebSocket   string `json:"websocket_url"`
	AttachToken string `json:"attach_token"`
}

type stats struct {
	attempted   atomic.Int64
	connected   atomic.Int64
	frames      atomic.Int64
	audioBytes  atomic.Int64
	acks        atomic.Int64
	transcripts atomic.Int64
	failures    atomic.Int64
	mu          sync.Mutex
	latencies   []time.Duration
}

func (s *stats) recordLatency(value time.Duration) {
	s.mu.Lock()
	if len(s.latencies) < 1_000_000 {
		s.latencies = append(s.latencies, value)
	}
	s.mu.Unlock()
}

func (s *stats) report(start time.Time) {
	s.mu.Lock()
	values := append([]time.Duration(nil), s.latencies...)
	s.mu.Unlock()
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	percentile := func(p float64) time.Duration {
		if len(values) == 0 {
			return 0
		}
		index := int(float64(len(values)-1) * p)
		return values[index]
	}
	fmt.Printf(
		"elapsed=%s attempted=%d connected=%d failures=%d frames=%d audio_bytes=%d acks=%d transcripts=%d ack_p50=%s ack_p95=%s ack_p99=%s\n",
		time.Since(start).Round(time.Millisecond),
		s.attempted.Load(), s.connected.Load(), s.failures.Load(),
		s.frames.Load(), s.audioBytes.Load(), s.acks.Load(), s.transcripts.Load(),
		percentile(.50), percentile(.95), percentile(.99),
	)
}

type runner struct {
	baseURL string
	token   string
	client  *http.Client
	stats   *stats
}

func main() {
	baseURL := flag.String("gateway", "http://127.0.0.1:8080", "gateway HTTP base URL")
	connections := flag.Int("connections", 100, "number of concurrent streams")
	duration := flag.Duration("duration", 30*time.Minute, "audio streaming duration per connection")
	ramp := flag.Duration("ramp", 30*time.Second, "time over which connections are started")
	token := flag.String("token", "", "Bearer token; empty uses the development token endpoint")
	flag.Parse()
	if *connections <= 0 || *duration <= 0 || *ramp < 0 {
		log.Fatal("connections and duration must be positive; ramp cannot be negative")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	httpClient := &http.Client{Timeout: 10 * time.Second}
	if *token == "" {
		var err error
		*token, err = developmentToken(ctx, httpClient, strings.TrimRight(*baseURL, "/"))
		if err != nil {
			log.Fatalf("obtain development token: %v", err)
		}
	}
	result := &stats{}
	load := &runner{
		baseURL: strings.TrimRight(*baseURL, "/"),
		token:   *token, client: httpClient, stats: result,
	}
	start := time.Now()
	var wg sync.WaitGroup
	delay := time.Duration(0)
	if *connections > 1 {
		delay = *ramp / time.Duration(*connections-1)
	}
	for i := 0; i < *connections; i++ {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := load.runStream(ctx, *duration); err != nil && !errors.Is(err, context.Canceled) {
				result.failures.Add(1)
			}
		}()
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
			}
		}
	}
	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				result.report(start)
			case <-progressDone:
				return
			}
		}
	}()
	wg.Wait()
	close(progressDone)
	result.report(start)
	if result.failures.Load() > 0 {
		os.Exit(1)
	}
}

func (r *runner) runStream(parent context.Context, duration time.Duration) error {
	r.stats.attempted.Add(1)
	stream, err := r.create(parent)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, duration+10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, stream.WebSocket, nil)
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	if err := writeJSON(ctx, conn, map[string]string{"type": "hello", "token": stream.AttachToken}); err != nil {
		return err
	}
	var ready struct {
		Type             string `json:"type"`
		NextSampleOffset uint64 `json:"next_sample_offset"`
	}
	attached := false
	for ready.Type != "ready" {
		messageType, data, err := conn.Read(ctx)
		if err != nil || messageType != websocket.MessageText {
			return fmt.Errorf("read attach handshake: %w", err)
		}
		if err := json.Unmarshal(data, &ready); err != nil {
			return fmt.Errorf("decode attach handshake: %w", err)
		}
		if ready.Type == "attached" {
			attached = true
		} else if ready.Type != "ready" {
			return errors.New("gateway returned an unexpected attach event")
		}
	}
	if !attached {
		return errors.New("gateway did not confirm attachment before ready")
	}
	r.stats.connected.Add(1)

	sentAt := make(map[uint64]time.Time)
	var sentMu sync.Mutex
	readErr := make(chan error, 1)
	go func() {
		for {
			kind, payload, err := conn.Read(ctx)
			if err != nil {
				readErr <- err
				return
			}
			if kind != websocket.MessageText {
				continue
			}
			var message struct {
				Type             string `json:"type"`
				NextSampleOffset uint64 `json:"next_sample_offset"`
			}
			if json.Unmarshal(payload, &message) != nil {
				continue
			}
			switch message.Type {
			case "ack":
				r.stats.acks.Add(1)
				sentMu.Lock()
				started, ok := sentAt[message.NextSampleOffset]
				delete(sentAt, message.NextSampleOffset)
				sentMu.Unlock()
				if ok {
					r.stats.recordLatency(time.Since(started))
				}
			case "transcript":
				r.stats.transcripts.Add(1)
			case "error":
				readErr <- errors.New("gateway returned an error")
				return
			}
		}
	}()

	streaming := time.NewTimer(duration)
	defer streaming.Stop()
	ticker := time.NewTicker(40 * time.Millisecond)
	defer ticker.Stop()
	offset := ready.NextSampleOffset
	pcm := make([]byte, 1280)
	for {
		select {
		case now := <-ticker.C:
			frame := make([]byte, 8+len(pcm))
			binary.BigEndian.PutUint64(frame[:8], offset)
			copy(frame[8:], pcm)
			if err := conn.Write(ctx, websocket.MessageBinary, frame); err != nil {
				return err
			}
			offset += 640
			sentMu.Lock()
			sentAt[offset] = now
			sentMu.Unlock()
			r.stats.frames.Add(1)
			r.stats.audioBytes.Add(int64(len(pcm)))
		case err := <-readErr:
			return err
		case <-streaming.C:
			_ = writeJSON(ctx, conn, map[string]string{"type": "end"})
			return nil
		case <-parent.Done():
			return parent.Err()
		}
	}
}

func (r *runner) create(ctx context.Context) (createResponse, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, r.baseURL+"/v1/streams", bytes.NewBufferString("{}"))
	if err != nil {
		return createResponse{}, err
	}
	request.Header.Set("Authorization", "Bearer "+r.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := r.client.Do(request)
	if err != nil {
		return createResponse{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return createResponse{}, fmt.Errorf("create stream: HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	var stream createResponse
	if err := json.NewDecoder(response.Body).Decode(&stream); err != nil {
		return createResponse{}, err
	}
	return stream, nil
}

func developmentToken(ctx context.Context, client *http.Client, baseURL string) (string, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/dev/token",
		bytes.NewBufferString(`{"tenant_id":"load-test","subject":"loadgen"}`))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d", response.StatusCode)
	}
	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		return "", err
	}
	return body.AccessToken, nil
}

func writeJSON(ctx context.Context, conn *websocket.Conn, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}
