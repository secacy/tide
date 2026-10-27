package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/secacy/tide-artisan/internal/workerpool"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc"
)

type admissionTestClient struct{}

func (admissionTestClient) StreamingRecognize(context.Context, ...grpc.CallOption) (asrv1.ASRService_StreamingRecognizeClient, error) {
	return &admissionTestStream{}, nil
}

type admissionTestStream struct{ grpc.ClientStream }

func (*admissionTestStream) Send(*asrv1.StreamingRecognizeRequest) error      { return nil }
func (*admissionTestStream) Recv() (*asrv1.StreamingRecognizeResponse, error) { return nil, io.EOF }
func (*admissionTestStream) CloseSend() error                                 { return nil }

func TestAdmissionObserverPinsRPCToOriginalAttempt(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "routing.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	r := &admissionRouter{start: time.Now(), calls: make(map[string]*admissionAssignment), events: file}
	workers := []workerpool.Worker{{ID: "A", Client: &admissionClient{client: admissionTestClient{}, router: r, worker: "A"}}, {ID: "B", Client: &admissionClient{client: admissionTestClient{}, router: r, worker: "B"}}}
	r.roundRobin, err = workerpool.NewRoundRobin(workers)
	if err != nil {
		t.Fatal(err)
	}
	r.setPending("L")
	l, err := r.TryAcquire()
	if err != nil {
		t.Fatal(err)
	}
	defer l.Release()
	first, err := l.Worker().Client.StreamingRecognize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r.setPending("S")
	s, err := r.TryAcquire()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Release()
	second, err := s.Worker().Client.StreamingRecognize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	a, _ := r.assignment("L")
	b, _ := r.assignment("S")
	if a.Worker != "A" || a.RPCEndNS == 0 || b.Worker != "B" || b.RPCEndNS != 0 {
		t.Fatal("late terminal was attached to another attempt")
	}
	if _, err := second.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	b, _ = r.assignment("S")
	if b.RPCEndNS == 0 || r.err != nil {
		t.Fatal("second terminal or event output missing")
	}
}
func TestAdmissionPlanPreservesDenominators(t *testing.T) {
	arms := admissionPlan()
	if len(arms) != 10 {
		t.Fatal("wrong arm count")
	}
	attempts := 0
	for _, arm := range arms {
		attempts += len(arm.Cases)
		if arm.State != "not_run" {
			t.Fatal("plan pre-filled outcomes")
		}
	}
	if attempts != 29 {
		t.Fatal("wrong attempt denominator")
	}
}
