package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	asrv1 "github.com/can/tide/gen/tide/asr/v1"
	"google.golang.org/grpc"
)

type server struct {
	asrv1.UnimplementedASRServer
}

func (server) Transcribe(stream grpc.BidiStreamingServer[asrv1.GatewayToASR, asrv1.ASRToGateway]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return errors.New("first ASR message must be start")
	}
	if start.Audio == nil || start.Audio.Encoding != "pcm_s16le" ||
		start.Audio.SampleRateHz != 16000 || start.Audio.Channels != 1 {
		return errors.New("unsupported audio configuration")
	}
	if err := stream.Send(&asrv1.ASRToGateway{Payload: &asrv1.ASRToGateway_Ready{
		Ready: &asrv1.Ready{Epoch: start.Epoch},
	}}); err != nil {
		return err
	}
	nextOffset := start.InitialSampleOffset
	frameCount := uint64(0)
	segment := uint64(1)
	for {
		message, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch payload := message.Payload.(type) {
		case *asrv1.GatewayToASR_Audio:
			audio := payload.Audio
			if audio.SampleOffset != nextOffset {
				return stream.Send(&asrv1.ASRToGateway{Payload: &asrv1.ASRToGateway_Error{
					Error: &asrv1.ASRError{Code: "offset_gap", Message: "sample offset is not contiguous"},
				}})
			}
			nextOffset += uint64(len(audio.Pcm) / 2)
			frameCount++
			if err := stream.Send(&asrv1.ASRToGateway{Payload: &asrv1.ASRToGateway_Ack{
				Ack: &asrv1.Ack{NextSampleOffset: nextOffset},
			}}); err != nil {
				return err
			}
			if frameCount%25 == 0 {
				isFinal := frameCount%75 == 0
				text := fmt.Sprintf("[mock] 已接收 %.1f 秒音频", float64(nextOffset-start.InitialSampleOffset)/16000)
				if err := stream.Send(&asrv1.ASRToGateway{Payload: &asrv1.ASRToGateway_Transcript{
					Transcript: &asrv1.Transcript{
						Epoch: start.Epoch, SegmentId: fmt.Sprintf("segment-%d", segment),
						Revision: frameCount / 25, Text: text, IsFinal: isFinal,
						StartMs: (segment - 1) * 3000, EndMs: (nextOffset - start.InitialSampleOffset) * 1000 / 16000,
					},
				}}); err != nil {
					return err
				}
				if isFinal {
					segment++
				}
			}
		case *asrv1.GatewayToASR_End:
			return nil
		default:
			return errors.New("unexpected ASR message")
		}
	}
}

func main() {
	addr := flag.String("addr", ":9091", "gRPC listen address")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Error("listen", "error", err)
		os.Exit(1)
	}
	grpcServer := grpc.NewServer()
	asrv1.RegisterASRServer(grpcServer, server{})
	go func() {
		logger.Info("mock ASR listening", "addr", *addr)
		if err := grpcServer.Serve(listener); err != nil {
			logger.Error("serve", "error", err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	grpcServer.GracefulStop()
}
