package mockasr

import (
	"errors"
	"fmt"
	"github.com/secacy/tide-artisan/internal/audio"
	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
)

// RecoverableRecognize implements independent synthetic segments. It proves the
// wire contract only: no real decoder state or speech quality is being modeled.
func (w *Worker) RecoverableRecognize(stream asrv1.ASRService_RecoverableRecognizeServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.RecoveryStart
	if start == nil || start.SessionId == "" || start.AttemptId == "" || len(first.Data) != 0 || first.AudioSeq != 0 || first.StartSample != 0 {
		return status.Error(codes.InvalidArgument, "recovery Start required")
	}
	if err := stream.Send(&asrv1.StreamingRecognizeResponse{Ready: true}); err != nil {
		return err
	}
	from, next := start.FromSample, start.FromSample
	var seq uint64
	interval := uint64(w.cfg.PartialEvery.Seconds() * 16000)
	if interval == 0 {
		interval = 8000
	}
	emit := func() error {
		if from == next {
			return nil
		}
		cp := &asrv1.RecoveryCheckpoint{FromSample: from, ThroughSample: next, Text: fmt.Sprintf("%s [%d,%d)", w.cfg.FinalText, from, next)}
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{Checkpoint: cp}); err != nil {
			return err
		}
		from = next
		return nil
	}
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return emit()
		}
		if err != nil {
			return err
		}
		if req.RecoveryStart != nil || len(req.Data) == 0 || len(req.Data)%audio.BytesDepth != 0 || req.AudioSeq != seq+1 || req.StartSample != next {
			return status.Error(codes.InvalidArgument, "noncontiguous recoverable audio")
		}
		n := uint64(len(req.Data) / audio.BytesDepth)
		if next+n < next {
			return status.Error(codes.InvalidArgument, "sample offset overflow")
		}
		if err := wait(stream.Context(), w.cfg.ResponseDelay); err != nil {
			return err
		}
		next += n
		seq++
		if err := stream.Send(&asrv1.StreamingRecognizeResponse{Progress: &asrv1.ProcessingProgress{ProcessedThroughSeq: seq}}); err != nil {
			return err
		}
		if next-from >= interval {
			if err := emit(); err != nil {
				return err
			}
		}
	}
}
