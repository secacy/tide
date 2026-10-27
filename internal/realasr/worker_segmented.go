package realasr

import (
	"io"
	"strconv"

	asrv1 "github.com/secacy/tide-artisan/proto/tide/asr/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recognizeSegmented 串行拥有整场模型、当前片段快照和原始输入计数。
// 每场仅调用工厂一次；无论成功或失败，Close 返回后才离开此分支。
func (w *Worker) recognizeSegmented(stream asrv1.ASRService_StreamingRecognizeServer) error {
	ctx := stream.Context()
	engine, factoryErr := w.segmentedFactory(ctx)
	if factoryErr == nil && engine != nil {
		defer engine.Close()
	}
	if err := contextFailure(ctx); err != nil {
		return err
	}
	if factoryErr != nil {
		return status.Error(codes.Internal, "model creation failed")
	}
	if engine == nil {
		return status.Error(codes.Internal, "model factory returned nil engine")
	}
	cursor, err := newSegmentCursor(w.cfg.MaxAudioBytes, w.maxSegmentBytes)
	if err != nil {
		return status.Error(codes.Internal, "segment cursor initialization failed")
	}
	mapper, err := newSnapshotMapper("1", w.cfg.MaxTextBytes)
	if err != nil {
		return status.Error(codes.Internal, "snapshot mapper initialization failed")
	}
	// 工厂已建立第一段流。满段收尾成功后置 false；计数仍停留在旧段。
	segmentActive := true
	for {
		if err := contextFailure(ctx); err != nil {
			return err
		}
		req, recvErr := stream.Recv()
		if err := contextFailure(ctx); err != nil {
			return err
		}
		if recvErr == io.EOF {
			if cursor.totalBytes == 0 {
				return status.Error(codes.InvalidArgument, "audio stream is empty")
			}
			if segmentActive {
				if err := w.finishSegment(stream, engine, mapper); err != nil {
					return err
				}
				engine.ReleaseSegment()
			}
			// 边界 EOF 不创建新流，也不重复发送已经定稿的片段。
			return contextFailure(ctx)
		}
		if recvErr != nil {
			return recvErr
		}
		if req == nil {
			return status.Error(codes.InvalidArgument, "nil audio request")
		}
		data := req.GetData()
		if len(data) == 0 {
			continue
		}
		if len(data)%2 != 0 {
			return status.Error(codes.InvalidArgument, "unaligned PCM audio")
		}
		if len(data) > w.cfg.MaxChunkBytes {
			return status.Error(codes.ResourceExhausted, "audio chunk limit exceeded")
		}
		// 在提交任何前缀或创建下一流之前，拒绝整份超出总预算的请求。
		if _, err := cursor.plan(len(data)); err != nil {
			return status.Error(codes.ResourceExhausted, "audio stream limit exceeded")
		}
		for len(data) > 0 {
			if err := contextFailure(ctx); err != nil {
				return err
			}
			if !segmentActive {
				startErr := engine.StartSegment()
				if err := contextFailure(ctx); err != nil {
					return err
				}
				if startErr != nil {
					return status.Error(codes.Internal, "model segment creation failed")
				}
				if err := cursor.advance(); err != nil {
					return status.Error(codes.Internal, "segment cursor advance failed")
				}
				mapper, err = newSnapshotMapper(strconv.Itoa(cursor.segmentNumber), w.cfg.MaxTextBytes)
				if err != nil {
					return status.Error(codes.Internal, "snapshot mapper initialization failed")
				}
				segmentActive = true
			}
			n, err := cursor.plan(len(data))
			if err != nil || n == 0 {
				return status.Error(codes.Internal, "segment cursor planning failed")
			}
			samples := pcm16Samples(data[:n])
			if err := contextFailure(ctx); err != nil {
				return err
			}
			acceptErr := engine.Accept(samples)
			if err := contextFailure(ctx); err != nil {
				return err
			}
			if acceptErr != nil {
				return status.Error(codes.Internal, "model accept failed")
			}
			if err := cursor.commit(n); err != nil {
				return status.Error(codes.Internal, "segment cursor commit failed")
			}
			if err := w.drain(stream, engine, mapper); err != nil {
				return err
			}
			if cursor.segmentBytes == cursor.maxSegmentBytes {
				if err := w.finishSegment(stream, engine, mapper); err != nil {
					return err
				}
				engine.ReleaseSegment()
				if err := contextFailure(ctx); err != nil {
					return err
				}
				mapper = nil
				segmentActive = false
			}
			data = data[n:]
		}
	}
}
