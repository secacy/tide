package loadgen_test

import (
	"bytes"
	"io"
	"math"
	"sync"
	"testing"

	"github.com/secacy/tide-artisan/internal/audio"
	"github.com/secacy/tide-artisan/internal/loadgen"
)

func TestSilenceSourceRejectsInvalidLength(t *testing.T) {
	for _, tc := range []struct {
		name string
		size int64
	}{
		{"zero", 0},
		{"negative", -2},
		{"minimum_integer", math.MinInt64},
		{"partial_sample", 1},
		{"maximum_unaligned", math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, err := loadgen.NewSilenceSource(tc.size)
			if err == nil || source != nil {
				t.Fatalf("NewSilenceSource(%d) = (%v, %v), want nil source and error", tc.size, source, err)
			}
		})
	}
}

// TestSilenceSourceReadFullMatchesClient 验证客户端 io.ReadFull 分块读取时，
// 完整块和不足一块的尾部都保留，结束后不会继续生成数据。
func TestSilenceSourceReadFullMatchesClient(t *testing.T) {
	for _, tc := range []struct {
		name  string
		total int64
	}{
		{"one_sample", int64(audio.BytesDepth)},
		{"exact_chunks", 2 * int64(audio.ChunkBytesDefault)},
		{"partial_last_chunk", 2*int64(audio.ChunkBytesDefault) + int64(audio.BytesDepth)},
		{"sixty_seconds", 60 * int64(audio.BytesPerSecond)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := newSource(t, tc.total)
			buf := make([]byte, audio.ChunkBytesDefault)
			var read int64
			for read < tc.total {
				// 每轮恢复非零内容，防止测试只验证了 make 的初始零值。
				for i := range buf {
					buf[i] = 0xa5
				}
				want := int(min(int64(len(buf)), tc.total-read))
				var wantErr error
				if want < len(buf) {
					wantErr = io.ErrUnexpectedEOF
				}
				n, err := io.ReadFull(source, buf)
				if n != want || err != wantErr {
					t.Fatalf("at byte %d: ReadFull = (%d, %v), want (%d, %v)", read, n, err, want, wantErr)
				}
				assertZero(t, buf[:n])
				for _, b := range buf[n:] {
					if b != 0xa5 {
						t.Fatal("read changed bytes outside the returned range")
					}
				}
				read += int64(n)
			}
			for range 2 {
				if n, err := source.Read(buf); n != 0 || err != io.EOF {
					t.Fatalf("after exhaustion: Read = (%d, %v), want (0, EOF)", n, err)
				}
			}
		})
	}
}

// TestSilenceSourceAllowsOddReads 验证 Reader 不把调用方缓冲区当作 PCM 块；
// 奇数字节读取和空读取都不应丢失或额外消耗数据。
func TestSilenceSourceAllowsOddReads(t *testing.T) {
	source := newSource(t, 8)
	for _, size := range []int{0, 3, 0, 5} {
		buf := bytes.Repeat([]byte{0xa5}, size)
		n, err := source.Read(buf)
		if n != size || err != nil {
			t.Fatalf("Read(%d bytes) = (%d, %v)", size, n, err)
		}
		assertZero(t, buf)
	}
	if n, err := source.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("after 8 bytes: Read = (%d, %v), want (0, EOF)", n, err)
	}
}

// TestSilenceSourcesHaveIndependentCursors 模拟多场会话各自读取音频。
// 先耗尽其中一场，其余各场仍应能并发读完各自的全部输入。
func TestSilenceSourcesHaveIndependentCursors(t *testing.T) {
	const sessions = 16
	const total int64 = 2*audio.ChunkBytesDefault + audio.BytesDepth
	sources := make([]io.Reader, sessions)
	for i := range sources {
		sources[i] = newSource(t, total)
	}
	if n, err := io.Copy(io.Discard, sources[0]); n != total || err != nil {
		t.Fatalf("first source = (%d, %v), want (%d, nil)", n, err, total)
	}
	var wg sync.WaitGroup
	for i := 1; i < sessions; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := io.Copy(io.Discard, sources[i])
			if n != total || err != nil {
				t.Errorf("source %d = (%d, %v), want (%d, nil)", i, n, err, total)
			}
		}()
	}
	wg.Wait()
}

// TestSilenceSourceLargeLengthSmallRead 只消费超大逻辑输入的一小段，
// 检查构造不要求先分配整个输入；这不是进程总内存上界测量。
func TestSilenceSourceLargeLengthSmallRead(t *testing.T) {
	const total int64 = math.MaxInt64 - math.MaxInt64%audio.BytesDepth
	source := newSource(t, total)
	buf := bytes.Repeat([]byte{0xa5}, audio.ChunkBytesDefault)
	if n, err := io.ReadFull(source, buf); n != len(buf) || err != nil {
		t.Fatalf("ReadFull = (%d, %v), want (%d, nil)", n, err, len(buf))
	}
	assertZero(t, buf)
}

func newSource(t *testing.T, total int64) io.Reader {
	t.Helper()
	source, err := loadgen.NewSilenceSource(total)
	if err != nil {
		t.Fatalf("NewSilenceSource(%d): %v", total, err)
	}
	return source
}

func assertZero(t *testing.T, data []byte) {
	t.Helper()
	for i, b := range data {
		if b != 0 {
			t.Fatalf("byte %d = %d, want silence", i, b)
		}
	}
}
