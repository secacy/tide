package relay

import (
	"errors"
	"time"
)

var (
	ErrBackpressure  = errors.New("audio backpressure limit reached")
	ErrInvalidOffset = errors.New("audio sample offset is not contiguous")
	ErrReplaced      = errors.New("attachment replaced by a newer connection")
	ErrClosed        = errors.New("relay is closed")
)

type AudioFrame struct {
	SampleOffset uint64
	PCM          []byte
	ReceivedAt   time.Time
}

type EventType string

const (
	EventAck           EventType = "ack"
	EventTranscript    EventType = "transcript"
	EventDiscontinuity EventType = "discontinuity"
	EventError         EventType = "error"
	EventEnded         EventType = "ended"
)

type Event struct {
	Type             EventType
	Epoch            uint64
	PreviousEpoch    uint64
	NextSampleOffset uint64
	EventID          uint64
	SegmentID        string
	Revision         uint64
	Text             string
	IsFinal          bool
	StartMS          uint64
	EndMS            uint64
	Code             string
	Message          string
	Retryable        bool
	Reason           string
	ReceivedAt       time.Time
}

type Ready struct {
	Epoch           uint64
	AcceptedOffset  uint64
	CommittedOffset uint64
}

type Bridge interface {
	Ready() Ready
	Events() <-chan Event
	SendAudio(frame AudioFrame) error
	End(reason string) error
	Close()
}
