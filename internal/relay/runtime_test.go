package relay

import (
	"testing"
	"time"

	"github.com/secacy/tide/internal/session"
)

func TestRuntimeReplaysTranscriptEventsAfterCursor(t *testing.T) {
	r := &runtime{
		manager: &Manager{detachWindow: time.Minute},
		stream:  session.Session{ID: "stream", Epoch: 1},
	}
	r.publish(Event{Type: EventTranscript, Epoch: 1, SegmentID: "one", Revision: 1, ReceivedAt: time.Now()})
	r.publish(Event{Type: EventTranscript, Epoch: 1, SegmentID: "two", Revision: 1, ReceivedAt: time.Now()})

	bridge, err := r.attach(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	event := <-bridge.Events()
	if event.Type != EventTranscript || event.EventID != 2 || event.SegmentID != "two" || !event.ReceivedAt.IsZero() {
		t.Fatalf("unexpected replay event: %+v", event)
	}
}

func TestRuntimeReportsTranscriptReplayGap(t *testing.T) {
	r := &runtime{
		manager: &Manager{detachWindow: time.Minute},
		stream:  session.Session{ID: "stream", Epoch: 1},
	}
	for revision := uint64(1); revision <= eventHistorySize+1; revision++ {
		r.publish(Event{
			Type: EventTranscript, Epoch: 1, SegmentID: "segment",
			Revision: revision, ReceivedAt: time.Now(),
		})
	}
	bridge, err := r.attach(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	event := <-bridge.Events()
	if event.Type != EventError || event.Code != "event_gap" || event.Retryable {
		t.Fatalf("unexpected replay gap event: %+v", event)
	}
}

func TestPeerTranscriptPreservesEventID(t *testing.T) {
	original := Event{
		Type: EventTranscript, EventID: 42, Epoch: 3,
		SegmentID: "segment", Revision: 2, Text: "text", IsFinal: true,
		ReceivedAt: time.Now(),
	}
	roundTrip := protoToEvent(eventToProto(original))
	if roundTrip.EventID != original.EventID || roundTrip.Epoch != original.Epoch ||
		roundTrip.SegmentID != original.SegmentID || roundTrip.Revision != original.Revision {
		t.Fatalf("peer transcript round trip: %+v", roundTrip)
	}
}
