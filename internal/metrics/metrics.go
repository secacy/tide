package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	ActiveConnections  prometheus.Gauge
	ActiveOwnedStreams prometheus.Gauge
	StreamsCreated     prometheus.Counter
	AudioBytes         prometheus.Counter
	Errors             *prometheus.CounterVec
	AudioRelaySeconds  prometheus.Histogram
	ResultRelaySeconds prometheus.Histogram
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ActiveConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "tide", Name: "active_connections",
			Help: "Current authenticated WebSocket connections.",
		}),
		ActiveOwnedStreams: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "tide", Name: "active_owned_streams",
			Help: "Current ASR streams owned by this gateway.",
		}),
		StreamsCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "tide", Name: "streams_created_total",
			Help: "Logical streams created.",
		}),
		AudioBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "tide", Name: "audio_bytes_total",
			Help: "PCM bytes accepted from clients.",
		}),
		Errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "tide", Name: "errors_total",
			Help: "Errors by low-cardinality code.",
		}, []string{"code"}),
		AudioRelaySeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "tide", Name: "audio_relay_seconds",
			Help:    "Time from WebSocket receipt to ASR send completion.",
			Buckets: []float64{.001, .0025, .005, .01, .02, .05, .1, .25},
		}),
		ResultRelaySeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "tide", Name: "result_relay_seconds",
			Help:    "Time from ASR result receipt to WebSocket write completion.",
			Buckets: []float64{.001, .0025, .005, .01, .02, .05, .1, .25},
		}),
	}
	reg.MustRegister(
		m.ActiveConnections, m.ActiveOwnedStreams, m.StreamsCreated,
		m.AudioBytes, m.Errors, m.AudioRelaySeconds, m.ResultRelaySeconds,
	)
	return m
}
