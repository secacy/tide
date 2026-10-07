package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/secacy/tide-artisan/internal/gateway"
	"github.com/secacy/tide-artisan/internal/workerpool"
)

func TestGatewayV2OptInRoute(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		enabled bool
	}{
		{"default", nil, false},
		{"enabled", []string{"-enable-v2"}, true},
		{"explicit_false", []string{"-enable-v2=false"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseGatewayConfig(tc.args)
			if err != nil {
				t.Fatal(err)
			}
			if (cfg.Gateway.V2 != nil) != tc.enabled {
				t.Fatal("v2 opt-in not preserved")
			}
			pool, err := workerpool.NewRoundRobin([]workerpool.Worker{{ID: "config", Client: &configOnlyWorker{}}})
			if err != nil {
				t.Fatal(err)
			}
			g, err := gateway.New(context.Background(), pool, cfg.Gateway)
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			routes(g).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v2/asr", nil))
			want := 404
			if tc.enabled {
				want = http.StatusUpgradeRequired
			} // 进入真实 handler，但没有 WebSocket 升级头。
			if w.Code != want || g.Snapshot().ActiveSessions != 0 {
				t.Fatalf("route status=%d want=%d", w.Code, want)
			}
			g.StopAccepting()
			if err := g.Wait(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
