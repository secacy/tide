.PHONY: generate lint test race build run-gateway run-mock load

generate:
	go run github.com/bufbuild/buf/cmd/buf@v1.47.2 generate

lint:
	go run github.com/bufbuild/buf/cmd/buf@v1.47.2 lint
	go vet ./...

test:
	go test ./...

race:
	go test -race ./...

build:
	go build ./cmd/gateway ./cmd/mock-asr ./cmd/loadgen

run-gateway:
	go run ./cmd/gateway

run-mock:
	go run ./cmd/mock-asr

load:
	go run ./cmd/loadgen -connections 100 -duration 5m -ramp 10s
