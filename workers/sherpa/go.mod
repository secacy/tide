module github.com/secacy/tide-artisan/workers/sherpa

go 1.26

require (
	github.com/k2-fsa/sherpa-onnx-go-macos v1.13.8
	github.com/secacy/tide-artisan v0.0.0
	google.golang.org/grpc v1.83.2
)

require (
	github.com/coder/websocket v1.8.15 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/secacy/tide-artisan => ../..
