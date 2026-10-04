.PHONY: help test-fast test test-race test-tools

help:
	@echo 'test-fast  Run audio and worker selection checks (limited scope)'
	@echo 'test       Run default Go tests, including local network/process tests'
	@echo 'test-race  Run default Go tests with the race detector'
	@echo 'test-tools Run Python experiment-tool correctness tests'
	@echo 'See docs/testing.md for opt-in experiments and scope.'

test-fast:
	go test ./internal/audio ./internal/workerpool -count=1 -timeout=60s

test:
	go test ./... -count=1 -timeout=5m

test-race:
	go test -race -p=1 ./... -count=1 -timeout=5m

test-tools:
	PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s docs/experiments/scripts -p 'test_*.py'
