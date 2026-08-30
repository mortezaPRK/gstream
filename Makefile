.DEFAULT_GOAL := help

.PHONY: help build vet test tidy fmt lint integration-test test-race verify-modules ci examples-up examples-topics example-filter-map example-stateful example-joins example-eos example-smoke examples-down

## help: Show this help message (default target).
help:
	@echo "Available targets:"
	@grep -E '^## [a-z]' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ": "}; {name=$$1; sub(/^## /, "", name); printf "  %-20s %s\n", name, $$2}'

## build: Compile all packages (go build ./...).
build:
	go build ./...

## vet: Run go vet on all packages.
vet:
	go vet ./...

## test: Run all tests (go test ./...).
test:
	go test ./...

## tidy: Tidy and verify the module graph (go mod tidy).
tidy:
	go mod tidy

## fmt: Format all Go source files (gofmt -l -w .).
fmt:
	gofmt -l -w .

## lint: Run golangci-lint.
lint:
	golangci-lint run

## integration-test: Run integration tests (requires Docker/Podman).
integration-test:
	TESTCONTAINERS_RYUK_DISABLED=true go test -p 1 -tags integration ./...

## test-race: Run all tests with race detector.
test-race:
	go test -race ./...

## examples-up: Start shared local Kafka broker for examples.
examples-up:
	docker compose -f examples/compose.yml up -d --wait

## examples-topics: Create caller-managed source, sink, and global-table topics.
examples-topics:
	@for topic in filter-map-input filter-map-output stateful-input stateful-count-output eos-input eos-output join-table-input join-stream-input join-table-output join-left-input join-right-input join-stream-output join-profiles join-orders join-global-output; do \
		docker compose -f examples/compose.yml exec -T kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic $$topic --partitions 3 --replication-factor 1; \
	done

## example-filter-map: Run stateless ALO filter/map example.
example-filter-map:
	go run ./examples/filter-map

## example-stateful: Run count, time-window, and session-window example.
example-stateful:
	go run ./examples/stateful

## example-joins: Run stream-table, stream-stream, and global-table joins example.
example-joins:
	go run ./examples/joins

## example-eos: Run controlled EOS stop/restart recovery example.
example-eos:
	go run ./examples/eos-recovery

## example-smoke: Run all public examples and verify outputs and clean shutdown.
example-smoke: examples-topics
	sh examples/smoke.sh

## examples-down: Stop shared local Kafka broker.
examples-down:
	docker compose -f examples/compose.yml down

## verify-modules: Build each module standalone (GOWORK=off) to catch per-module go.sum gaps.
verify-modules:
	GOWORK=off go build ./...
	cd serde/proto && GOWORK=off go build ./...

## ci: Run vet, test, build, and verify-modules (full CI gate).
ci: vet test build verify-modules
