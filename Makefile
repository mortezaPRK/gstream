.DEFAULT_GOAL := help

.PHONY: help build vet test tidy fmt lint integration-test verify-modules ci

MODULES := serdes/bytes serdes/json serdes/proto stores/memory stores/pebble loggers/slog examples integration/kafka

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
	@for module in $(MODULES); do \
		echo "vetting $$module"; \
		(cd $$module && GOWORK=off go vet ./...) || exit 1; \
	done

## test: Run all tests (go test ./...).
test:
	go test ./...
	@for module in $(MODULES); do \
		echo "testing $$module"; \
		(cd $$module && GOWORK=off go test ./...) || exit 1; \
	done

## tidy: Tidy and verify the module graph (go mod tidy).
tidy:
	go mod tidy
	@for module in $(MODULES); do \
		echo "tidying $$module"; \
		(cd $$module && GOWORK=off go mod tidy) || exit 1; \
	done

## fmt: Format all Go source files (gofmt -l -w .).
fmt:
	gofmt -l -w .

## lint: Run golangci-lint.
lint:
	golangci-lint run

## integration-test: Run Testcontainers integration tests (requires Docker/Podman).
integration-test:
	cd integration/kafka && GOWORK=off go test -p 1 -tags integration ./...

## verify-modules: Build each module standalone (GOWORK=off) to catch per-module go.sum gaps.
verify-modules:
	GOWORK=off go build ./...
	@for module in $(filter-out examples integration/kafka,$(MODULES)); do \
		echo "building $$module"; \
		(cd $$module && GOWORK=off go build ./...) || exit 1; \
	done
	cd examples && GOWORK=off go test -run '^$$' ./...
	cd integration/kafka && GOWORK=off go build -tags integration ./...

## ci: Run vet, test, build, and verify-modules (full CI gate).
ci: vet test build verify-modules
