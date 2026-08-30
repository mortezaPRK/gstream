.DEFAULT_GOAL := help

.PHONY: help build vet test tidy fmt lint integration-test verify-modules ci

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

## verify-modules: Build each module standalone (GOWORK=off) to catch per-module go.sum gaps.
verify-modules:
	GOWORK=off go build ./...
	cd serde/proto && GOWORK=off go build ./...

## ci: Run vet, test, build, and verify-modules (full CI gate).
ci: vet test build verify-modules
