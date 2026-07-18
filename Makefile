.DEFAULT_GOAL := help

.PHONY: help build vet test tidy fmt lint ci

## help: Show this help message (default target).
help:
	@echo "Available targets:"
	@grep -E '^## [a-z]' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ": "}; {name=$$1; sub(/^## /, "", name); printf "  %-18s %s\n", name, $$2}'

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

## lint: Run golangci-lint if available, otherwise skip.
lint:
	@if command -v golangci-lint > /dev/null 2>&1; then \
		golangci-lint run; \
	else \
		echo "golangci-lint not found — skipping lint (install from https://golangci-lint.run)"; \
	fi

## ci: Run vet, test, and build (full CI gate).
ci: vet test build
