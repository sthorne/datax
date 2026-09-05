GO ?= go

.PHONY: build test test-race vet fmt fmt-check proto lint all bench

all: build

build:
	$(GO) build -o bin/datax ./cmd/datax

test:
	$(GO) test ./...

test-race:
	$(GO) test -race -timeout 20m ./...

vet:
	$(GO) vet ./...

# Format with gofmt, never goimports: goimports regroups the imports of the
# generated *.pb.go files (std library first), which `make proto` then flips
# back to protoc-gen-go's order — a spurious diff either way. The generated
# files are canonical as committed; `make proto` is a no-op on them.
fmt:
	gofmt -l -s -w $$(git ls-files '*.go' | grep -v '\.pb\.go$$')

fmt-check:
	@out=$$(gofmt -l -s .); if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi

# Regenerate protobuf/gRPC code. Requires buf, protoc-gen-go, protoc-gen-go-grpc
# (all pure Go: `go install github.com/bufbuild/buf/cmd/buf@latest` etc.).
# Generated code is committed, so users never need this.
proto:
	buf generate

# Run the checked-in workload set against a fresh single node and a fresh
# 3-node local cluster; records land under bench-results/<timestamp>/.
# See bench/README.md for recording a before/after.
bench: build
	bench/run.sh

lint: vet fmt-check
