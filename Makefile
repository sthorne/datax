GO ?= go

.PHONY: build test test-race vet fmt-check proto lint all

all: build

build:
	$(GO) build -o bin/datax ./cmd/datax

test:
	$(GO) test ./...

test-race:
	$(GO) test -race -timeout 20m ./...

vet:
	$(GO) vet ./...

fmt-check:
	@out=$$(gofmt -l -s .); if [ -n "$$out" ]; then echo "gofmt needed on:"; echo "$$out"; exit 1; fi

# Regenerate protobuf/gRPC code. Requires buf, protoc-gen-go, protoc-gen-go-grpc
# (all pure Go: `go install github.com/bufbuild/buf/cmd/buf@latest` etc.).
# Generated code is committed, so users never need this.
proto:
	buf generate

lint: vet fmt-check
