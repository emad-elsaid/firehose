run:
	@go run ./cmd/firehose/

lint:
	@go fmt ./...
	-@go build -gcflags -m . 2>&1 | grep "escapes to heap"
	@golangci-lint run


fix:
	@go fmt ./...
	@golangci-lint run --fix
