run:
	@go run ./cmd/firehose/

lint:
	@go fmt ./...
	-@go build -gcflags -m . 2>&1 | grep "escapes to heap"
	@go tool deadcode ./...
	@golangci-lint run


fix:
	@go fmt ./...
	@golangci-lint run --fix
