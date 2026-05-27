run:
	@go run ./cmd/firehose/

lint:
	@go fmt ./...
	@golangci-lint run


fix:
	@go fmt ./...
	@golangci-lint run --fix
