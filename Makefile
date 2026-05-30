run:
	@go run ./cmd/firehose/

lint:
	@go fmt ./...
	-@go build -gcflags -m . 2>&1 | grep "escapes to heap"
	@go tool deadcode ./...
	@go tool nilaway ./...
	@golangci-lint run


fix:
	@go fmt ./...
	@golangci-lint run --fix

opts:
	-@go build -gcflags -m .


test:
	@go test -race -coverprofile=coverage.out ./...
