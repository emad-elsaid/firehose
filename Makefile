check: fmt escapes deadcode nilaway lint

lint:
	@golangci-lint run

fmt:
	@go fmt ./...

escapes:
	-@go build -gcflags -m . 2>&1 | grep -E "escapes to heap|moved to heap"

deadcode:
	@go tool deadcode ./...

nilaway:
	@go tool nilaway ./...

fix:
	@go fmt ./...
	@golangci-lint run --fix

opts:
	-@go build -gcflags -m .


test:
	@go test -v -race -coverprofile=coverage.out ./...

generate:
	@mockery
