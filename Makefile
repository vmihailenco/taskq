all:
	go test ./...
	go build ./examples/api_worker/...
	redis-cli flushdb
	go test ./... -short -race
	go test ./... -run=NONE -bench=. -benchmem
	golangci-lint run

test:
	go test
