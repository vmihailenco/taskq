all:
	go test ./...
	redis-cli flushdb
	go test ./... -short -race
	go test ./... -run=NONE -bench=. -benchmem
	go vet ./...
	go get github.com/gordonklaus/ineffassign
	ineffassign .
