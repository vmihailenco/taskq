test:
	go test ./...
	go test ./... -short -race
	go test ./... -run=NONE -bench=. -benchmem

go_mod_tidy:
	go get -u && go mod tidy
	set -e; for dir in $(ALL_GO_MOD_DIRS); do \
	  echo "go mod tidy in $${dir}"; \
	  (cd "$${dir}" && \
	    go get -u ./... && \
	    go mod tidy); \
	done

fmt:
	gofmt -w -s ./
	goimports -w  -local github.com/vmihailenco/taskq ./
