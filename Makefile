ALL_GO_MOD_DIRS := $(shell find . -type f -name 'go.mod' -exec dirname {} \; | sort)

test:
	go test ./...
	go test ./... -short -race
	go test ./... -run=NONE -bench=. -benchmem

# No `go get -u` here: newer releases of some dependencies (go-redis v9.23.0+,
# golang.org/x/net v0.59.0+) require Go 1.26, and taskq still supports Go 1.25.
go_mod_tidy:
	set -e; for dir in $(ALL_GO_MOD_DIRS); do \
	  echo "go mod tidy in $${dir}"; \
	  (cd "$${dir}" && go mod tidy -go=1.25.0); \
	done

fmt:
	gofmt -w -s ./
	goimports -w  -local github.com/vmihailenco/taskq ./
