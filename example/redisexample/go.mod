module github.com/vmihailenco/taskq/example/redisexample

go 1.25.0

toolchain go1.27.4

require (
	github.com/redis/go-redis/v9 v9.22.0
	github.com/vmihailenco/taskq/v5 v5.0.0
)

require (
	github.com/bsm/redislock v0.10.0 // indirect
	github.com/capnm/sysinfo v0.0.0-20130621111458-5909a53897f3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-farm v0.0.0-20240924180020-3414d57e47da // indirect
	github.com/go-redis/redis_rate/v10 v10.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/klauspost/compress v1.20.4 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
)

replace github.com/vmihailenco/taskq/v5 => ../..
