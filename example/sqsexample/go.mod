module github.com/vmihailenco/taskq/example/sqsexample

go 1.17

require (
	github.com/aws/aws-sdk-go v1.55.7
	github.com/redis/go-redis/v9 v9.8.0
	github.com/vmihailenco/taskq/v3 v3.2.9
)

require (
	github.com/bsm/redislock v0.9.4 // indirect
	github.com/capnm/sysinfo v0.0.0-20130621111458-5909a53897f3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-farm v0.0.0-20240924180020-3414d57e47da // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-redis/redis_rate/v10 v10.0.1 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.15.1 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
)

replace github.com/vmihailenco/taskq/v3 => ../..
