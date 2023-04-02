module github.com/vmihailenco/taskq/v3/azsqs

go 1.20

replace github.com/vmihailenco/taskq/v3 => ./..

replace github.com/vmihailenco/taskq/v3/taskqtest => ../taskqtest

require (
	github.com/aws/aws-sdk-go v1.44.234
	github.com/vmihailenco/taskq/v3 v3.0.0-00010101000000-000000000000
)

require (
	github.com/bsm/redislock v0.9.1 // indirect
	github.com/capnm/sysinfo v0.0.0-20130621111458-5909a53897f3 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-redis/redis_rate/v10 v10.0.1 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.15.1 // indirect
	github.com/redis/go-redis/v9 v9.0.2 // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.5 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
)
