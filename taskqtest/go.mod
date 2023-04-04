module github.com/vmihailenco/taskq/taskqtest/v4

go 1.20

replace github.com/vmihailenco/taskq/v4 => ./..

require (
	github.com/go-redis/redis_rate/v10 v10.0.1
	github.com/redis/go-redis/v9 v9.0.3
	github.com/stretchr/testify v1.8.1
	github.com/vmihailenco/taskq/v4 v4.0.0-00010101000000-000000000000
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/klauspost/compress v1.15.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/vmihailenco/msgpack/v5 v5.3.5 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
