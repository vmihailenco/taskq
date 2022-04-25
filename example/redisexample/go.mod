module github.com/vmihailenco/taskq/example/redisexample

go 1.15

require (
	github.com/go-redis/redis/v8 v8.11.4
	github.com/vmihailenco/taskq/v3 v3.2.8
)

replace github.com/vmihailenco/taskq/v3 => ../..
