module github.com/vmihailenco/taskq/example/sqsexample

go 1.15

require (
	github.com/aws/aws-sdk-go v1.42.7
	github.com/go-redis/redis/v8 v8.11.4
	github.com/vmihailenco/taskq/v3 v3.2.8
)

replace github.com/vmihailenco/taskq/v3 => ../..
