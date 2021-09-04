module github.com/vmihailenco/taskq/example/api_worker/sqs_api_worker

go 1.15

require (
	github.com/aws/aws-sdk-go v1.40.25
	github.com/go-redis/redis/v8 v8.11.2
	github.com/vmihailenco/taskq/v3 v3.2.5
)

replace github.com/vmihailenco/taskq/v3 => ../..
