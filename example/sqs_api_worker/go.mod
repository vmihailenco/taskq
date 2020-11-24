module github.com/vmihailenco/taskq/example/api_worker/sqs_api_worker

go 1.15

require (
	github.com/aws/aws-sdk-go v1.35.28
	github.com/go-redis/redis/v8 v8.4.0
	github.com/go-sql-driver/mysql v1.5.0 // indirect
	github.com/vmihailenco/taskq/v3 v3.2.3
)

replace github.com/vmihailenco/taskq/v3 => ../..
