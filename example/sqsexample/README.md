# taskq example using SQS backend

The example requires `AWS_ACCOUNT_ID`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` environment
variables.

First, start the consumer:

```shell
go run consumer/main.go

```

Then, start the producer:

```shell
go run producer/main.go
```
