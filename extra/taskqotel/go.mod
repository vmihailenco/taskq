module github.com/vmihailenco/taskq/extra/taskqotel/v3

go 1.15

replace github.com/vmihailenco/taskq/v3 => ../..

require (
	github.com/vmihailenco/taskq/v3 v3.2.3
	go.opentelemetry.io/otel v0.14.0
)
