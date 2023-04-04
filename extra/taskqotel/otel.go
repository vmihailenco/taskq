package taskqotel

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/vmihailenco/taskq/v4"
)

var tracer = otel.Tracer("github.com/vmihailenco/taskq")

type OpenTelemetryHook struct{}

var _ taskq.ConsumerHook = (*OpenTelemetryHook)(nil)

func NewHook() *OpenTelemetryHook {
	return new(OpenTelemetryHook)
}

func (h OpenTelemetryHook) BeforeProcessJob(evt *taskq.ProcessJobEvent) error {
	evt.Ctx, _ = tracer.Start(evt.Ctx, evt.Job.TaskName)
	return nil
}

func (h OpenTelemetryHook) AfterProcessJob(evt *taskq.ProcessJobEvent) error {
	ctx := evt.Ctx

	span := trace.SpanFromContext(ctx)
	defer span.End()

	if err := evt.Job.Err; err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}

	return nil
}
