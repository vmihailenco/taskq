package taskqext

import (
	"reflect"

	"github.com/vmihailenco/taskq/v3"
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/trace"
	"google.golang.org/grpc/codes"
)

type OpenTelemetryHook struct{}

var _ taskq.ConsumerHook = (*OpenTelemetryHook)(nil)

func (h *OpenTelemetryHook) BeforeProcessMessage(evt *taskq.ProcessMessageEvent) error {
	tracer := global.Tracer("github.com/vmihailenco/taskq")

	ctx, _ := tracer.Start(evt.Message.Ctx, evt.Message.TaskName)
	evt.Message.Ctx = ctx
	return nil
}

func (h *OpenTelemetryHook) AfterProcessMessage(evt *taskq.ProcessMessageEvent) error {
	ctx := evt.Message.Ctx
	span := trace.SpanFromContext(ctx)

	if err := evt.Message.Err; err != nil {
		span.SetStatus(codes.Internal, "")
		span.AddEvent(ctx, "error",
			kv.String("error.type", reflect.TypeOf(err).String()),
			kv.String("error.message", err.Error()),
		)
	}

	span.End()

	return nil
}
