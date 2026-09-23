package tracing

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

func TestInitDisabledIsNoop(t *testing.T) {
	t.Setenv("FIREPAAS_TRACING_ENABLED", "")
	shutdown := Init(context.Background(), "test")
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	// noop provider 的 span 恒不采样。
	_, span := otel.Tracer("test").Start(context.Background(), "x")
	if span.SpanContext().IsSampled() {
		t.Fatal("disabled tracing must not sample")
	}
	span.End()
	// 非法采样率回退默认不断档。
	t.Setenv("FIREPAAS_TRACING_ENABLED", "true")
	t.Setenv("FIREPAAS_TRACING_SAMPLE_RATIO", "bogus")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:1") // 不可达也只影响导出
	shutdown = Init(context.Background(), "test")
	_ = shutdown(context.Background())
	Init(context.Background(), "test-off") // 复位 noop，避免污染其它测试
	t.Setenv("FIREPAAS_TRACING_ENABLED", "")
	Init(context.Background(), "test-off")
}

func TestMDCarrierRoundTrip(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})
	// 显式合法 SpanContext（不依赖全局 provider 是否采样）。
	traceID, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled, Remote: true,
	})
	ctx := trace.ContextWithRemoteSpanContext(context.Background(), sc)
	out := InjectToMetadata(ctx)
	md, ok := metadata.FromOutgoingContext(out)
	if !ok {
		t.Fatal("injected metadata missing")
	}
	tp := md.Get("traceparent")
	if len(tp) == 0 || !strings.HasPrefix(tp[0], "00-4bf92f3577b34da6a3ce929d0e0e4736-") {
		t.Fatalf("traceparent = %v", tp)
	}
	// 服务端续接：extract 出同一 trace。
	ctx2 := metadata.NewIncomingContext(context.Background(), md)
	ctx2 = ExtractFromMetadata(ctx2)
	got := trace.SpanContextFromContext(ctx2).TraceID()
	if got != traceID {
		t.Fatalf("trace %s != %s", got, traceID)
	}
}

func TestExtractWithoutMetadata(t *testing.T) {
	ctx := ExtractFromMetadata(context.Background())
	if ctx == nil {
		t.Fatal("must return context")
	}
}
