// Package tracing 是三进程共用的 OTel trace 接线（Wave6，最小闭环）。
//
// 默认关闭（FIREPAAS_TRACING_ENABLED=true 开启），关闭时 Init 装 noop
// provider：全仓 `otel.Tracer(...).Start` 零开销、无行为变化。开启时经 OTLP
// gRPC 上报中心 collector（见 iac/observability/otel-collector.yaml）。
//
// 传播：W3C traceparent/tracestate（OTel 标准 propagator），与既有
// X-Firepaas-Request-ID 并存——request-id 仍是日志主关联，span 属性记
// request.id 实现互跳。凭证头永不进属性（AGENTS.md 数据边界）。
package tracing

import (
	"context"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc/metadata"
)

// HeaderTraceparent/HeaderTracestate 是透传头（agent 转 guest 前剥离，
// guest 是用户镜像不可信——与 request-id 同纪律）。
const (
	HeaderTraceparent = "traceparent"
	HeaderTracestate  = "tracestate"
)

// Init 按环境变量初始化全局 trace 接线，返回 shutdown（进程退出时调用，
// flush 残留 span）。关闭时返回 noop shutdown。
//
//	FIREPAAS_TRACING_ENABLED: true 开启（默认关闭）
//	OTEL_EXPORTER_OTLP_ENDPOINT: collector 地址（默认 localhost:4317，明文；
//	  生产 TLS 由 collector 前置，见 iac 示例）
//	FIREPAAS_TRACING_SAMPLE_RATIO: 采样率 (0,1]（默认 0.01；parent-based）
func Init(ctx context.Context, serviceName string) func(context.Context) error {
	noopShutdown := func(context.Context) error { return nil }
	// 透传头行为在开关两边一致（edge→proxy→guest 剥离纪律不变）：disabled
	// 时 span 为 noop（不采样、不导出；noop 上下文 Inject 不写 traceparent，
	// 开启采样后新请求自然带上）。每请求仅两次 header 编解码，外部不可见。
	otel.SetTextMapPropagator(propagation.TraceContext{})
	if !strings.EqualFold(strings.TrimSpace(os.Getenv("FIREPAAS_TRACING_ENABLED")), "true") {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return noopShutdown
	}
	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if endpoint == "" {
		endpoint = "localhost:4317"
	}
	ratio := 0.01
	if raw := strings.TrimSpace(os.Getenv("FIREPAAS_TRACING_SAMPLE_RATIO")); raw != "" {
		if f, err := strconv.ParseFloat(raw, 64); err == nil && f > 0 && f <= 1 {
			ratio = f
		}
	}
	exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure())
	if err != nil {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return noopShutdown
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			attribute.String("firepaas.cell", strings.TrimSpace(os.Getenv("FIREPAAS_CELL"))),
		))
	if err != nil {
		res = resource.Default()
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp.Shutdown
}

// MDCarrier 让 W3C propagator 读写 gRPC metadata（dispatch 链 controller→agent）。
type MDCarrier metadata.MD

// Get 实现 propagation.TextMapCarrier.
func (c MDCarrier) Get(key string) string {
	vals := metadata.MD(c).Get(key)
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

// Set 实现 propagation.TextMapCarrier.
func (c MDCarrier) Set(key, value string) {
	metadata.MD(c).Set(key, value)
}

// Keys 实现 propagation.TextMapCarrier.
func (c MDCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// InjectToMetadata 把当前 span 上下文注入外发 gRPC metadata（controller 派发侧）。
func InjectToMetadata(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	otel.GetTextMapPropagator().Inject(ctx, MDCarrier(md))
	return metadata.NewOutgoingContext(ctx, md)
}

// ExtractFromMetadata 从入站 gRPC metadata 续接 trace（agent server 侧）。
func ExtractFromMetadata(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, MDCarrier(md))
}

// StartServerSpan 为入站请求建 server span（attrs 仅限 allowlist 业务字段：
// host/machine/execution/generation/operation/request_id——凭证类永不进入）。
func StartServerSpan(
	ctx context.Context,
	tracer, spanName string,
	attrs ...attribute.KeyValue,
) (context.Context, trace.Span) {
	ctx, span := otel.Tracer(tracer).Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindServer))
	if len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
	return ctx, span
}
