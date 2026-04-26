package main

import (
	"context"
	"fmt"
	"os"
	"time"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := os.Getenv("OCTODB_ADDR")
	if addr == "" {
		addr = "localhost:4317"
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := collectortrace.NewTraceServiceClient(conn)

	// Build a minimal OTLP ResourceSpans batch
	now := time.Now()
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"}}},
						{Key: "k8s.namespace.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "team-payments"}}},
					},
				},
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Scope: &commonpb.InstrumentationScope{Name: "test-scope"},
						Spans: []*tracepb.Span{
							{
								TraceId:           []byte("abcdef1234567890abcdef1234567890"),
								SpanId:            []byte("abc123def4567890"),
								ParentSpanId:      nil,
								Name:              "test-span-1",
								StartTimeUnixNano: uint64(now.UnixNano()),
								EndTimeUnixNano:   uint64(now.Add(10 * time.Millisecond).UnixNano()),
								Kind:              tracepb.Span_SPAN_KIND_INTERNAL,
								Attributes: []*commonpb.KeyValue{
									{Key: "http.status_code", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 200}}},
								},
							},
						},
					},
				},
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Export(ctx, req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Export failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Export OK. Partial error count: %d\n", len(resp.PartialSuccess.GetErrorMessage()))
	fmt.Printf("Sent %d ResourceSpans to %s\n", len(req.ResourceSpans), addr)
}
