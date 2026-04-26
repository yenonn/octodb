package server

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	grpcpeer "google.golang.org/grpc/peer"

	"github.com/octodb/octodb/internal/store"
)

func setupStore(t *testing.T) store.Store {
	t.Helper()
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	st, err := store.NewBlock2Store(dataDir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestTraceServerExportWithPeer(t *testing.T) {
	st := setupStore(t)
	srv := NewTraceServer(st)

	// Create a context with peer information.
	ctx := grpcpeer.NewContext(context.Background(), &grpcpeer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345},
	})

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "peer-svc"}}},
					},
				},
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Spans: []*tracepb.Span{
							{
								TraceId: []byte("1234567890abcdef1234567890abcdef"),
								SpanId:  []byte("abcd1234efgh5678"),
								Name:    "peer-span",
							},
						},
					},
				},
			},
		},
	}

	resp, err := srv.Export(ctx, req)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestLogServerExportWithPeer(t *testing.T) {
	st := setupStore(t)
	srv := NewLogServer(st)

	ctx := grpcpeer.NewContext(context.Background(), &grpcpeer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345},
	})

	req := &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "peer-log"}}},
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						LogRecords: []*logspb.LogRecord{
							{
								ObservedTimeUnixNano: 1000000,
								Body:                 &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "peer log"}},
							},
						},
					},
				},
			},
		},
	}

	resp, err := srv.Export(ctx, req)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestMetricServerExportWithPeer(t *testing.T) {
	st := setupStore(t)
	srv := NewMetricServer(st)

	ctx := grpcpeer.NewContext(context.Background(), &grpcpeer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345},
	})

	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "peer-metric"}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Metrics: []*metricspb.Metric{
							{
								Name: "cpu",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{TimeUnixNano: 1000000, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.0}},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	resp, err := srv.Export(ctx, req)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestTraceServerExport(t *testing.T) {
	st := setupStore(t)
	srv := NewTraceServer(st)

	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test-svc"}}},
					},
				},
				ScopeSpans: []*tracepb.ScopeSpans{
					{
						Spans: []*tracepb.Span{
							{
								TraceId: []byte("1234567890abcdef1234567890abcdef"),
								SpanId:  []byte("abcd1234efgh5678"),
								Name:    "test-span",
							},
						},
					},
				},
			},
		},
	}

	resp, err := srv.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestLogServerExport(t *testing.T) {
	st := setupStore(t)
	srv := NewLogServer(st)

	req := &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "log-svc"}}},
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						LogRecords: []*logspb.LogRecord{
							{
								ObservedTimeUnixNano: 1000000,
								Body:                 &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "test log"}},
								SeverityNumber:       logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
							},
						},
					},
				},
			},
		},
	}

	resp, err := srv.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestMetricServerExport(t *testing.T) {
	st := setupStore(t)
	srv := NewMetricServer(st)

	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "metric-svc"}}},
					},
				},
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Metrics: []*metricspb.Metric{
							{
								Name: "cpu.usage",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{
												TimeUnixNano: 1000000,
												Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 42.5},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	resp, err := srv.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("Export: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestRegisterAll(t *testing.T) {
	st := setupStore(t)
	s := grpc.NewServer()
	RegisterAll(s, st)
	s.Stop()
}
