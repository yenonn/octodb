package server

import (
	"context"
	"fmt"
	"log"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	"github.com/octodb/octodb/internal/store"
	"github.com/octodb/octodb/internal/tenant"
)

// TraceServer implements the OTLP TraceService gRPC endpoint.
type TraceServer struct {
	collectortrace.UnimplementedTraceServiceServer
	store     store.Store
	resolver  tenant.Resolver
}

// NewTraceServer creates a new TraceServer backed by the given Store and Resolver.
func NewTraceServer(s store.Store, resolver tenant.Resolver) *TraceServer {
	return &TraceServer{store: s, resolver: resolver}
}

// Export receives OTLP trace batches, resolves tenant from resource attributes,
// groups per-tenant, and writes to the Store.
func (s *TraceServer) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	p, ok := peer.FromContext(ctx)
	addr := "unknown"
	if ok {
		addr = p.Addr.String()
	}
	log.Printf("[Trace] Export from %s — %d ResourceSpans", addr, len(req.ResourceSpans))

	groups := tenant.ResolveBatch(req.ResourceSpans, func(rs *tracepb.ResourceSpans) *resourcepb.Resource {
		if rs == nil {
			return nil
		}
		return rs.Resource
	}, s.resolver)

	for tenantID, spans := range groups {
		if err := s.store.WriteTraces(ctx, tenantID, spans); err != nil {
			log.Printf("[Trace] WriteTraces failed for tenant %q: %v", tenantID, err)
			return nil, fmt.Errorf("write traces for tenant %q: %w", tenantID, err)
		}
	}
	return &collectortrace.ExportTraceServiceResponse{}, nil
}

// LogServer implements the OTLP LogService gRPC endpoint.
type LogServer struct {
	collectorlogs.UnimplementedLogsServiceServer
	store     store.Store
	resolver  tenant.Resolver
}

// NewLogServer creates a new LogServer backed by the given Store and Resolver.
func NewLogServer(s store.Store, resolver tenant.Resolver) *LogServer {
	return &LogServer{store: s, resolver: resolver}
}

// Export receives OTLP log batches, resolves tenant per-Resource, and writes.
func (s *LogServer) Export(ctx context.Context, req *collectorlogs.ExportLogsServiceRequest) (*collectorlogs.ExportLogsServiceResponse, error) {
	p, ok := peer.FromContext(ctx)
	addr := "unknown"
	if ok {
		addr = p.Addr.String()
	}
	log.Printf("[Log] Export from %s — %d ResourceLogs", addr, len(req.ResourceLogs))

	groups := tenant.ResolveBatch(req.ResourceLogs, func(rl *logspb.ResourceLogs) *resourcepb.Resource {
		if rl == nil {
			return nil
		}
		return rl.Resource
	}, s.resolver)

	for tenantID, logs := range groups {
		if err := s.store.WriteLogs(ctx, tenantID, logs); err != nil {
			log.Printf("[Log] WriteLogs failed for tenant %q: %v", tenantID, err)
			return nil, fmt.Errorf("write logs for tenant %q: %w", tenantID, err)
		}
	}
	return &collectorlogs.ExportLogsServiceResponse{}, nil
}

// MetricServer implements the OTLP MetricsService gRPC endpoint.
type MetricServer struct {
	collectormetrics.UnimplementedMetricsServiceServer
	store     store.Store
	resolver  tenant.Resolver
}

// NewMetricServer creates a new MetricServer backed by the given Store and Resolver.
func NewMetricServer(s store.Store, resolver tenant.Resolver) *MetricServer {
	return &MetricServer{store: s, resolver: resolver}
}

// Export receives OTLP metric batches, resolves tenant per-Resource, and writes.
func (s *MetricServer) Export(ctx context.Context, req *collectormetrics.ExportMetricsServiceRequest) (*collectormetrics.ExportMetricsServiceResponse, error) {
	p, ok := peer.FromContext(ctx)
	addr := "unknown"
	if ok {
		addr = p.Addr.String()
	}
	log.Printf("[Metric] Export from %s — %d ResourceMetrics", addr, len(req.ResourceMetrics))

	groups := tenant.ResolveBatch(req.ResourceMetrics, func(rm *metricspb.ResourceMetrics) *resourcepb.Resource {
		if rm == nil {
			return nil
		}
		return rm.Resource
	}, s.resolver)

	for tenantID, metrics := range groups {
		if err := s.store.WriteMetrics(ctx, tenantID, metrics); err != nil {
			log.Printf("[Metric] WriteMetrics failed for tenant %q: %v", tenantID, err)
			return nil, fmt.Errorf("write metrics for tenant %q: %w", tenantID, err)
		}
	}
	return &collectormetrics.ExportMetricsServiceResponse{}, nil
}

// RegisterAll registers TraceServer, LogServer, and MetricServer on the grpc.Server
// using the provided Store and tenant Resolver.
func RegisterAll(s *grpc.Server, st store.Store, resolver tenant.Resolver) {
	collectortrace.RegisterTraceServiceServer(s, NewTraceServer(st, resolver))
	collectorlogs.RegisterLogsServiceServer(s, NewLogServer(st, resolver))
	collectormetrics.RegisterMetricsServiceServer(s, NewMetricServer(st, resolver))
}
