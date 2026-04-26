package server

import (
	"context"
	"log"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"github.com/octodb/octodb/internal/store"
)

// TraceServer implements the OTLP TraceService gRPC endpoint.
type TraceServer struct {
	collectortrace.UnimplementedTraceServiceServer
	store store.Store
}

// NewTraceServer creates a new TraceServer backed by the given Store.
func NewTraceServer(s store.Store) *TraceServer {
	return &TraceServer{store: s}
}

// Export receives OTLP trace batches, writes them to the Store, and ACKs.
func (s *TraceServer) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	p, ok := peer.FromContext(ctx)
	addr := "unknown"
	if ok {
		addr = p.Addr.String()
	}
	log.Printf("[Trace] Export from %s — %d ResourceSpans", addr, len(req.ResourceSpans))

	const tenantID = "default"
	if err := s.store.WriteTraces(ctx, tenantID, req.ResourceSpans); err != nil {
		log.Printf("[Trace] WriteTraces failed: %v", err)
		return nil, err
	}
	return &collectortrace.ExportTraceServiceResponse{}, nil
}

// LogServer implements the OTLP LogService gRPC endpoint.
type LogServer struct {
	collectorlogs.UnimplementedLogsServiceServer
	store store.Store
}

// NewLogServer creates a new LogServer backed by the given Store.
func NewLogServer(s store.Store) *LogServer {
	return &LogServer{store: s}
}

// Export receives OTLP log batches, writes them to the Store, and ACKs.
func (s *LogServer) Export(ctx context.Context, req *collectorlogs.ExportLogsServiceRequest) (*collectorlogs.ExportLogsServiceResponse, error) {
	p, ok := peer.FromContext(ctx)
	addr := "unknown"
	if ok {
		addr = p.Addr.String()
	}
	log.Printf("[Log] Export from %s — %d ResourceLogs", addr, len(req.ResourceLogs))

	const tenantID = "default"
	if err := s.store.WriteLogs(ctx, tenantID, req.ResourceLogs); err != nil {
		log.Printf("[Log] WriteLogs failed: %v", err)
		return nil, err
	}
	return &collectorlogs.ExportLogsServiceResponse{}, nil
}

// MetricServer implements the OTLP MetricsService gRPC endpoint.
type MetricServer struct {
	collectormetrics.UnimplementedMetricsServiceServer
	store store.Store
}

// NewMetricServer creates a new MetricServer backed by the given Store.
func NewMetricServer(s store.Store) *MetricServer {
	return &MetricServer{store: s}
}

// Export receives OTLP metric batches, writes them to the Store, and ACKs.
func (s *MetricServer) Export(ctx context.Context, req *collectormetrics.ExportMetricsServiceRequest) (*collectormetrics.ExportMetricsServiceResponse, error) {
	p, ok := peer.FromContext(ctx)
	addr := "unknown"
	if ok {
		addr = p.Addr.String()
	}
	log.Printf("[Metric] Export from %s — %d ResourceMetrics", addr, len(req.ResourceMetrics))

	const tenantID = "default"
	if err := s.store.WriteMetrics(ctx, tenantID, req.ResourceMetrics); err != nil {
		log.Printf("[Metric] WriteMetrics failed: %v", err)
		return nil, err
	}
	return &collectormetrics.ExportMetricsServiceResponse{}, nil
}

// RegisterAll registers TraceServer, LogServer, and MetricServer on the grpc.Server.
func RegisterAll(s *grpc.Server, st store.Store) {
	collectortrace.RegisterTraceServiceServer(s, NewTraceServer(st))
	collectorlogs.RegisterLogsServiceServer(s, NewLogServer(st))
	collectormetrics.RegisterMetricsServiceServer(s, NewMetricServer(st))
}
