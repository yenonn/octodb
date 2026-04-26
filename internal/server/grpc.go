package server

import (
	"context"
	"log"

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

// RegisterTrace registers the TraceServer with the provided grpc.Server.
func RegisterTrace(s *grpc.Server, ts *TraceServer) {
	collectortrace.RegisterTraceServiceServer(s, ts)
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

// RegisterLog registers the LogServer with the provided grpc.Server.
func RegisterLog(s *grpc.Server, ls *LogServer) {
	collectorlogs.RegisterLogsServiceServer(s, ls)
}

// RegisterAll registers both TraceServer and LogServer on the grpc.Server.
func RegisterAll(s *grpc.Server, st store.Store) {
	RegisterTrace(s, NewTraceServer(st))
	RegisterLog(s, NewLogServer(st))
}
