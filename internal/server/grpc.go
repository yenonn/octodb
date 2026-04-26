package server

import (
	"context"
	"log"

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
	log.Printf("[Block1] Export from %s — %d ResourceSpans", addr, len(req.ResourceSpans))

	// Block 1: tenant routing deferred. All data lands in the default tenant.
	// Block 1: tenant resolution is intentionally skipped.
	// Tenant identity (mTLS OU, dynamic resource attributes) is a Block 2 concern.
	const tenantID = "default"

	if err := s.store.WriteTraces(ctx, tenantID, req.ResourceSpans); err != nil {
		log.Printf("[Block1] WriteTraces failed: %v", err)
		return nil, err
	}

	return &collectortrace.ExportTraceServiceResponse{}, nil
}

// Register registers the TraceServer with the provided grpc.Server.
func Register(s *grpc.Server, ts *TraceServer) {
	collectortrace.RegisterTraceServiceServer(s, ts)
}
