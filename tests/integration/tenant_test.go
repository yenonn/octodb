package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// buildTestResourceSpansWithAttrs creates a ResourceSpan with custom resource attributes.
func buildTestResourceSpansWithAttrs(attrs []*commonpb.KeyValue, spanName string) *tracepb.ResourceSpans {
	now := time.Now().UnixNano()
	return &tracepb.ResourceSpans{
		Resource: &resourcepb.Resource{
			Attributes: attrs,
		},
		ScopeSpans: []*tracepb.ScopeSpans{
			{
				Scope: &commonpb.InstrumentationScope{Name: "test-scope"},
				Spans: []*tracepb.Span{
					{
						TraceId:           []byte("1234567890abcdef1234567890abcdef"),
						SpanId:            []byte("abcd1234efgh5678"),
						Name:              spanName,
						StartTimeUnixNano: uint64(now),
						EndTimeUnixNano:   uint64(now + 1e6),
						Kind:              tracepb.Span_SPAN_KIND_INTERNAL,
					},
				},
			},
		},
	}
}

// mustDialOrCached connects to the gRPC server with retry logic, caching the
// connection for reuse within a single test to avoid port exhaustion.
func mustDialOrCached(t *testing.T) *grpc.ClientConn {
	t.Helper()
	var conn *grpc.ClientConn
	var err error
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		conn, err = grpc.DialContext(ctx, "localhost:4317", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return conn
}

// sendTraceBatch sends multiple ResourceSpans in a single OTLP Export call.
func sendTraceBatch(t *testing.T, dataDir string, spans []*tracepb.ResourceSpans) {
	t.Helper()
	conn := mustDialOrCached(t)
	defer conn.Close()

	client := collectortrace.NewTraceServiceClient(conn)
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: spans,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Export(ctx, req); err != nil {
		t.Fatalf("batch export: %v", err)
	}
}

// queryTenantTraces fetches the trace count for a specific tenant via HTTP.
func queryTenantTraces(t *testing.T, tenantID string) float64 {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://localhost:8080/v1/traces?tenant=%s", tenantID))
	if err != nil {
		t.Fatalf("http get traces tenant=%q: %v", tenantID, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("http status %d for tenant=%q: %s", resp.StatusCode, tenantID, string(body))
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("json unmarshal for tenant=%q: %v", tenantID, err)
	}
	count, ok := result["count"].(float64)
	if !ok {
		t.Fatalf("unexpected response for tenant=%q: %v", tenantID, result)
	}
	return count
}

// TestTenant_MultiTenantIngestionAndIsolation sends data for two tenants in a
// single OTLP batch, triggers a flush, and asserts each tenant only sees its
// own data.
func TestTenant_MultiTenantIngestionAndIsolation(t *testing.T) {
	bin := buildServer(t)
	dataDir := filepath.Join(t.TempDir(), "octodb-data")
	_ = os.MkdirAll(dataDir, 0755)

	cmd := startServer(t, bin, dataDir)
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	time.Sleep(1 * time.Second)

	// Build a batch with both tenants in one Export call.
	batch := []*tracepb.ResourceSpans{
		buildTestResourceSpansWithAttrs([]*commonpb.KeyValue{
			{Key: "tenant.id", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "payments"}}},
			{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "payment-svc"}}},
		}, "payment-span-1"),
		buildTestResourceSpansWithAttrs([]*commonpb.KeyValue{
			{Key: "tenant.id", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "payments"}}},
			{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "payment-svc"}}},
		}, "payment-span-2"),
		buildTestResourceSpansWithAttrs([]*commonpb.KeyValue{
			{Key: "tenant.id", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "search"}}},
			{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "search-svc"}}},
		}, "search-span-1"),
	}

	// Send enough data (50 times) to exceed the 1KB memtable threshold.
	for i := 0; i < 50; i++ {
		for _, span := range batch {
			span.ScopeSpans[0].Spans[0].Name = fmt.Sprintf("%d-%s", i, span.Resource.Attributes[1].Value.GetStringValue())
		}
		sendTraceBatch(t, dataDir, batch)
	}

	// Wait for background flush.
	time.Sleep(3 * time.Second)

	// Query each tenant — isolation must hold.
	paymentsCount := queryTenantTraces(t, "payments")
	searchCount := queryTenantTraces(t, "search")
	defaultCount := queryTenantTraces(t, "default")

	if paymentsCount == 0 {
		t.Fatalf("expected payments tenant to have traces, got 0")
	}
	if searchCount == 0 {
		t.Fatalf("expected search tenant to have traces, got 0")
	}
	if defaultCount != 0 {
		t.Fatalf("expected default tenant to have 0 traces (no default data), got %.0f", defaultCount)
	}

	t.Logf("Tenant isolation passed: payments=%.0f search=%.0f default=%.0f", paymentsCount, searchCount, defaultCount)
}

// TestTenant_CrashRecovery sends multi-tenant data, crashes, restarts, and
// asserts per-tenant isolation is preserved.
func TestTenant_CrashRecovery(t *testing.T) {
	bin := buildServer(t)
	dataDir := filepath.Join(t.TempDir(), "octodb-data")
	_ = os.MkdirAll(dataDir, 0755)

	cmd := startServer(t, bin, dataDir)
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	time.Sleep(1 * time.Second)

	// Send interleaved tenants.
	for i := 0; i < 30; i++ {
		var tenant, svc string
		if i%2 == 0 {
			tenant = "alpha"
			svc = "alpha-svc"
		} else {
			tenant = "beta"
			svc = "beta-svc"
		}
		rs := buildTestResourceSpansWithAttrs([]*commonpb.KeyValue{
			{Key: "tenant.id", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: tenant}}},
			{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: svc}}},
		}, fmt.Sprintf("span-%d", i))
		sendTraceBatch(t, dataDir, []*tracepb.ResourceSpans{rs})
	}

	time.Sleep(3 * time.Second)

	// Assert pre-crash counts.
	alphaCount := queryTenantTraces(t, "alpha")
	betaCount := queryTenantTraces(t, "beta")
	if alphaCount == 0 || betaCount == 0 {
		t.Fatalf("pre-crash: expected alpha and beta to both have traces, got alpha=%.0f beta=%.0f", alphaCount, betaCount)
	}
	t.Logf("Pre-crash: alpha=%.0f beta=%.0f", alphaCount, betaCount)

	// --- Crash ---
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("sigkill: %v", err)
	}
	_ = cmd.Wait()

	// --- Restart ---
	cmd2 := startServer(t, bin, dataDir)
	defer func() {
		if cmd2.Process != nil {
			_ = cmd2.Process.Kill()
			_ = cmd2.Wait()
		}
	}()
	time.Sleep(2 * time.Second)

	// Post-crash isolation must still hold.
	alphaAfter := queryTenantTraces(t, "alpha")
	betaAfter := queryTenantTraces(t, "beta")
	if alphaAfter == 0 || betaAfter == 0 {
		t.Fatalf("post-crash: expected alpha and beta to both have traces, got alpha=%.0f beta=%.0f", alphaAfter, betaAfter)
	}
	if alphaAfter != alphaCount {
		t.Fatalf("alpha count changed after crash: pre=%.0f post=%.0f", alphaCount, alphaAfter)
	}
	if betaAfter != betaCount {
		t.Fatalf("beta count changed after crash: pre=%.0f post=%.0f", betaCount, betaAfter)
	}

	t.Logf("Post-crash: alpha=%.0f beta=%.0f", alphaAfter, betaAfter)
	t.Log("Tenant crash recovery PASSED")
}

// TestTenant_AttributeFallback verifies tenant resolution via fallback attribute
// keys: tenant_id and k8s.namespace.name, when tenant.id is absent.
func TestTenant_AttributeFallback(t *testing.T) {
	bin := buildServer(t)
	dataDir := filepath.Join(t.TempDir(), "octodb-data")
	_ = os.MkdirAll(dataDir, 0755)

	cmd := startServer(t, bin, dataDir)
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	time.Sleep(1 * time.Second)

	// 50 spans with tenant_id=legacy-team (not tenant.id).
	for i := 0; i < 50; i++ {
		rs := buildTestResourceSpansWithAttrs([]*commonpb.KeyValue{
			{Key: "tenant_id", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "legacy-team"}}},
			{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "legacy-svc"}}},
		}, fmt.Sprintf("legacy-span-%d", i))
		sendTraceBatch(t, dataDir, []*tracepb.ResourceSpans{rs})
	}

	// 50 spans with k8s.namespace.name=kube-ns (neither tenant.id nor tenant_id).
	for i := 0; i < 50; i++ {
		rs := buildTestResourceSpansWithAttrs([]*commonpb.KeyValue{
			{Key: "k8s.namespace.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "production-ns"}}},
			{Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "kube-svc"}}},
		}, fmt.Sprintf("kube-span-%d", i))
		sendTraceBatch(t, dataDir, []*tracepb.ResourceSpans{rs})
	}

	time.Sleep(3 * time.Second)

	legacyCount := queryTenantTraces(t, "legacy-team")
	kubeCount := queryTenantTraces(t, "production-ns")

	if legacyCount == 0 {
		t.Fatalf("expected legacy-team via tenant_id fallback, got 0")
	}
	if kubeCount == 0 {
		t.Fatalf("expected production-ns via k8s.namespace.name fallback, got 0")
	}

	t.Logf("Fallback attribute resolution passed: legacy-team=%.0f production-ns=%.0f", legacyCount, kubeCount)
}
