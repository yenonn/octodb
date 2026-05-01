// Package integration provides end-to-end tests for OctoDB blocks.
//
// These tests compile the octodb binary, start it as a subprocess,
// send real OTLP data via gRPC, simulate crashes, and assert durability.
package integration

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"github.com/octodb/octodb/internal/wal"
)

// buildServer compiles cmd/octodb into a temp binary.
func buildServer(tb testing.TB) string {
	tb.Helper()
	bin := filepath.Join(tb.TempDir(), "octodb")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/octodb/octodb/cmd/octodb")
	cmd.Dir = projectRoot(tb)
	out, err := cmd.CombinedOutput()
	if err != nil {
		tb.Fatalf("build server: %v\n%s", err, out)
	}
	return bin
}

// projectRoot returns the module root (../.. from this file).
func projectRoot(tb testing.TB) string {
	tb.Helper()
	_, f, _, ok := runtime.Caller(0)
	if !ok {
		tb.Fatal("cannot get caller")
	}
	return filepath.Dir(filepath.Dir(filepath.Dir(f)))
}

// startServer runs the octodb binary with a temp data dir.
func startServer(tb testing.TB, bin string, dataDir string) *exec.Cmd {
	tb.Helper()
	cmd := exec.Command(bin, "-config", "none")
	cmd.Env = append(os.Environ(),
		"OCTODB_GRPC_ADDR=:4317",
		"OCTODB_DATA_DIR="+dataDir,
		"OCTODB_MEMTABLE_FLUSH_THRESHOLD=1024", // 1KB for fast flush in tests
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		tb.Fatalf("start server: %v", err)
	}
	return cmd
}

func TestBlock1_WALCrashRecovery(t *testing.T) {
	// Build server binary.
	bin := buildServer(t)

	// Clean data dir.
	dataDir := filepath.Join(t.TempDir(), "octodb-data")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatal(err)
	}

	// --- Step 1: Start OctoDB ---
	cmd := startServer(t, bin, dataDir)
	defer func() {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	time.Sleep(1 * time.Second)

	// --- Step 2: Send OTLP Export ---
	sendExport(t, dataDir, buildTestResourceSpans("integration-test-svc", "test-span"))

	// --- Step 3: Verify WAL has bytes ---
	walPath := filepath.Join(dataDir, "tenant", "default", "000001.wal")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("wal stat: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("wal is empty after export")
	}
	t.Logf("wal size after first export: %d bytes", info.Size())

	// --- Step 4: Kill -9 (simulate crash) ---
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("sigkill: %v", err)
	}
	_ = cmd.Wait()

	// --- Step 5: Verify WAL still on disk after crash ---
	info, err = os.Stat(walPath)
	if os.IsNotExist(err) {
		t.Fatal("wal missing after crash")
	}
	if info.Size() == 0 {
		t.Fatal("wal empty after crash")
	}
	t.Logf("wal size after crash: %d bytes", info.Size())

	// --- Step 6: Replay WAL and assert exact match ---
	replayed, err := wal.ReplayToSlice(walPath)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(replayed) != 1 {
		t.Fatalf("expected 1 record, got %d", len(replayed))
	}

	expected := buildTestResourceSpans("integration-test-svc", "test-span")

	if expected.Resource.Attributes[0].Value.GetStringValue() !=
		replayed[0].Resource.Attributes[0].Value.GetStringValue() {
		t.Fatal("replayed service.name does not match")
	}
	if len(replayed[0].ScopeSpans) == 0 || len(replayed[0].ScopeSpans[0].Spans) == 0 {
		t.Fatal("replayed spans missing")
	}
	if replayed[0].ScopeSpans[0].Spans[0].Name != expected.ScopeSpans[0].Spans[0].Name {
		t.Fatalf("replayed span name mismatch: got %q, want %q",
			replayed[0].ScopeSpans[0].Spans[0].Name,
			expected.ScopeSpans[0].Spans[0].Name)
	}
	if replayed[0].ScopeSpans[0].Spans[0].Kind != expected.ScopeSpans[0].Spans[0].Kind {
		t.Fatal("replayed span kind mismatch")
	}

	t.Log("Block 1 integration test PASSED: WAL survives crash and replays exactly")
}

// sendExport connects to the running server and sends one ResourceSpans batch.
func sendExport(t *testing.T, dataDir string, rs *tracepb.ResourceSpans) {
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
	defer conn.Close()

	client := collectortrace.NewTraceServiceClient(conn)
	req := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{rs},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Export(ctx, req); err != nil {
		t.Fatalf("export: %v", err)
	}
}

// sendLogExport sends one ResourceLogs batch to the server.
func sendLogExport(t *testing.T, dataDir string, rl *logspb.ResourceLogs) {
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
	defer conn.Close()

	client := collectorlogs.NewLogsServiceClient(conn)
	req := &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{rl},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Export(ctx, req); err != nil {
		t.Fatalf("log export: %v", err)
	}
}

// buildTestResourceSpans creates a minimal ResourceSpans proto for tests.
func buildTestResourceSpans(svc, spanName string) *tracepb.ResourceSpans {
	now := time.Now().UnixNano()
	return &tracepb.ResourceSpans{
		Resource: &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{
					Key:   "service.name",
					Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: svc}},
				},
			},
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
						Attributes: []*commonpb.KeyValue{
							{
								Key:   "http.status_code",
								Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 200}},
							},
						},
					},
				},
			},
		},
	}
}

// buildTestResourceLogs creates a minimal ResourceLogs proto for tests.
func buildTestResourceLogs(svc, body string, severity logspb.SeverityNumber) *logspb.ResourceLogs {
	now := uint64(time.Now().UnixNano())
	return &logspb.ResourceLogs{
		Resource: &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{
					Key:   "service.name",
					Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: svc}},
				},
			},
		},
		ScopeLogs: []*logspb.ScopeLogs{
			{
				Scope: &commonpb.InstrumentationScope{Name: "test-scope"},
				LogRecords: []*logspb.LogRecord{
					{
						ObservedTimeUnixNano: now,
						Body:                 &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: body}},
						SeverityNumber:       severity,
						SeverityText:         severity.String(),
						TraceId:              []byte("1234567890abcdef1234567890abcdef"),
						SpanId:               []byte("abcd1234efgh5678"),
					},
				},
			},
		},
	}
}

// Test helper: assert file exists.
func assertFileExists(t *testing.T, path string, minSize int64) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("expected file %s to exist: %v", path, err)
	}
	if info.Size() < minSize {
		t.Fatalf("expected file %s to be >= %d bytes, got %d", path, minSize, info.Size())
	}
}

// Test helper: assert file does not exist.
func assertNoFile(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected file %s NOT to exist", path)
	}
}

// Test helper: get file size.
func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}
