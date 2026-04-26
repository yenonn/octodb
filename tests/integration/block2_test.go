package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
)

// TestBlock2_FlushAndQuery sends enough data to trigger a memtable flush,
// verifies segment + manifest creation, queries via HTTP, survives a crash,
// and asserts data is still queryable after restart.
func TestBlock2_FlushAndQuery(t *testing.T) {
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

	// Send 100 exports to exceed 1KB memtable threshold.
	for i := 0; i < 100; i++ {
		spanName := fmt.Sprintf("flush-span-%d", i)
		sendExport(t, dataDir, buildTestResourceSpans("flush-service", spanName))
	}

	// Wait for background flush.
	time.Sleep(3 * time.Second)

	// Verify flush artifacts.
	traceDir := filepath.Join(dataDir, "traces")
	manifestPath := filepath.Join(traceDir, "manifest.log")
	checkpointPath := filepath.Join(traceDir, "manifest.log.checkpoint")
	assertFileExists(t, manifestPath, 1)
	assertFileExists(t, checkpointPath, 1)

	matches, err := filepath.Glob(filepath.Join(traceDir, "sst-*.pb"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) == 0 {
		t.Fatal("expected at least one segment file after flush, got none")
	}
	for _, seg := range matches {
		assertFileExists(t, seg, 1)
		t.Logf("segment file: %s (size %d)", seg, fileSize(t, seg))
	}

	walPath := filepath.Join(traceDir, "traces.wal")
	walInfo, _ := os.Stat(walPath)
	if walInfo != nil {
		t.Logf("wal size after flush: %d", walInfo.Size())
	}

	// --- Phase C: Query via HTTP ---
	resp, err := http.Get("http://localhost:8080/v1/traces?tenant=default")
	if err != nil {
		t.Fatalf("http get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("http status %d: %s", resp.StatusCode, string(body))
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	count, ok := result["count"].(float64)
	if !ok {
		t.Fatalf("unexpected response: %v", result)
	}
	if count == 0 {
		t.Fatalf("query returned 0 spans after flush")
	}
	t.Logf("HTTP query returned count=%.0f", count)

	// --- Phase D: Kill -9, restart, query again ---
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("sigkill: %v", err)
	}
	_ = cmd.Wait()

	cmd2 := startServer(t, bin, dataDir)
	defer func() {
		if cmd2.Process != nil {
			_ = cmd2.Process.Kill()
			_ = cmd2.Wait()
		}
	}()
	time.Sleep(2 * time.Second)

	resp2, err := http.Get("http://localhost:8080/v1/traces?tenant=default")
	if err != nil {
		t.Fatalf("http get after restart: %v", err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("http status after restart %d: %s", resp2.StatusCode, string(body2))
	}
	var result2 map[string]any
	if err := json.Unmarshal(body2, &result2); err != nil {
		t.Fatalf("json unmarshal after restart: %v", err)
	}
	count2, ok := result2["count"].(float64)
	if !ok {
		t.Fatalf("unexpected response after restart: %v", result2)
	}
	if count2 == 0 {
		t.Fatalf("query after restart returned 0 spans — data lost")
	}
	t.Logf("HTTP query after restart returned count=%.0f", count2)
	t.Log("Block 2 integration test PASSED")
}

// TestBlock2_LogFlushAndQuery sends log data, triggers a flush, queries it.
func TestBlock2_LogFlushAndQuery(t *testing.T) {
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

	// Send 100 log exports to trigger flush threshold.
	for i := 0; i < 100; i++ {
		body := fmt.Sprintf("log-line-%d", i)
		sendLogExport(t, dataDir, buildTestResourceLogs("log-service", body, logspb.SeverityNumber_SEVERITY_NUMBER_INFO))
	}

	time.Sleep(3 * time.Second)

	logDir := filepath.Join(dataDir, "logs")
	manifestPath := filepath.Join(logDir, "manifest.log")
	assertFileExists(t, manifestPath, 1)

	matches, err := filepath.Glob(filepath.Join(logDir, "sst-*.pb"))
	if err != nil || len(matches) == 0 {
		t.Fatal("expected at least one log segment file after flush")
	}
	for _, seg := range matches {
		t.Logf("log segment: %s (size %d)", seg, fileSize(t, seg))
	}

	// Query logs via HTTP.
	resp, err := http.Get("http://localhost:8080/v1/logs?tenant=default")
	if err != nil {
		t.Fatalf("http get logs: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("logs http status %d: %s", resp.StatusCode, string(body))
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("logs json unmarshal: %v", err)
	}
	count, ok := result["count"].(float64)
	if !ok {
		t.Fatalf("unexpected logs response: %v", result)
	}
	if count == 0 {
		t.Fatal("logs query returned 0 after flush")
	}
	t.Logf("HTTP logs query returned count=%.0f", count)
}

// TestBlock2_MixedSignalCrashRecovery writes traces and logs, crashes, restarts,
// and asserts both signals are queryable.
func TestBlock2_MixedSignalCrashRecovery(t *testing.T) {
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

	for i := 0; i < 50; i++ {
		spanName := fmt.Sprintf("mixed-span-%d", i)
		sendExport(t, dataDir, buildTestResourceSpans("mixed-svc", spanName))
		body := fmt.Sprintf("mixed-log-%d", i)
		sendLogExport(t, dataDir, buildTestResourceLogs("mixed-svc", body, logspb.SeverityNumber_SEVERITY_NUMBER_WARN))
	}

	time.Sleep(3 * time.Second)

	// Verify both query endpoints return data.
	respTraces, _ := http.Get("http://localhost:8080/v1/traces?tenant=default")
	trBody, _ := io.ReadAll(respTraces.Body)
	respTraces.Body.Close()
	var trResult map[string]any
	json.Unmarshal(trBody, &trResult)
	trCount := trResult["count"].(float64)

	respLogs, _ := http.Get("http://localhost:8080/v1/logs?tenant=default")
	lgBody, _ := io.ReadAll(respLogs.Body)
	respLogs.Body.Close()
	var lgResult map[string]any
	json.Unmarshal(lgBody, &lgResult)
	lgCount := lgResult["count"].(float64)

	if trCount == 0 {
		t.Fatal("mixed signal: traces query returned 0 before crash")
	}
	if lgCount == 0 {
		t.Fatal("mixed signal: logs query returned 0 before crash")
	}
	t.Logf("Pre-crash: traces=%.0f logs=%.0f", trCount, lgCount)

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

	respTraces2, _ := http.Get("http://localhost:8080/v1/traces?tenant=default")
	trBody2, _ := io.ReadAll(respTraces2.Body)
	respTraces2.Body.Close()
	var trResult2 map[string]any
	json.Unmarshal(trBody2, &trResult2)
	trCount2 := trResult2["count"].(float64)

	respLogs2, _ := http.Get("http://localhost:8080/v1/logs?tenant=default")
	lgBody2, _ := io.ReadAll(respLogs2.Body)
	respLogs2.Body.Close()
	var lgResult2 map[string]any
	json.Unmarshal(lgBody2, &lgResult2)
	lgCount2 := lgResult2["count"].(float64)

	if trCount2 == 0 {
		t.Fatal("mixed signal crash recovery: traces query returned 0")
	}
	if lgCount2 == 0 {
		t.Fatal("mixed signal crash recovery: logs query returned 0")
	}
	t.Logf("Post-crash: traces=%.0f logs=%.0f", trCount2, lgCount2)
	t.Log("Mixed signal crash recovery test PASSED")
}
