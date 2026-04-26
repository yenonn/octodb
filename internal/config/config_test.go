package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("/nonexistent/path.yaml")
	if err != nil {
		t.Fatalf("expected no error for missing file: %v", err)
	}
	if cfg.Server.GRPCAddr != ":4317" {
		t.Fatalf("expected default grpc_addr :4317, got %q", cfg.Server.GRPCAddr)
	}
	if cfg.WAL.Path != "octodb.wal" {
		t.Fatalf("expected default wal.path octodb.wal, got %q", cfg.WAL.Path)
	}
}

func TestLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.yaml")
	content := "server:\n  grpc_addr: :9999\nwal:\n  path: /tmp/test.wal\n"
	_ = os.WriteFile(path, []byte(content), 0644)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Server.GRPCAddr != ":9999" {
		t.Fatalf("expected :9999, got %q", cfg.Server.GRPCAddr)
	}
	if cfg.WAL.Path != "/tmp/test.wal" {
		t.Fatalf("expected /tmp/test.wal, got %q", cfg.WAL.Path)
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	os.Setenv("OCTODB_GRPC_ADDR", ":7777")
	os.Setenv("OCTODB_WAL_PATH", "/env/path.wal")
	defer os.Unsetenv("OCTODB_GRPC_ADDR")
	defer os.Unsetenv("OCTODB_WAL_PATH")

	cfg, err := Load("/nonexistent/path.yaml")
	if err != nil {
		t.Fatalf("expected no error: %v", err)
	}
	if cfg.Server.GRPCAddr != ":7777" {
		t.Fatalf("expected env override :7777, got %q", cfg.Server.GRPCAddr)
	}
	if cfg.WAL.Path != "/env/path.wal" {
		t.Fatalf("expected env override /env/path.wal, got %q", cfg.WAL.Path)
	}
}

func TestLoadFileOverridesDefaultsNotEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "partial.yaml")
	content := "wal:\n  path: /from/file.wal\n"
	_ = os.WriteFile(path, []byte(content), 0644)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Server.GRPCAddr != ":4317" {
		t.Fatal("expected default grpc_addr")
	}
	if cfg.WAL.Path != "/from/file.wal" {
		t.Fatalf("expected wal from file: %q", cfg.WAL.Path)
	}
}

func TestLoadPartialEnv(t *testing.T) {
	os.Setenv("OCTODB_GRPC_ADDR", ":1111")
	defer os.Unsetenv("OCTODB_GRPC_ADDR")

	cfg, err := Load("/nonexistent/path.yaml")
	if err != nil {
		t.Fatalf("expected no error: %v", err)
	}
	if cfg.Server.GRPCAddr != ":1111" {
		t.Fatalf("expected env partial override :1111, got %q", cfg.Server.GRPCAddr)
	}
}
