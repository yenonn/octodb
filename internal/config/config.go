package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds OctoDB runtime configuration.
type Config struct {
	Server struct {
		GRPCAddr string `yaml:"grpc_addr"`
	} `yaml:"server"`
	WAL struct {
		Path string `yaml:"path"`
	} `yaml:"wal"`
}

// Load reads a YAML config file, falls back to defaults, and applies env overrides.
func Load(path string) (*Config, error) {
	var cfg Config

	// Defaults
	cfg.Server.GRPCAddr = ":4317"
	cfg.WAL.Path = "octodb.wal"

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			// No config file is fine — env vars can still override.
			return applyEnv(&cfg), nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return applyEnv(&cfg), nil
}

func applyEnv(cfg *Config) *Config {
	if v := os.Getenv("OCTODB_GRPC_ADDR"); v != "" {
		cfg.Server.GRPCAddr = v
	}
	if v := os.Getenv("OCTODB_WAL_PATH"); v != "" {
		cfg.WAL.Path = v
	}
	return cfg
}
