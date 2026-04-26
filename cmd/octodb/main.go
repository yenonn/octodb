package main

import (
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"github.com/octodb/octodb/internal/config"
	"github.com/octodb/octodb/internal/server"
	"github.com/octodb/octodb/internal/store"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "octodb.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Block 2: Use Block2Store (WAL + memtable + flush)
	st, err := store.NewBlock2Store(cfg.WAL.Path)
	if err != nil {
		log.Fatalf("failed to open Block 2 store: %v", err)
	}

	// gRPC server
	lis, err := net.Listen("tcp", cfg.Server.GRPCAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	ts := server.NewTraceServer(st)
	server.Register(grpcServer, ts)

	go func() {
		log.Printf("OctoDB Block 2 starting — gRPC on %s | WAL at %s", cfg.Server.GRPCAddr, cfg.WAL.Path)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve grpc: %v", err)
		}
	}()

	// HTTP query server (Block 2)
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/v1/traces", func(w http.ResponseWriter, r *http.Request) {
			tenantID := r.URL.Query().Get("tenant")
			if tenantID == "" {
				tenantID = "default"
			}
			req := store.ReadRequest{
				TenantID:  tenantID,
				Service:   r.URL.Query().Get("service"),
				StartTime: 0,
				EndTime: 1<<63 - 1,
			}
			spans, err := st.ReadTraces(r.Context(), req)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"count": len(spans),
				"tenant": tenantID,
			})
		})
		addr := cfg.Server.GRPCAddr
		if addr[0] == ':' {
			addr = "localhost" + addr
		}
		httpPort := ":8080"
		log.Printf("OctoDB Block 2 query API on %s", httpPort)
		log.Fatal(http.ListenAndServe(httpPort, mux))
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down OctoDB...")
	grpcServer.GracefulStop()
	st.Close()
	log.Println("shutdown complete")
}
