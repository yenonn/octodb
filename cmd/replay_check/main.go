// replay_check is a standalone helper to inspect the WAL.
// Run: go run cmd/replay_check/main.go octodb.wal
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/octodb/octodb/internal/wal"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: replay_check <wal_file>")
	}
	path := os.Args[1]
	
	records, err := wal.ReplayToSlice(path)
	if err != nil {
		log.Fatalf("replay failed: %v", err)
	}
	
	fmt.Printf("Replayed %d ResourceSpans from WAL:\n", len(records))
	for i, rs := range records {
		svc := "unknown"
		for _, attr := range rs.Resource.Attributes {
			if attr.Key == "service.name" {
				svc = attr.Value.GetStringValue()
			}
		}
		spanCount := 0
		for _, ss := range rs.ScopeSpans {
			spanCount += len(ss.Spans)
		}
		fmt.Printf("  Record %d: service=%s spans=%d\n", i+1, svc, spanCount)
	}
}
