package bloom

import (
	"fmt"
	"testing"
)

// BenchmarkBloomBuild measures bloom filter construction time.
func BenchmarkBloomBuild(b *testing.B) {
	keys := make([]string, 10000)
	for i := range keys {
		keys[i] = fmt.Sprintf("tenant\x00svc\x00%d", i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Build(keys, 0.01)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "filters/sec")
}

// BenchmarkBloomQuery measures bloom filter lookup throughput.
func BenchmarkBloomQuery(b *testing.B) {
	keys := make([]string, 10000)
	for i := range keys {
		keys[i] = fmt.Sprintf("tenant\x00svc\x00%d", i)
	}
	bf := Build(keys, 0.01)

	b.ReportAllocs()
	b.ResetTimer()
	found := 0
	for i := 0; i < b.N; i++ {
		if bf.MightContain(keys[i%len(keys)]) {
			found++
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
	_ = found
}
