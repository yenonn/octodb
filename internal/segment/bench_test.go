package segment

import (
	"os"
	"testing"
)

// BenchmarkSegmentWrite measures raw segment write throughput.
func BenchmarkSegmentWrite(b *testing.B) {
	path := "bench_seg_write.pb"
	defer os.Remove(path)

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = os.Remove(path)
		w, err := Create(path)
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < 100; j++ {
			if err := w.Append(payload); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Close("last-key"); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds(), "records/sec")
}

// BenchmarkSegmentRead measures raw segment read throughput.
func BenchmarkSegmentRead(b *testing.B) {
	path := "bench_seg_read.pb"
	defer os.Remove(path)

	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	w, err := Create(path)
	if err != nil {
		b.Fatal(err)
	}
	for j := 0; j < 10000; j++ {
		_ = w.Append(payload)
	}
	_ = w.Close("last-key")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := Open(path)
		if err != nil {
			b.Fatal(err)
		}
		for {
			_, err := r.Next()
			if err != nil {
				break
			}
		}
		_ = r.Close()
	}
	b.ReportMetric(float64(b.N*10000)/b.Elapsed().Seconds(), "records/sec")
}

// BenchmarkSegmentSeekTo measures index seek performance.
func BenchmarkSegmentSeekTo(b *testing.B) {
	path := "bench_seg_seek.pb"
	defer os.Remove(path)

	w, err := Create(path)
	if err != nil {
		b.Fatal(err)
	}
	for j := 0; j < 10000; j++ {
		key := []byte{byte(j >> 24), byte(j >> 16), byte(j >> 8), byte(j)}
		_ = w.Append(key)
	}
	_ = w.Close("last-key")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := Open(path)
		if err != nil {
			b.Fatal(err)
		}
		_ = r.SeekTo("\x00\x00\x27\x10") // seek to mid-point
		_ = r.Close()
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "seeks/sec")
}

// BenchmarkSegmentSnappy measures compression vs no-compression record sizes.
func BenchmarkSegmentSnappy(b *testing.B) {
	compressible := make([]byte, 1024) // repeated pattern = highly compressible
	random := make([]byte, 1024)       // will be less compressible
	for i := range compressible {
		compressible[i] = byte(i % 4)
		random[i] = byte(i)
	}

	b.ReportAllocs()
	b.Run("compressible", func(b *testing.B) {
		path := "bench_seg_comp.pb"
		defer os.Remove(path)
		w, _ := Create(path)
		for i := 0; i < b.N; i++ {
			_ = w.Append(compressible)
		}
		_ = w.Close("last")
		info, _ := os.Stat(path)
		b.ReportMetric(float64(info.Size())/float64(b.N*len(compressible)), "ratio")
	})

	b.Run("random", func(b *testing.B) {
		path := "bench_seg_rand.pb"
		defer os.Remove(path)
		w, _ := Create(path)
		for i := 0; i < b.N; i++ {
			_ = w.Append(random)
		}
		_ = w.Close("last")
		info, _ := os.Stat(path)
		b.ReportMetric(float64(info.Size())/float64(b.N*len(random)), "ratio")
	})
}
