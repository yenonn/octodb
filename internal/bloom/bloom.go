// Package bloom provides a simple Bloom filter for segment skip indexing.
//
// Block 3: Used in Gate 3 of the 5-gate read path to skip segments that
// definitely do not contain a given key or key prefix.
package bloom

import (
	"fmt"
	"hash/fnv"
	"math"
	"os"
)

// Bloom is a simple bloom filter backed by a bit vector.
type Bloom struct {
	bits []uint64 // 64-bit buckets; each bucket holds 64 bits
	m    uint64   // total bits
	k    uint     // number of hash functions
}

// Build creates a Bloom filter for the given keys.
// targetFPP is the desired false positive probability (e.g., 0.001 for 0.1%).
func Build(keys []string, targetFPP float64) *Bloom {
	n := uint64(len(keys))
	if n == 0 {
		return &Bloom{m: 64, k: 1, bits: make([]uint64, 1)}
	}
	// Optimal bits: m = -n * ln(fpp) / (ln(2)^2)
	totalBits := uint64(math.Ceil(-float64(n) * math.Log(targetFPP) / (math.Ln2 * math.Ln2)))
	if totalBits < 64 {
		totalBits = 64
	}
	// Optimal hash functions: k = (m/n) * ln(2)
	numHashes := uint(math.Ceil(float64(totalBits) / float64(n) * math.Ln2))
	if numHashes < 1 {
		numHashes = 1
	}
	// If too many hashes, cap it for Go performance.
	if numHashes > 4 {
		numHashes = 4
	}
	buckets := (totalBits + 63) / 64
	b := &Bloom{
		bits: make([]uint64, buckets),
		m:    totalBits,
		k:    numHashes,
	}
	for _, key := range keys {
		b.Add(key)
	}
	return b
}

// hash computes two 64-bit hashes for the given string.
func (b *Bloom) hash(data string) (uint64, uint64) {
	h1 := fnv.New64a()
	_, _ = h1.Write([]byte(data))
	val1 := h1.Sum64()
	// Secondary hash derived by string prepended with 0x01.
	h2 := fnv.New64a()
	_, _ = h2.Write([]byte{0x01})
	_, _ = h2.Write([]byte(data))
	val2 := h2.Sum64()
	return val1, val2
}

// Add inserts a key into the Bloom filter.
func (b *Bloom) Add(key string) {
	h1, h2 := b.hash(key)
	for i := uint(0); i < b.k; i++ {
		bitsIdx := (h1 + uint64(i)*h2) % b.m
		b.bits[bitsIdx/64] |= 1 << (bitsIdx % 64)
	}
}

// MightContain returns true if the key might be in the set.
// Returns false if the key is definitely NOT in the set.
func (b *Bloom) MightContain(key string) bool {
	h1, h2 := b.hash(key)
	for i := uint(0); i < b.k; i++ {
		bitsIdx := (h1 + uint64(i)*h2) % b.m
		if (b.bits[bitsIdx/64] & (1 << (bitsIdx % 64))) == 0 {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// File I/O
// ---------------------------------------------------------------------------

// Save writes the bloom filter to a file as raw bytes.
func (b *Bloom) Save(path string) error {
	// Format: [8 bytes m] [8 bytes k] [remaining bit vector bytes]
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("bloom save: %w", err)
	}
	defer f.Close()

	if err := binaryWriteUint64(f, b.m); err != nil {
		return err
	}
	if err := binaryWriteUint64(f, uint64(b.k)); err != nil {
		return err
	}
	for _, bucket := range b.bits {
		if err := binaryWriteUint64(f, bucket); err != nil {
			return err
		}
	}
	return f.Sync()
}

// Load reads a bloom filter from a file.
func Load(path string) (*Bloom, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("bloom load: %w", err)
	}
	if len(data) < 16 {
		return nil, fmt.Errorf("bloom load: file too short")
	}
	m := readUint64(data[0:8])
	k := readUint64(data[8:16])
	remaining := data[16:]
	expectedBuckets := int((m + 63) / 64)
	if len(remaining) != expectedBuckets*8 {
		return nil, fmt.Errorf("bloom load: size mismatch")
	}
	bits := make([]uint64, expectedBuckets)
	for i := 0; i < expectedBuckets; i++ {
		bits[i] = readUint64(remaining[i*8 : i*8+8])
	}
	return &Bloom{
		bits: bits,
		m:    m,
		k:    uint(k),
	}, nil
}

func binaryWriteUint64(f *os.File, v uint64) error {
	_, err := f.Write([]byte{
		byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24),
		byte(v >> 32), byte(v >> 40), byte(v >> 48), byte(v >> 56),
	})
	return err
}

func readUint64(b []byte) uint64 {
	return uint64(b[0]) |
		uint64(b[1])<<8 |
		uint64(b[2])<<16 |
		uint64(b[3])<<24 |
		uint64(b[4])<<32 |
		uint64(b[5])<<40 |
		uint64(b[6])<<48 |
		uint64(b[7])<<56
}

// ---------------------------------------------------------------------------
// EstimateFalsePositiveRate returns the current FP probability
// for bloom filter of m bits, k functions, and n inserted keys.
func EstimateFalsePositiveRate(m uint64, k uint, n uint64) float64 {
	if n == 0 {
		return 0
	}
	return math.Pow(1-math.Exp(-float64(k)*float64(n)/float64(m)), float64(k))
}
