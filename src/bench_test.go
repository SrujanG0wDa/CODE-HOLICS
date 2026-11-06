package main

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

// Benchmark individual synchronous Put calls (each fsyncs to disk)
func BenchmarkPutSync(b *testing.B) {
	dbPath := filepath.Join(b.TempDir(), "bench_put_sync.db")
	eng, err := Open(dbPath)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	val := []byte("benchmark-test-value-128-bytes-long-padding-to-simulate-typical-workload-payloads-for-storage-engine-performance-evaluation")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k%d", i)
		if err := eng.Put([]byte(key), val); err != nil {
			b.Fatalf("Put failed: %v", err)
		}
	}
}

// Benchmark Batched Put calls (single fsync per batch of 100 ops)
func BenchmarkBatchPut(b *testing.B) {
	dbPath := filepath.Join(b.TempDir(), "bench_batch.db")
	eng, err := Open(dbPath)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	val := []byte("benchmark-test-value-128-bytes-long-padding-to-simulate-typical-workload-payloads-for-storage-engine-performance-evaluation")
	const batchSize = 100

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ops := make([]BatchOp, batchSize)
		for j := 0; j < batchSize; j++ {
			ops[j] = BatchOp{
				Key:   []byte(fmt.Sprintf("k_%d_%d", i, j)),
				Value: val,
			}
		}
		if err := eng.Batch(ops); err != nil {
			b.Fatalf("Batch failed: %v", err)
		}
	}
}

// Benchmark in-memory index Get with disk ReadAt and CRC verification
func BenchmarkGet(b *testing.B) {
	dbPath := filepath.Join(b.TempDir(), "bench_get.db")
	eng, err := Open(dbPath)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	val := []byte("benchmark-test-value-128-bytes-long-padding-to-simulate-typical-workload-payloads-for-storage-engine-performance-evaluation")
	const numKeys = 1000

	// Pre-populate database
	ops := make([]BatchOp, numKeys)
	for i := 0; i < numKeys; i++ {
		ops[i] = BatchOp{
			Key:   []byte(fmt.Sprintf("key_%d", i)),
			Value: val,
		}
	}
	if err := eng.Batch(ops); err != nil {
		b.Fatalf("Pre-populating Batch failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := fmt.Sprintf("key_%d", i%numKeys)
		got, err := eng.Get([]byte(k))
		if err != nil || len(got) == 0 {
			b.Fatalf("Get failed for %s: %v", k, err)
		}
	}
}

// Benchmark Concurrent Get under parallel reader goroutines
func BenchmarkGetParallel(b *testing.B) {
	dbPath := filepath.Join(b.TempDir(), "bench_get_parallel.db")
	eng, err := Open(dbPath)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer eng.Close()

	val := []byte("benchmark-test-value-128-bytes-long-padding-to-simulate-typical-workload-payloads-for-storage-engine-performance-evaluation")
	const numKeys = 1000

	ops := make([]BatchOp, numKeys)
	for i := 0; i < numKeys; i++ {
		ops[i] = BatchOp{
			Key:   []byte(fmt.Sprintf("key_%d", i)),
			Value: val,
		}
	}
	if err := eng.Batch(ops); err != nil {
		b.Fatalf("Batch failed: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.Intn(numKeys)
		for pb.Next() {
			k := fmt.Sprintf("key_%d", r%numKeys)
			r++
			if _, err := eng.Get([]byte(k)); err != nil {
				b.Fatalf("Parallel Get failed: %v", err)
			}
		}
	})
}
