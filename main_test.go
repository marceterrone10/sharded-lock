package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewShardedMapDefaultsToOneShard(t *testing.T) {
	for _, n := range []int{0, -1, -10} {
		sm := NewShardedMap[string, int](n)
		if sm.num != 1 {
			t.Fatalf("numShards=%d: want 1 shard, got %d", n, sm.num)
		}
		if len(sm.shards) != 1 {
			t.Fatalf("numShards=%d: want 1 bucket, got %d", n, len(sm.shards))
		}
	}
}

func TestNewShardedMapCreatesRequestedShards(t *testing.T) {
	sm := NewShardedMap[string, int](8)
	if sm.num != 8 {
		t.Fatalf("want 8 shards, got %d", sm.num)
	}
	if len(sm.shards) != 8 {
		t.Fatalf("want 8 buckets, got %d", len(sm.shards))
	}
	for i, b := range sm.shards {
		if b == nil {
			t.Fatalf("shard %d is nil", i)
		}
		if b.entries == nil {
			t.Fatalf("shard %d entries map is nil", i)
		}
	}
}

func TestSetGetDelete(t *testing.T) {
	sm := NewShardedMap[string, int](4)

	if _, ok := sm.Get("missing"); ok {
		t.Fatal("expected missing key to be absent")
	}

	sm.Set("a", 1)
	sm.Set("b", 2)

	if v, ok := sm.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a): got (%v, %v), want (1, true)", v, ok)
	}
	if v, ok := sm.Get("b"); !ok || v != 2 {
		t.Fatalf("Get(b): got (%v, %v), want (2, true)", v, ok)
	}

	sm.Set("a", 99)
	if v, ok := sm.Get("a"); !ok || v != 99 {
		t.Fatalf("overwrite Get(a): got (%v, %v), want (99, true)", v, ok)
	}

	sm.Delete("a")
	if _, ok := sm.Get("a"); ok {
		t.Fatal("expected key a to be deleted")
	}
	if v, ok := sm.Get("b"); !ok || v != 2 {
		t.Fatalf("Get(b) after deleting a: got (%v, %v), want (2, true)", v, ok)
	}

	sm.Delete("missing")
}

func TestIntKeys(t *testing.T) {
	sm := NewShardedMap[int, string](4)
	sm.Set(42, "answer")
	sm.Set(7, "lucky")

	if v, ok := sm.Get(42); !ok || v != "answer" {
		t.Fatalf("Get(42): got (%q, %v), want (\"answer\", true)", v, ok)
	}
	if v, ok := sm.Get(7); !ok || v != "lucky" {
		t.Fatalf("Get(7): got (%q, %v), want (\"lucky\", true)", v, ok)
	}

	sm.Delete(42)
	if _, ok := sm.Get(42); ok {
		t.Fatal("expected key 42 to be deleted")
	}
}

func TestSameKeyAlwaysMapsToSameShard(t *testing.T) {
	sm := NewShardedMap[string, int](16)
	key := "stable-key"
	want := sm.switchShardIndex(key)
	for i := 0; i < 100; i++ {
		got := sm.switchShardIndex(key)
		if got != want {
			t.Fatalf("iteration %d: shard index changed from %d to %d", i, want, got)
		}
	}
}

func TestShardIndexInRange(t *testing.T) {
	const n = 8
	sm := NewShardedMap[string, int](n)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%d", i)
		idx := sm.switchShardIndex(key)
		if idx >= sm.num {
			t.Fatalf("key %q mapped to shard %d, out of range [0, %d)", key, idx, sm.num)
		}
	}
}

// --- Race Test ---
// Run: go test -race -run TestRaceConcurrentReadWriteDelete
func TestRaceConcurrentReadWriteDelete(t *testing.T) {
	sm := NewShardedMap[int, interface{}](64)
	const goroutines = 32
	const opsPerG = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerG; i++ {
				key := g*opsPerG + i
				switch i % 3 {
				case 0:
					sm.Set(key, key)
				case 1:
					sm.Get(key)
				case 2:
					sm.Delete(key)
				}
				sm.Get(key % 97)
				sm.Set(key%13, key)
			}
		}()
	}

	wg.Wait()
}

// --- Memory Test ---
// Store 1M int keys with interface{} values; fail if >50MB over baseline map.
func TestMemoryOverhead(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory test in short mode")
	}

	const n = 1_000_000
	const maxExtraBytes = 50 * 1024 * 1024

	baselineBytes := measureMapMemory(t, n)
	shardedBytes := measureShardedMapMemory(t, n)

	extra := int64(shardedBytes) - int64(baselineBytes)
	t.Logf("baseline map: %.2f MB", float64(baselineBytes)/1024/1024)
	t.Logf("sharded map:  %.2f MB", float64(shardedBytes)/1024/1024)
	t.Logf("extra memory: %.2f MB (limit: %.2f MB)", float64(extra)/1024/1024, float64(maxExtraBytes)/1024/1024)

	if extra > maxExtraBytes {
		t.Fatalf("sharded map uses %.2f MB more than baseline map (limit: 50 MB)",
			float64(extra)/1024/1024)
	}
}

func measureMapMemory(t *testing.T, n int) uint64 {
	t.Helper()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	m := make(map[int]interface{}, n)
	for i := 0; i < n; i++ {
		m[i] = i
	}

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(m)

	if after.HeapAlloc < before.HeapAlloc {
		t.Fatalf("unexpected heap shrink during baseline measurement")
	}
	return after.HeapAlloc - before.HeapAlloc
}

func measureShardedMapMemory(t *testing.T, n int) uint64 {
	t.Helper()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	sm := NewShardedMap[int, interface{}](64)
	for i := 0; i < n; i++ {
		sm.Set(i, i)
	}

	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(sm)

	if after.HeapAlloc < before.HeapAlloc {
		t.Fatalf("unexpected heap shrink during sharded map measurement")
	}
	return after.HeapAlloc - before.HeapAlloc
}

// --- Contention Test ---
// Run benchmarks: go test -bench=BenchmarkContention -cpuprofile=cpu.prof
// Inspect with: go tool pprof -top cpu.prof
func TestContentionScaling(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping contention test in short mode")
	}

	const ops = 200_000

	oneShard := runContentionSets(1, ops)
	manyShards := runContentionSets(64, ops)

	t.Logf("1 shard:  %v (%d ops)", oneShard, ops)
	t.Logf("64 shards: %v (%d ops)", manyShards, ops)

	// 64 shards should be meaningfully faster under parallel Set load.
	if manyShards >= oneShard {
		t.Fatalf("expected 64 shards faster than 1 shard, got 64=%v 1=%v", manyShards, oneShard)
	}

	ratio := float64(oneShard) / float64(manyShards)
	if ratio < 1.5 {
		t.Fatalf("expected near-linear scaling (ratio >= 1.5x), got %.2fx", ratio)
	}
}

func runContentionSets(shards, ops int) time.Duration {
	sm := NewShardedMap[int, int](shards)
	var wg sync.WaitGroup
	const goroutines = 8

	keysPerG := ops / goroutines
	start := make(chan struct{})

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			<-start
			base := g * keysPerG
			for i := 0; i < keysPerG; i++ {
				sm.Set(base+i, base+i)
			}
		}()
	}

	t0 := time.Now()
	close(start)
	wg.Wait()
	return time.Since(t0)
}

func BenchmarkContention1Shard(b *testing.B) {
	benchmarkContentionSets(b, 1)
}

func BenchmarkContention64Shards(b *testing.B) {
	benchmarkContentionSets(b, 64)
}

func benchmarkContentionSets(b *testing.B, shards int) {
	sm := NewShardedMap[int, int](shards)
	b.SetParallelism(8)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var seq atomic.Int64
		for pb.Next() {
			key := int(seq.Add(1))
			sm.Set(key, key)
		}
	})
}
