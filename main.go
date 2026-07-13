package main

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sync"
)

type ShardedMap[K comparable, V any] struct {
	shards []*Bucket[K, V]
	num    uint64
}

type Bucket[K comparable, V any] struct {
	mu      sync.RWMutex // lock para cada shard
	entries map[K]V      //data
}

func NewShardedMap[K comparable, V any](numShards int) *ShardedMap[K, V] {
	if numShards <= 0 {
		numShards = 1
	}

	sm := &ShardedMap[K, V]{
		shards: make([]*Bucket[K, V], numShards),
		num:    uint64(numShards),
	}

	for i := range sm.shards {
		sm.shards[i] = &Bucket[K, V]{
			entries: make(map[K]V),
		}
	}

	return sm
}

func (sm *ShardedMap[K, V]) switchShardIndex(key K) uint64 {
	h := fnv.New64a()

	switch k := any(key).(type) {
	case string:
		h.Write([]byte(k))
	case int:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(k))
		h.Write(buf[:])
	case int64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(k))
		h.Write(buf[:])
	case uint64:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], k)
		h.Write(buf[:])
	case uint:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(k))
		h.Write(buf[:])
	default:
		s := fmt.Sprintf("%v", key)
		h.Write([]byte(s))
	}

	index := h.Sum64() % sm.num

	return index
}

// methods

func (sm *ShardedMap[K, V]) Get(key K) (V, bool) {
	idx := sm.switchShardIndex(key)
	shard := sm.shards[idx]
	shard.mu.RLock() // shared read lock
	val, ok := shard.entries[key]
	shard.mu.RUnlock()
	return val, ok
}

func (sm *ShardedMap[K, V]) Set(key K, value V) {
	idx := sm.switchShardIndex(key)
	shard := sm.shards[idx]
	shard.mu.Lock() // exclusive write lock
	shard.entries[key] = value
	shard.mu.Unlock()
}

func (sm *ShardedMap[K, V]) Delete(key K) {
	idx := sm.switchShardIndex(key)
	shard := sm.shards[idx]
	shard.mu.Lock() // exclusive write lock
	delete(shard.entries, key)
	shard.mu.Unlock()
}
