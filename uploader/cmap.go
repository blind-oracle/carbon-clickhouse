package uploader

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash"
)

const shardCount = 128

// A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (shardCount) map shards.
type CMap []*CMapShard

// A "thread" safe string to anything map.
type CMapShard struct {
	sync.RWMutex // Read Write mutex, guards access to internal map.
	items        map[string]uint64
}

// Creates a new concurrent map.
func NewCMap() CMap {
	m := make(CMap, shardCount)
	for i := 0; i < shardCount; i++ {
		m[i] = &CMapShard{items: make(map[string]uint64)}
	}
	return m
}

// Returns the number of elements within the map.
func (m CMap) Count() int {
	count := 0
	for i := 0; i < shardCount; i++ {
		shard := m[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

func (m CMap) Clear() int {
	count := 0
	for i := 0; i < shardCount; i++ {
		shard := m[i]
		shard.Lock()
		shard.items = make(map[string]uint64)
		shard.Unlock()
	}
	return count
}

// Returns shard under given key
func (m CMap) GetShard(key string) *CMapShard {
	return m[xxhash.Sum64String(key)%shardCount]
}

// Retrieves an element from map under given key.
func (m CMap) Exists(key string, id uint8) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// We don't care if it exists.
	// If not then v will be 0 -> flag unset
	v, _ := shard.items[key]
	shard.RUnlock()
	return v&(1<<(32+id)) != 0
}

// Retrieves an element from map under given key.
func (m CMap) Get(key string) (ok bool, value uint64) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	value, ok = shard.items[key]
	shard.RUnlock()
	return
}

// Sets the given value under the specified key.
func (m CMap) Set(key string, id uint8, ts uint32) {
	shard := m.GetShard(key)
	shard.Lock()
	v, _ := shard.items[key]
	v = v >> 32 << 32        // Clear timestamp
	v = v | (1 << (32 + id)) // Set ID bit
	v = v | uint64(ts)       // Add timestamp
	shard.items[key] = v
	shard.Unlock()
}

func (m CMap) Merge(id uint8, keys map[string]bool, ts uint32) {
	for key := range keys {
		m.Set(key, id, ts)
	}
}

func (m CMap) Expire(ctx context.Context, ttl uint32) (count int) {
	deadline := timeRel() - ttl

	for i := 0; i < shardCount; i++ {
		select {
		case <-ctx.Done():
			return count
		default:
			// pass
		}

		shard := m[i]
		shard.Lock()
		for k, v := range shard.items {
			ts := uint32(v << 32 >> 32)
			if ts < deadline {
				delete(shard.items, k)
				count++
			}
		}
		shard.Unlock()
	}

	return
}

func (m CMap) ExpireWorker(ctx context.Context, ttl time.Duration, expiredCounter *uint32) {
	for {
		interval := time.Minute
		// @TODO: adaptive interval, based on min value from prev Expire run

		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			cnt := m.Expire(ctx, uint32(ttl.Seconds()))
			if expiredCounter != nil {
				atomic.AddUint32(expiredCounter, uint32(cnt))
			}
		}
	}
}
