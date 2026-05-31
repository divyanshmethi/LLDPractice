package singleFlightWithChannel

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const numShards = 64

type Result struct {
	res string
	err error
}

type Call struct {
	channel chan struct{}
	result  Result
}

type CacheItem struct {
	value      string
	expiration time.Time
}

type Shard struct {
	mu      sync.Mutex
	calls   map[string]*Call
	cache   map[string]CacheItem
	cachemu sync.RWMutex
}

type Deduplicator struct {
	shards []Shard
	ttl    time.Duration
}

func NewShard() Shard {
	return Shard{
		calls: make(map[string]*Call),
		cache: make(map[string]CacheItem),
	}
}

func NewDeduplicator(ttl time.Duration) *Deduplicator {
	shards := make([]Shard, numShards)

	for i := range shards {
		shards[i] = NewShard()
	}

	return &Deduplicator{
		shards: shards,
		ttl:    ttl,
	}
}

func hash(key string) uint32 {
	var h uint32 = 2166136261

	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}

	return h
}

func (d *Deduplicator) getShard(key string) *Shard {
	idx := hash(key) % numShards
	return &d.shards[idx]
}

func (d *Deduplicator) Do(ctx context.Context, key string, fn func(ctx context.Context) (string, error)) (string, error) {
	s := d.getShard(key)
	now := time.Now()
	s.cachemu.RLock()
	//1. Check in cache first
	cacheItem, exists := s.cache[key]
	if exists && now.Before(cacheItem.expiration) {
		s.cachemu.RUnlock()
		return cacheItem.value, nil
	}
	s.cachemu.RUnlock()
	if exists {
		s.cachemu.Lock()
		cacheItem, exists = s.cache[key]
		if exists && now.After(cacheItem.expiration) {
			delete(s.cache, key)
		}
		s.cachemu.Unlock()
	}
	// -----------------------------
	// INFLIGHT REQUEST EXISTS
	// -----------------------------
	s.mu.Lock()
	if call, exists := s.calls[key]; exists {
		s.mu.Unlock()
		select {
		case <-call.channel:
			return call.result.res, call.result.err
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	// -----------------------------
	// FIRST GOROUTINE
	// -----------------------------
	call := &Call{channel: make(chan struct{})}
	s.calls[key] = call
	s.mu.Unlock()
	// -------------------------------------------------
	// GUARANTEED CLEANUP + RECOVERY
	// -------------------------------------------------
	defer s.finishCall(key, call)
	// -------------------------------------------------
	// EXECUTE EXPENSIVE FUNCTION
	// -------------------------------------------------
	res, err := fn(ctx)
	call.result = Result{res: res, err: err}
	// -------------------------------------------------
	// POPULATE CACHE
	// -------------------------------------------------
	if err == nil {
		s.cachemu.Lock()
		s.cache[key] = CacheItem{value: res, expiration: time.Now().Add(d.ttl)}
		s.cachemu.Unlock()
	}
	return res, err
}

func (s *Shard) finishCall(key string, call *Call) {
	// Panic recovery
	if r := recover(); r != nil {
		call.result.err = fmt.Errorf("panic: %v", r)
	}
	// Wake all waiters
	close(call.channel)
	// Cleanup inflight map
	s.mu.Lock()
	delete(s.calls, key)
	s.mu.Unlock()
}

func expensiveOperation(ctx context.Context) (string, error) {
	fmt.Println("Executing expensive operation...")
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(time.Second * 2):
		return "user-profile-data", nil
	}
}

func Master() {
	deduplicator := NewDeduplicator(10 * time.Second)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	var wg sync.WaitGroup
	for i := 1; i < 11; i++ {
		wg.Add(1)
		go func(routineIndex int) {
			defer wg.Done()
			val, err := deduplicator.Do(ctx, "user123", expensiveOperation)
			if err != nil {
				fmt.Printf("Goroutine %d got error: %v", routineIndex, err.Error())
			}
			fmt.Printf("Goroutine %d got: %s\n", routineIndex, val)
		}(i)
	}
	wg.Wait()
	// -------------------------------------------------
	// CACHE HIT DEMO
	// -------------------------------------------------

	fmt.Println("\nCalling again (should hit cache)...")

	value, err := deduplicator.Do(
		context.Background(),
		"user123",
		expensiveOperation,
	)

	fmt.Println(value, err)
}
