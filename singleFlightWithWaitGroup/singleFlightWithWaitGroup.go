package singleFlightWithWaitGroup

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const numShards = 64

type Result struct {
	val string
	err error
}

type Call struct {
	wg     sync.WaitGroup
	result Result
}

type CacheItem struct {
	value  string
	expiry time.Time
}

type Shard struct {
	mu      sync.Mutex
	cacheMu sync.RWMutex
	calls   map[string]*Call
	cache   map[string]CacheItem
}

type SingleFlight struct {
	shards []Shard
	ttl    time.Duration
}

func NewSingleFlight(ttl time.Duration) *SingleFlight {
	shards := make([]Shard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = Shard{
			calls: make(map[string]*Call),
			cache: make(map[string]CacheItem),
		}
	}
	return &SingleFlight{
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

func (s *SingleFlight) getShard(key string) *Shard {
	return &s.shards[hash(key)%numShards]
}

func (s *SingleFlight) Do(ctx context.Context, key string, fn func(ctx context.Context) (string, error)) (string, error) {
	shard := s.getShard(key)
	now := time.Now()
	shard.cacheMu.RLock()
	cacheItem, exists := shard.cache[key]
	if exists && now.Before(cacheItem.expiry) {
		shard.cacheMu.RUnlock()
		return cacheItem.value, nil
	}
	shard.cacheMu.RUnlock()
	if exists {
		shard.cacheMu.Lock()
		if cacheItem, exists = shard.cache[key]; exists {
			if now.After(cacheItem.expiry) {
				delete(shard.cache, key)
			}
		}
		shard.cacheMu.Unlock()
	}
	shard.mu.Lock()
	if call, exists := shard.calls[key]; exists {
		shard.mu.Unlock()
		call.wg.Wait() //Does not support context -- downside
		return call.result.val, call.result.err
	}
	call := &Call{wg: sync.WaitGroup{}}
	call.wg.Add(1)
	shard.calls[key] = call
	shard.mu.Unlock()
	defer shard.finishCall(key, call)
	call.result.val, call.result.err = fn(ctx)
	//populate cache
	if call.result.err == nil {
		shard.cacheMu.Lock()
		shard.cache[key] = CacheItem{value: call.result.val, expiry: time.Now().Add(s.ttl)}
		shard.cacheMu.Unlock()
	}
	return call.result.val, call.result.err
}

func (s *Shard) finishCall(key string, call *Call) {
	if r := recover(); r != nil {
		call.result.err = fmt.Errorf("panic: %v", r)
	}
	call.wg.Done()
	s.mu.Lock()
	delete(s.calls, key)
	s.mu.Unlock()
}

func expensiveFunction(ctx context.Context) (string, error) {
	// Simulate an expensive operation
	select {
	case <-time.After(2 * time.Second):
		return "user-profile-data", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func Master() {
	var wg sync.WaitGroup
	newFlight := NewSingleFlight(time.Second * 10)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(routineIndex int) {
			defer wg.Done()
			res, err := newFlight.Do(ctx, "user123", expensiveFunction)
			if err != nil {
				fmt.Printf("error with goroutine %d, err: %v\n", routineIndex, err.Error())
				return
			}
			fmt.Printf("goroutine %d: result: %s\n", routineIndex, res)
		}(i)
	}
	wg.Wait()
	res, err := newFlight.Do(ctx, "user123", expensiveFunction)
	if err != nil {
		fmt.Printf("error with cache hit, err: %v\n", err.Error())
		return
	}
	fmt.Printf("cache hit result: %s\n", res)
}
