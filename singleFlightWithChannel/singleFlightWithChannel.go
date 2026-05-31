package singleFlightWithChannel

import (
	"context"
	"fmt"
	"sync"
	"time"
)

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

type Deduplicator struct {
	mu    sync.Mutex
	calls map[string]*Call
	ttl   time.Duration
	cache map[string]CacheItem
}

func NewDeduplicator(ttl time.Duration) *Deduplicator {
	return &Deduplicator{
		calls: make(map[string]*Call),
		ttl:   ttl,
		cache: make(map[string]CacheItem),
	}
}

func (d *Deduplicator) Do(ctx context.Context, key string, fn func(ctx context.Context) (string, error)) (string, error) {
	d.mu.Lock()
	//1. Check in cache first
	if cacheItem, exists := d.cache[key]; exists {
		if time.Now().After(cacheItem.expiration) {
			delete(d.cache, key)
		} else {
			d.mu.Unlock()
			return cacheItem.value, nil
		}
	}
	// -----------------------------
	// INFLIGHT REQUEST EXISTS
	// -----------------------------
	if call, exists := d.calls[key]; exists {
		d.mu.Unlock()
		select {
		case <-call.channel:
			return call.result.res, call.result.err
		case <-ctx.Done():
			return call.result.res, ctx.Err()
		}
	}
	// -----------------------------
	// FIRST GOROUTINE
	// -----------------------------
	call := &Call{channel: make(chan struct{})}
	d.calls[key] = call
	d.mu.Unlock()
	// -------------------------------------------------
	// GUARANTEED CLEANUP + RECOVERY
	// -------------------------------------------------
	defer d.finishCall(key, call)
	// -------------------------------------------------
	// EXECUTE EXPENSIVE FUNCTION
	// -------------------------------------------------
	res, err := fn(ctx)
	call.result = Result{res: res, err: err}
	// -------------------------------------------------
	// POPULATE CACHE
	// -------------------------------------------------
	if err == nil {
		d.mu.Lock()
		d.cache[key] = CacheItem{value: res, expiration: time.Now().Add(d.ttl)}
		d.mu.Unlock()
	}
	return res, err
}

func (d *Deduplicator) finishCall(key string, call *Call) {
	// Panic recovery
	if r := recover(); r != nil {
		call.result.err = fmt.Errorf("panic: %v", r)
	}
	// Wake all waiters
	close(call.channel)
	// Cleanup inflight map
	d.mu.Lock()
	delete(d.calls, key)
	d.mu.Unlock()
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
