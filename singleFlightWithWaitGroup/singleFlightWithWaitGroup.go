package singleFlightWithWaitGroup

import (
	"context"
	"fmt"
	"sync"
	"time"
)

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

type SingleFlight struct {
	mu    sync.Mutex
	calls map[string]*Call
	cache map[string]CacheItem
	ttl   time.Duration
}

func NewSingleFlight(ttl time.Duration) *SingleFlight {
	return &SingleFlight{
		calls: make(map[string]*Call),
		cache: make(map[string]CacheItem),
		ttl:   ttl,
	}
}

func (s *SingleFlight) Do(ctx context.Context, key string, fn func(ctx context.Context) (string, error)) (string, error) {
	s.mu.Lock()
	if cacheItem, exists := s.cache[key]; exists {
		if time.Now().After(cacheItem.expiry) {
			delete(s.cache, key)
		} else {
			s.mu.Unlock()
			return cacheItem.value, nil
		}
	}
	if call, exists := s.calls[key]; exists {
		s.mu.Unlock()
		call.wg.Wait() //Does not support context -- downside
		return call.result.val, call.result.err
	}
	call := &Call{wg: sync.WaitGroup{}}
	call.wg.Add(1)
	s.calls[key] = call
	s.mu.Unlock()
	defer s.finishCall(key, call)
	call.result.val, call.result.err = fn(ctx)
	//populate cache
	if call.result.err != nil {
		s.mu.Lock()
		s.cache[key] = CacheItem{value: call.result.val, expiry: time.Now().Add(s.ttl)}
		s.mu.Unlock()
	}
	return call.result.val, call.result.err
}

func (s *SingleFlight) finishCall(key string, call *Call) {
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
