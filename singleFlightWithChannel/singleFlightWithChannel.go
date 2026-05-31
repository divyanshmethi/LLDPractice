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
type Deduplicator struct {
	mu    sync.Mutex
	calls map[string]*Call
}

func NewDeduplicator() *Deduplicator {
	return &Deduplicator{
		calls: make(map[string]*Call),
	}
}

func (d *Deduplicator) Do(ctx context.Context, key string, fn func(ctx context.Context) (string, error)) (string, error) {
	d.mu.Lock()
	if call, exists := d.calls[key]; exists {
		d.mu.Unlock()
		select {
		case <-call.channel:
			return call.result.res, call.result.err
		case <-ctx.Done():
			return call.result.res, ctx.Err()
		}
	}
	call := &Call{channel: make(chan struct{})}
	d.calls[key] = call
	d.mu.Unlock()
	defer d.finishCall(key, call)
	res, err := fn(ctx)
	call.result = Result{res: res, err: err}
	return res, err
}

func (d *Deduplicator) finishCall(key string, call *Call) {
	if r := recover(); r != nil {
		call.result.err = fmt.Errorf("panic: %v", r)
	}
	close(call.channel)
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
	deduplicator := NewDeduplicator()
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
}
