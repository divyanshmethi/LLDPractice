package singleFlightWithChannel

import (
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

func (d *Deduplicator) Do(key string, fn func() (string, error)) (string, error) {
	d.mu.Lock()
	if call, exists := d.calls[key]; exists {
		d.mu.Unlock()
		<-call.channel
		return call.result.res, call.result.err
	}
	call := &Call{channel: make(chan struct{})}
	d.calls[key] = call
	d.mu.Unlock()
	res, err := fn()
	call.result = Result{res: res, err: err}
	close(call.channel)
	d.mu.Lock()
	delete(d.calls, key)
	d.mu.Unlock()
	return res, err
}

func expensiveOperation() (string, error) {
	time.Sleep(2 * time.Second)
	// Simulate an expensive operation
	return "expensive result", nil
}

func Master() {
	deduplicator := NewDeduplicator()
	var wg sync.WaitGroup
	for i := 1; i < 11; i++ {
		wg.Add(1)
		go func(routineIndex int) {
			defer wg.Done()
			val, err := deduplicator.Do("user123", expensiveOperation)
			if err != nil {
				fmt.Printf("Goroutine %d got error: %v", routineIndex, err.Error())
			}
			fmt.Printf("Goroutine %d got: %s\n", routineIndex, val)
		}(i)
	}
	wg.Wait()
}
