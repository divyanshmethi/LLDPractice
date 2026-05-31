package singleFlightWithWaitGroup

import (
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

type SingleFlight struct {
	mu    sync.Mutex
	calls map[string]*Call
}

func NewSingleFlight() *SingleFlight {
	return &SingleFlight{
		calls: make(map[string]*Call),
	}
}

func (s *SingleFlight) Do(key string, fn func() (string, error)) (string, error) {
	s.mu.Lock()
	if call, exists := s.calls[key]; exists {
		s.mu.Unlock()
		call.wg.Wait()
		return call.result.val, call.result.err
	}
	call := &Call{wg: sync.WaitGroup{}}
	call.wg.Add(1)
	s.calls[key] = call
	s.mu.Unlock()
	call.result.val, call.result.err = fn()
	call.wg.Done()
	s.mu.Lock()
	delete(s.calls, key)
	s.mu.Unlock()
	return call.result.val, call.result.err
}

func expensiveFunction() (string, error) {
	// Simulate an expensive operation
	time.Sleep(2 * time.Second)
	return "expensive result", nil
}

func Master() {
	var wg sync.WaitGroup
	newFlight := NewSingleFlight()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(routineIndex int) {
			defer wg.Done()
			res, err := newFlight.Do("user123", expensiveFunction)
			if err != nil {
				fmt.Printf("error with goroutine %d, err: %v\n", routineIndex, err.Error())
				return
			}
			fmt.Printf("goroutine %d: result: %s\n", routineIndex, res)
		}(i)
	}
	wg.Wait()
}
