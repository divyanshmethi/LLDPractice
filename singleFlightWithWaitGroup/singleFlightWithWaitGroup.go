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

type SingleFlight struct {
	mu    sync.Mutex
	calls map[string]*Call
}

func NewSingleFlight() *SingleFlight {
	return &SingleFlight{
		calls: make(map[string]*Call),
	}
}

func (s *SingleFlight) Do(ctx context.Context, key string, fn func(ctx context.Context) (string, error)) (string, error) {
	s.mu.Lock()
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
	newFlight := NewSingleFlight()
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
}
