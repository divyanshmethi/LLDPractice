package fanInFanOutWithContext

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Result struct {
	Error   error
	Resp    string
	Service string
}

func callAPI(ctx context.Context, out chan<- Result, service string) {
	delay := time.Duration(rand.Intn(4)+1) * time.Second
	select {
	case <-ctx.Done():
		out <- Result{Error: ctx.Err(), Service: service}
	case <-time.After(delay):
		out <- Result{Error: nil, Resp: fmt.Sprintf("response from %s", service), Service: service}
	}
}

func fetchData(ctx context.Context, services []string, out chan<- Result) {
	wg := sync.WaitGroup{}
	for _, service := range services {
		wg.Add(1)
		go func(service string) {
			defer wg.Done()
			callAPI(ctx, out, service)
		}(service)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
}

func Master() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	services := []string{
		"UserProfile",
		"OrderService",
		"PaymentService",
		"CartService",
	}
	out := make(chan Result)
	fetchData(ctx, services, out)
	for result := range out {
		if result.Error != nil {
			fmt.Printf("Got error for service : %s, err:%s\n", result.Service, result.Error)
			continue
		}
		fmt.Printf("Response for service: %s, resp : %s\n", result.Service, result.Resp)
		time.Sleep(500 * time.Millisecond) //Simulate sequential processing
	}
	fmt.Println("All services have been fetched")
}
