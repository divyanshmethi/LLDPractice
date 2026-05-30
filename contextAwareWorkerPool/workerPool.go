package contextAwareWorkerPool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Task struct {
	ID    int
	Input int
}

type Result struct {
	TaskID int
	Output int
	Err    error
}

func worker(
	ctx context.Context,
	workerID int,
	taskCh <-chan Task,
	resultCh chan<- Result,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for {
		select {

		// Stop worker if context cancelled
		case <-ctx.Done():
			fmt.Printf("[Worker %d] Shutting down\n", workerID)
			return

		// Receive task
		case task, ok := <-taskCh:
			if !ok {
				return
			}

			fmt.Printf("[Worker %d] Processing task %d\n", workerID, task.ID)

			output, err := process(ctx, task)

			result := Result{
				TaskID: task.ID,
				Output: output,
				Err:    err,
			}

			// Avoid blocking forever if context cancelled
			select {
			case <-ctx.Done():
				return
			case resultCh <- result:
			}
		}
	}
}

func process(ctx context.Context, task Task) (int, error) {

	// Simulate expensive work
	select {
	case <-ctx.Done():
		return 0, ctx.Err()

	case <-time.After(2 * time.Second):
	}

	if task.Input < 0 {
		return 0, errors.New("negative input not allowed")
	}

	return task.Input * task.Input, nil
}

func main() {

	// Global timeout for entire worker pool
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	const numWorkers = 3

	tasks := []Task{
		{ID: 1, Input: 2},
		{ID: 2, Input: 4},
		{ID: 3, Input: -1},
		{ID: 4, Input: 8},
		{ID: 5, Input: 10},
	}

	taskCh := make(chan Task)
	resultCh := make(chan Result)

	var workerWG sync.WaitGroup

	// Start workers
	for i := 1; i <= numWorkers; i++ {
		workerWG.Add(1)

		go worker(
			ctx,
			i,
			taskCh,
			resultCh,
			&workerWG,
		)
	}

	// Producer
	go func() {
		defer close(taskCh)

		for _, task := range tasks {

			select {
			case <-ctx.Done():
				fmt.Println("[Producer] Context cancelled")
				return

			case taskCh <- task:
			}
		}
	}()

	// Result channel closer
	go func() {
		workerWG.Wait()
		close(resultCh)
	}()

	// Aggregate results
	for result := range resultCh {

		if result.Err != nil {
			fmt.Printf(
				"[ERROR] Task %d failed: %v\n",
				result.TaskID,
				result.Err,
			)
			continue
		}

		fmt.Printf(
			"[SUCCESS] Task %d => %d\n",
			result.TaskID,
			result.Output,
		)
	}

	fmt.Println("Worker pool shutdown complete")
}
