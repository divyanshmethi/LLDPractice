# LLDPractice

# fanInFanOutWithContext
  1. Implementation of fanInFanOutWithContext in go
  2. Problem statement: Write a program that spins up multiple goroutines to fetch data from different APIs concurrently, unifies the data stream into a single channel, and      processes it sequentially.

# contextAwareWorkerPool
  1. Implementation of workerPool with context in go
  2. Problem statement: Write a functional worker pool where a fixed number of goroutines process incoming tasks from a channel, handle errors gracefully, and aggregate the results.

# singleFlight
  1. Implementation of singleFlight pattern
  2. Problem statement: Write an in-memory thread-safe cache or deduplicator wrapper that ensures if 10 goroutines call an expensive function simultaneously for the same ID, the function is only executed once (using sync.Once or channels)
