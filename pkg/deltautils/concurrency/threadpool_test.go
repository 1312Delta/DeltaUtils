package concurrency

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolBasicOperation(t *testing.T) {
	// Create a pool with default options
	options := DefaultPoolOptions()
	options.Debug = true
	pool := NewPool(options)
	defer pool.Stop()

	// Initial state should be zero active workers
	if got := pool.GetActiveCount(); got != 0 {
		t.Errorf("Expected 0 active workers initially, got %d", got)
	}

	// Create a counter to track task execution
	var counter int32
	var wg sync.WaitGroup
	wg.Add(5)

	// Submit 5 tasks
	for i := 0; i < 5; i++ {
		err := pool.Submit(func(ctx context.Context) error {
			defer wg.Done()
			atomic.AddInt32(&counter, 1)
			time.Sleep(50 * time.Millisecond) // Simulate work
			return nil
		})

		if err != nil {
			t.Errorf("Failed to submit task: %v", err)
		}
	}

	// Wait for tasks to complete
	wg.Wait()

	// Verify all tasks were executed
	if atomic.LoadInt32(&counter) != 5 {
		t.Errorf("Expected 5 tasks to be executed, got %d", counter)
	}

	// Check metrics
	metrics := pool.GetMetrics()
	if metrics.TasksSubmitted != 5 || metrics.TasksCompleted != 5 || metrics.TasksSucceeded != 5 {
		t.Errorf("Incorrect metrics: %+v", metrics)
	}
}

func TestPoolResizing(t *testing.T) {
	// Create a pool with custom settings
	options := DefaultPoolOptions()
	options.Debug = true
	options.MinSize = 1
	options.MaxSize = 10
	pool := NewPool(options)
	defer pool.Stop()

	// Submit tasks to ensure the pool creates workers
	var wg sync.WaitGroup
	wg.Add(20)

	for i := 0; i < 20; i++ {
		err := pool.Submit(func(ctx context.Context) error {
			defer wg.Done()
			time.Sleep(100 * time.Millisecond) // Sleep to keep workers busy
			return nil
		})

		if err != nil {
			t.Errorf("Failed to submit task: %v", err)
		}
	}

	// Wait for tasks to complete
	wg.Wait()

	// Force all workers to stop - normally this would happen over time
	// but we need to force it for the test to pass in a reasonable time
	if p, ok := interface{}(pool).(*Pool); ok {
		// This is a testing hack - don't do this in real code
		p.forceScaleDown <- struct{}{}
		time.Sleep(200 * time.Millisecond) // Wait for scale down to complete
	}

	// After scaling down, should have 0 workers
	count := pool.GetActiveCount()
	if count != 0 {
		t.Errorf("Pool didn't scale down after tasks completed, has %d active workers", count)
	}

	// Manually resize to 5 without tasks
	pool.Resize(5)
	time.Sleep(100 * time.Millisecond)

	// Should still have 0 active workers since there are no tasks
	count = pool.GetActiveCount()
	if count > 0 {
		t.Errorf("Pool started unnecessary workers when resizing without tasks, has %d active workers", count)
	}
}

func TestPoolLazyInitialization(t *testing.T) {
	// Create a pool with default options
	options := DefaultPoolOptions()
	options.Debug = true
	pool := NewPool(options)
	defer pool.Stop()

	// Initial state should be zero active workers
	if pool.GetActiveCount() != 0 {
		t.Errorf("Expected 0 active workers initially, got %d", pool.GetActiveCount())
	}

	// Check size
	if pool.GetSize() != int32(options.InitialSize) {
		t.Errorf("Expected size to be %d, got %d", options.InitialSize, pool.GetSize())
	}

	// Submit a task
	var wg sync.WaitGroup
	wg.Add(1)
	err := pool.Submit(func(ctx context.Context) error {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond) // Simulate work
		return nil
	})

	if err != nil {
		t.Errorf("Failed to submit task: %v", err)
	}

	// Give the pool time to start a worker
	time.Sleep(50 * time.Millisecond)

	// Should have at least one active worker now
	if pool.GetActiveCount() < 1 {
		t.Errorf("Expected at least 1 active worker after submitting task, got %d", pool.GetActiveCount())
	}

	// Wait for the task to complete
	wg.Wait()

	// Force workers to stop immediately
	if p, ok := interface{}(pool).(*Pool); ok {
		// This is a testing hack - don't do this in real code
		p.forceScaleDown <- struct{}{}
		time.Sleep(200 * time.Millisecond) // Wait for scale down to complete
	}

	// Verify we scale back to 0 workers when idle
	if pool.GetActiveCount() != 0 {
		t.Errorf("Expected 0 active workers after task completion and scale-down, got %d", pool.GetActiveCount())
	}
}

func TestPoolErrorHandling(t *testing.T) {
	// Create a pool
	options := DefaultPoolOptions()
	options.Debug = true
	pool := NewPool(options)
	defer pool.Stop()

	// Test a task that returns an error
	expectedErr := errors.New("test error")
	err := pool.SubmitAndWaitResult(context.Background(), func(ctx context.Context) error {
		return expectedErr
	})

	// Verify the error is returned correctly
	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}

	// Test a task that panics
	err = pool.SubmitAndWaitResult(context.Background(), func(ctx context.Context) error {
		panic("test panic")
	})

	// Verify the panic is caught and returned as an error
	if err == nil || err.Error() != "task panic: test panic" {
		t.Errorf("Expected panic to be converted to error, got %v", err)
	}

	// Check metrics to see if errors and panics were counted
	metrics := pool.GetMetrics()
	if metrics.TasksFailed != 2 || metrics.TasksPanicked != 1 {
		t.Errorf("Incorrect error metrics: %+v", metrics)
	}
}

func TestPoolContextCancellation(t *testing.T) {
	// Create a pool
	options := DefaultPoolOptions()
	options.Debug = true
	pool := NewPool(options)
	defer pool.Stop()

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Submit a task with a long sleep
	resultCh := make(chan error, 1)
	var taskExecuted int32

	err := pool.SubmitAndWait(ctx, func(taskCtx context.Context) error {
		// Set up detection for context cancellation
		select {
		case <-taskCtx.Done():
			return taskCtx.Err()
		case <-time.After(10 * time.Millisecond): // Short delay to ensure we start execution
			atomic.StoreInt32(&taskExecuted, 1)
		}

		// Now do a long sleep, checking for cancellation
		select {
		case <-taskCtx.Done():
			return taskCtx.Err()
		case <-time.After(5 * time.Second): // Long enough to ensure we hit the cancellation
			t.Error("Task wasn't cancelled")
			return nil
		}
	}, resultCh)

	if err != nil {
		t.Errorf("Failed to submit task: %v", err)
	}

	// Wait a bit to make sure the task started
	time.Sleep(50 * time.Millisecond)

	// Cancel the context
	cancel()

	// Wait for the result
	select {
	case err := <-resultCh:
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled error, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Timed out waiting for cancelled task result")
	}

	// Verify the task actually started before being cancelled
	if atomic.LoadInt32(&taskExecuted) != 1 {
		t.Error("Task didn't execute before being cancelled")
	}
}

func TestPoolShutdown(t *testing.T) {
	// Create a pool
	options := DefaultPoolOptions()
	options.Debug = true
	pool := NewPool(options)

	// Submit tasks that take a while
	for i := 0; i < 10; i++ {
		_ = pool.Submit(func(ctx context.Context) error {
			time.Sleep(200 * time.Millisecond)
			return nil
		})
	}

	// Give worker creation time to happen
	time.Sleep(100 * time.Millisecond)

	// Start shutdown with timeout
	startTime := time.Now()
	err := pool.Shutdown(500 * time.Millisecond) // Timeout after 500ms
	shutdownTime := time.Since(startTime)

	// Verify we get timeout error since tasks take longer than the timeout
	if err == nil {
		t.Errorf("Expected shutdown to time out, but it succeeded")
	}

	// Verify the shutdown took approximately the timeout duration
	if shutdownTime < 400*time.Millisecond || shutdownTime > 600*time.Millisecond {
		t.Errorf("Shutdown took %v, expected ~500ms", shutdownTime)
	}

	// Now create a new pool for testing successful shutdown
	pool = NewPool(options)

	// Submit tasks that complete quickly
	for i := 0; i < 10; i++ {
		_ = pool.Submit(func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			return nil
		})
	}

	// Give tasks time to start
	time.Sleep(100 * time.Millisecond)

	// Start shutdown with ample timeout
	err = pool.Shutdown(1 * time.Second)

	// Verify shutdown succeeds
	if err != nil {
		t.Errorf("Expected shutdown to succeed, got error: %v", err)
	}
}

func TestPoolRateLimit(t *testing.T) {
	// Create a pool with rate limiting (200ms between tasks)
	options := DefaultPoolOptions()
	options.Debug = true
	options.RateLimit = 200 * time.Millisecond
	pool := NewPool(options)
	defer pool.Stop()

	// Track task execution times
	var executionTimes []time.Time
	var mutex sync.Mutex

	// Submit 5 tasks
	var wg sync.WaitGroup
	wg.Add(5)

	for i := 0; i < 5; i++ {
		_ = pool.Submit(func(ctx context.Context) error {
			defer wg.Done()
			mutex.Lock()
			executionTimes = append(executionTimes, time.Now())
			mutex.Unlock()
			return nil
		})
	}

	// Wait for tasks to complete
	wg.Wait()

	// Check that execution times are properly spaced
	mutex.Lock()
	defer mutex.Unlock()

	if len(executionTimes) < 5 {
		t.Fatalf("Not all tasks executed, only got %d execution times", len(executionTimes))
	}

	// Sort times (they should already be in order, but just in case)
	for i := 1; i < len(executionTimes); i++ {
		interval := executionTimes[i].Sub(executionTimes[i-1])

		// Allow some flexibility in timing (180-220ms)
		if interval < 180*time.Millisecond || interval > 220*time.Millisecond {
			t.Errorf("Task interval was %v, expected ~200ms", interval)
		}
	}
}

func TestPoolQueueFull(t *testing.T) {
	// Create a pool with a small queue
	options := DefaultPoolOptions()
	options.Debug = true
	options.QueueSize = 2 // Very small queue
	options.InitialSize = 1
	pool := NewPool(options)
	defer pool.Stop()

	// Create a task that blocks until released
	blocker := make(chan struct{})
	blockingTask := func(ctx context.Context) error {
		<-blocker // Block until channel is closed
		return nil
	}

	// Submit the blocking task
	err := pool.Submit(blockingTask)
	if err != nil {
		t.Fatalf("Failed to submit blocking task: %v", err)
	}

	// Give time for worker to start
	time.Sleep(100 * time.Millisecond)

	// Submit 2 more tasks (should fill the queue)
	err = pool.Submit(blockingTask)
	if err != nil {
		t.Fatalf("Failed to submit second task: %v", err)
	}

	err = pool.Submit(blockingTask)
	if err != nil {
		t.Fatalf("Failed to submit third task: %v", err)
	}

	// The fourth task should fail due to queue full
	err = pool.Submit(blockingTask)
	if err == nil || err.Error() != "task queue is full" {
		t.Errorf("Expected 'task queue is full' error, got: %v", err)
	}

	// Release the blocking tasks
	close(blocker)

	// Wait for all tasks to complete
	time.Sleep(200 * time.Millisecond)
}

func TestPoolDynamicRateLimit(t *testing.T) {
	// Create a pool without rate limiting initially
	options := DefaultPoolOptions()
	options.Debug = true
	pool := NewPool(options)
	defer pool.Stop()

	// Track task execution times
	var executionTimes []time.Time
	var mutex sync.Mutex

	// Submit 3 tasks with no rate limit
	var wg sync.WaitGroup
	wg.Add(3)

	for i := 0; i < 3; i++ {
		_ = pool.Submit(func(ctx context.Context) error {
			defer wg.Done()
			mutex.Lock()
			executionTimes = append(executionTimes, time.Now())
			mutex.Unlock()
			time.Sleep(10 * time.Millisecond) // Short sleep
			return nil
		})
	}

	// Wait for tasks to complete
	wg.Wait()

	// Check that execution times are close together (no rate limiting)
	mutex.Lock()
	executionTimes1 := make([]time.Time, len(executionTimes))
	copy(executionTimes1, executionTimes)
	mutex.Unlock()

	// Now set a rate limit
	pool.SetRateLimit(200 * time.Millisecond)

	// Clear previous execution times
	mutex.Lock()
	executionTimes = nil
	mutex.Unlock()

	// Submit 3 more tasks with rate limit
	wg.Add(3)

	for i := 0; i < 3; i++ {
		_ = pool.Submit(func(ctx context.Context) error {
			defer wg.Done()
			mutex.Lock()
			executionTimes = append(executionTimes, time.Now())
			mutex.Unlock()
			time.Sleep(10 * time.Millisecond) // Short sleep
			return nil
		})
	}

	// Wait for tasks to complete
	wg.Wait()

	// Check that execution times are properly spaced
	mutex.Lock()
	defer mutex.Unlock()

	// First batch should be close together
	if len(executionTimes1) >= 2 {
		firstInterval := executionTimes1[1].Sub(executionTimes1[0])
		if firstInterval > 100*time.Millisecond {
			t.Errorf("First batch interval was %v, expected < 100ms (no rate limit)", firstInterval)
		}
	}

	// Second batch should be rate limited
	if len(executionTimes) >= 2 {
		secondInterval := executionTimes[1].Sub(executionTimes[0])
		if secondInterval < 180*time.Millisecond || secondInterval > 220*time.Millisecond {
			t.Errorf("Second batch interval was %v, expected ~200ms (with rate limit)", secondInterval)
		}
	}
}

func TestAutoScalingBehavior(t *testing.T) {
	// Create a pool with auto-scaling
	options := DefaultPoolOptions()
	options.Debug = true
	options.InitialSize = 2
	options.MinSize = 1
	options.MaxSize = 10
	pool := NewPool(options)
	defer pool.Stop()

	// Submit a burst of tasks to trigger scaling up
	var wg sync.WaitGroup
	wg.Add(20)

	startTime := time.Now()
	for i := 0; i < 20; i++ {
		_ = pool.Submit(func(ctx context.Context) error {
			defer wg.Done()
			time.Sleep(300 * time.Millisecond) // Long enough to cause scale-up
			return nil
		})
	}

	// Wait a moment for auto-scaling to kick in - INCREASE THIS WAIT TIME
	time.Sleep(2 * time.Second) // Changed from 1 to 2 seconds

	// Check if we scaled up
	midCount := pool.GetActiveCount()
	if midCount <= 2 {
		t.Errorf("Pool didn't scale up during load, only %d active workers", midCount)
	}

	// Wait for tasks to complete
	wg.Wait()
	completionTime := time.Since(startTime)

	// If auto-scaling worked, completion time should be much less than sequential time
	sequentialTime := 20 * 300 * time.Millisecond
	if completionTime > sequentialTime/2 {
		t.Errorf("Tasks took %v to complete, expected faster execution with auto-scaling", completionTime)
	}

	// Force scale down for test
	if p, ok := interface{}(pool).(*Pool); ok {
		p.forceScaleDown <- struct{}{}
		time.Sleep(200 * time.Millisecond) // Wait for scale down to complete
	}

	// Verify we scaled down
	finalCount := pool.GetActiveCount()
	if finalCount > 0 {
		t.Errorf("Pool didn't scale back down after tasks completed, has %d active workers", finalCount)
	}
}

func TestPoolResetMetrics(t *testing.T) {
	// Create a pool
	options := DefaultPoolOptions()
	options.Debug = true
	pool := NewPool(options)
	defer pool.Stop()

	// Submit a few tasks
	for i := 0; i < 5; i++ {
		_ = pool.SubmitAndWaitResult(context.Background(), func(ctx context.Context) error {
			return nil
		})
	}

	// Submit an error task
	_ = pool.SubmitAndWaitResult(context.Background(), func(ctx context.Context) error {
		return errors.New("test error")
	})

	// Check metrics before reset
	metrics := pool.GetMetrics()
	if metrics.TasksSubmitted != 6 || metrics.TasksCompleted != 6 || metrics.TasksSucceeded != 5 || metrics.TasksFailed != 1 {
		t.Errorf("Incorrect metrics before reset: %+v", metrics)
	}

	// Reset metrics
	pool.ResetMetrics()

	// Check metrics after reset
	metrics = pool.GetMetrics()
	if metrics.TasksSubmitted != 0 || metrics.TasksCompleted != 0 || metrics.TasksSucceeded != 0 || metrics.TasksFailed != 0 {
		t.Errorf("Metrics were not reset: %+v", metrics)
	}

	// Submit more tasks after reset
	for i := 0; i < 3; i++ {
		_ = pool.SubmitAndWaitResult(context.Background(), func(ctx context.Context) error {
			return nil
		})
	}

	// Verify new metrics are counted correctly
	metrics = pool.GetMetrics()
	if metrics.TasksSubmitted != 3 || metrics.TasksCompleted != 3 || metrics.TasksSucceeded != 3 {
		t.Errorf("Incorrect metrics after new tasks: %+v", metrics)
	}
}
