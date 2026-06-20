package runner

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasic_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func() (func(), *sync.WaitGroup, *atomic.Int32)
		expectedCalls  int32
		timeout        time.Duration
		checkAssertion func(*testing.T, *atomic.Int32)
	}{
		{
			name: "single function execution",
			setup: func() (func(), *sync.WaitGroup, *atomic.Int32) {
				var wg sync.WaitGroup
				var counter atomic.Int32
				wg.Add(1)

				f := func() {
					defer wg.Done()
					counter.Add(1)
				}

				return f, &wg, &counter
			},
			expectedCalls: 1,
			timeout:       100 * time.Millisecond,
			checkAssertion: func(t *testing.T, counter *atomic.Int32) {
				assert.Equal(t, int32(1), counter.Load())
			},
		},
		{
			name: "multiple sequential runs",
			setup: func() (func(), *sync.WaitGroup, *atomic.Int32) {
				var wg sync.WaitGroup
				var counter atomic.Int32
				wg.Add(5)

				f := func() {
					defer wg.Done()
					counter.Add(1)
				}

				return f, &wg, &counter
			},
			expectedCalls: 5,
			timeout:       100 * time.Millisecond,
			checkAssertion: func(t *testing.T, counter *atomic.Int32) {
				assert.Equal(t, int32(5), counter.Load())
			},
		},
		{
			name: "function with delay",
			setup: func() (func(), *sync.WaitGroup, *atomic.Int32) {
				var wg sync.WaitGroup
				var counter atomic.Int32
				wg.Add(1)

				f := func() {
					defer wg.Done()
					time.Sleep(10 * time.Millisecond)
					counter.Add(1)
				}

				return f, &wg, &counter
			},
			expectedCalls: 1,
			timeout:       100 * time.Millisecond,
			checkAssertion: func(t *testing.T, counter *atomic.Int32) {
				assert.Equal(t, int32(1), counter.Load())
			},
		},
		{
			name: "function modifying shared state",
			setup: func() (func(), *sync.WaitGroup, *atomic.Int32) {
				var wg sync.WaitGroup
				var counter atomic.Int32
				wg.Add(1)

				f := func() {
					defer wg.Done()
					// Simulate work - single goroutine incrementing multiple times
					for i := 0; i < 10; i++ {
						counter.Add(1)
					}
				}

				return f, &wg, &counter
			},
			expectedCalls: 1,
			timeout:       100 * time.Millisecond,
			checkAssertion: func(t *testing.T, counter *atomic.Int32) {
				assert.Equal(t, int32(10), counter.Load())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runner := Basic{}
			f, wg, counter := tc.setup()

			// Run the function expected number of times
			for i := int32(0); i < tc.expectedCalls; i++ {
				runner.Run(f)
			}

			// Wait for completion with timeout
			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				tc.checkAssertion(t, counter)
			case <-time.After(tc.timeout):
				t.Fatal("timeout waiting for goroutines to complete")
			}
		})
	}
}

func TestBasic_ConcurrentExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		numGoroutines int
		workDuration  time.Duration
		maxDuration   time.Duration
	}{
		{
			name:          "10 concurrent tasks",
			numGoroutines: 10,
			workDuration:  50 * time.Millisecond,
			maxDuration:   150 * time.Millisecond,
		},
		{
			name:          "100 concurrent tasks",
			numGoroutines: 100,
			workDuration:  10 * time.Millisecond,
			maxDuration:   100 * time.Millisecond,
		},
		{
			name:          "1000 concurrent tasks",
			numGoroutines: 1000,
			workDuration:  1 * time.Millisecond,
			maxDuration:   100 * time.Millisecond,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runner := Basic{}
			var wg sync.WaitGroup
			var counter atomic.Int32

			wg.Add(tc.numGoroutines)
			start := time.Now()

			for i := 0; i < tc.numGoroutines; i++ {
				runner.Run(func() {
					defer wg.Done()
					time.Sleep(tc.workDuration)
					counter.Add(1)
				})
			}

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()

			select {
			case <-done:
				duration := time.Since(start)
				assert.Equal(t, int32(tc.numGoroutines), counter.Load())
				assert.Less(t, duration, tc.maxDuration,
					"concurrent execution should complete faster than sequential")
			case <-time.After(tc.maxDuration + time.Second):
				t.Fatal("timeout waiting for concurrent goroutines")
			}
		})
	}
}

func TestBasic_NonBlockingExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		numTasks     int
		taskDuration time.Duration
		maxWait      time.Duration
	}{
		{
			name:         "single long-running task",
			numTasks:     1,
			taskDuration: 50 * time.Millisecond,
			maxWait:      10 * time.Millisecond,
		},
		{
			name:         "multiple long-running tasks",
			numTasks:     5,
			taskDuration: 100 * time.Millisecond,
			maxWait:      10 * time.Millisecond,
		},
		{
			name:         "many quick tasks",
			numTasks:     100,
			taskDuration: 1 * time.Millisecond,
			maxWait:      10 * time.Millisecond,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runner := Basic{}
			start := time.Now()

			for i := 0; i < tc.numTasks; i++ {
				runner.Run(func() {
					time.Sleep(tc.taskDuration)
				})
			}

			elapsed := time.Since(start)
			assert.Less(t, elapsed, tc.maxWait,
				"Run() should not block - all tasks should be dispatched quickly")
		})
	}
}

func TestBasic_SharedStateAccess(t *testing.T) {
	t.Parallel()

	runner := Basic{}
	const numGoroutines = 100
	const iterations = 1000

	var mu sync.Mutex
	sharedMap := make(map[int]int)
	var wg sync.WaitGroup

	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		goroutineID := i
		runner.Run(func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				mu.Lock()
				sharedMap[goroutineID]++
				mu.Unlock()
			}
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Verify all goroutines completed their work
		mu.Lock()
		defer mu.Unlock()
		require.Len(t, sharedMap, numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			assert.Equal(t, iterations, sharedMap[i],
				"goroutine %d should have completed %d iterations", i, iterations)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for shared state access test")
	}
}

func TestBasic_ImmediateExecution(t *testing.T) {
	t.Parallel()

	runner := Basic{}
	started := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	runner.Run(func() {
		defer wg.Done()
		close(started)
	})

	// Verify goroutine starts quickly
	select {
	case <-started:
		// Success - goroutine started immediately
	case <-time.After(100 * time.Millisecond):
		t.Fatal("goroutine did not start within expected time")
	}

	wg.Wait()
}

func TestBasic_ZeroValue(t *testing.T) {
	t.Parallel()

	var runner Basic
	var wg sync.WaitGroup
	executed := false

	wg.Add(1)
	runner.Run(func() {
		defer wg.Done()
		executed = true
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		assert.True(t, executed, "zero value Basic should still execute functions")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout with zero value runner")
	}
}
