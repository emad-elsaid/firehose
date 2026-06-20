package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewMemory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		defaultTTL time.Duration
		cleanup    time.Duration
	}{
		{
			name:       "standard TTL and cleanup",
			defaultTTL: 5 * time.Minute,
			cleanup:    10 * time.Minute,
		},
		{
			name:       "short TTL and cleanup",
			defaultTTL: 1 * time.Second,
			cleanup:    2 * time.Second,
		},
		{
			name:       "no expiration",
			defaultTTL: -1,
			cleanup:    -1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewMemory[string](tc.defaultTTL, tc.cleanup)
			require.NotNil(t, cache.cache)
		})
	}
}

func TestMemory_Get(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setup         func(*Memory[string])
		key           string
		expectedValue string
		expectedOk    bool
		checkReport   func(*testing.T, fh.Report)
	}{
		{
			name: "get existing key",
			setup: func(m *Memory[string]) {
				ctx := context.Background()
				m.Set(ctx, "key1", "value1", fh.NewReport(fh.StatusSuccess, nil), time.Minute)
			},
			key:           "key1",
			expectedValue: "value1",
			expectedOk:    true,
			checkReport: func(t *testing.T, r fh.Report) {
				assert.Equal(t, fh.StatusSuccess, r.Status)
				assert.Nil(t, r.Err)
			},
		},
		{
			name: "get non-existent key",
			setup: func(m *Memory[string]) {
				// No setup needed
			},
			key:           "nonexistent",
			expectedValue: "",
			expectedOk:    false,
			checkReport: func(t *testing.T, r fh.Report) {
				assert.Equal(t, fh.StatusError, r.Status)
			},
		},
		{
			name: "get key with custom report",
			setup: func(m *Memory[string]) {
				ctx := context.Background()
				customErr := errors.New("custom error")
				m.Set(ctx, "key2", "value2", fh.NewReport(fh.StatusError, customErr), time.Minute)
			},
			key:           "key2",
			expectedValue: "value2",
			expectedOk:    true,
			checkReport: func(t *testing.T, r fh.Report) {
				assert.Equal(t, fh.StatusError, r.Status)
				assert.NotNil(t, r.Err)
				assert.Equal(t, "custom error", r.Err.Error())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewMemory[string](5*time.Minute, 10*time.Minute)
			tc.setup(&cache)

			ctx := context.Background()
			value, report, ok := cache.Get(ctx, tc.key)

			assert.Equal(t, tc.expectedValue, value)
			assert.Equal(t, tc.expectedOk, ok)
			tc.checkReport(t, report)
		})
	}
}

func TestMemory_Set(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		key         string
		value       string
		ttl         time.Duration
		inputReport fh.Report
	}{
		{
			name:        "set with standard TTL",
			key:         "key1",
			value:       "value1",
			ttl:         time.Minute,
			inputReport: fh.NewReport(fh.StatusSuccess, nil),
		},
		{
			name:        "set with no expiration",
			key:         "key2",
			value:       "value2",
			ttl:         -1,
			inputReport: fh.NewReport(fh.StatusSuccess, nil),
		},
		{
			name:        "set with error report",
			key:         "key3",
			value:       "value3",
			ttl:         time.Hour,
			inputReport: fh.NewReport(fh.StatusError, errors.New("test error")),
		},
		{
			name:        "overwrite existing key",
			key:         "key1",
			value:       "new_value",
			ttl:         time.Minute,
			inputReport: fh.NewReport(fh.StatusSuccess, nil),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewMemory[string](5*time.Minute, 10*time.Minute)
			ctx := context.Background()

			report := cache.Set(ctx, tc.key, tc.value, tc.inputReport, tc.ttl)

			require.Equal(t, fh.StatusSuccess, report.Status)
			require.Nil(t, report.Err)

			// Verify the value was actually set
			value, storedReport, ok := cache.Get(ctx, tc.key)
			require.True(t, ok)
			assert.Equal(t, tc.value, value)
			assert.Equal(t, tc.inputReport.Status, storedReport.Status)
		})
	}
}

func TestMemory_GetOrSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(*Memory[string])
		key            string
		ttl            time.Duration
		callback       func() (string, fh.Report)
		expectedValue  string
		expectedCached bool
		callbackCalled *bool
	}{
		{
			name: "get existing cached value",
			setup: func(m *Memory[string]) {
				ctx := context.Background()
				m.Set(ctx, "cached_key", "cached_value", fh.NewReport(fh.StatusSuccess, nil), time.Minute)
			},
			key: "cached_key",
			ttl: time.Minute,
			callback: func() (string, fh.Report) {
				return "new_value", fh.NewReport(fh.StatusSuccess, nil)
			},
			expectedValue:  "cached_value",
			expectedCached: true,
			callbackCalled: new(bool),
		},
		{
			name: "set new value when cache miss",
			setup: func(m *Memory[string]) {
				// No setup - cache miss
			},
			key: "new_key",
			ttl: time.Minute,
			callback: func() (string, fh.Report) {
				return "computed_value", fh.NewReport(fh.StatusSuccess, nil)
			},
			expectedValue:  "computed_value",
			expectedCached: false,
			callbackCalled: new(bool),
		},
		{
			name: "callback with error report",
			setup: func(m *Memory[string]) {
				// No setup - cache miss
			},
			key: "error_key",
			ttl: time.Minute,
			callback: func() (string, fh.Report) {
				return "error_value", fh.NewReport(fh.StatusError, errors.New("callback error"))
			},
			expectedValue:  "error_value",
			expectedCached: false,
			callbackCalled: new(bool),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewMemory[string](5*time.Minute, 10*time.Minute)
			tc.setup(&cache)

			callbackInvoked := false
			wrappedCallback := func() (string, fh.Report) {
				callbackInvoked = true
				return tc.callback()
			}

			ctx := context.Background()
			value, report, cached := cache.GetOrSet(ctx, tc.key, tc.ttl, wrappedCallback)

			assert.Equal(t, tc.expectedValue, value)
			assert.Equal(t, tc.expectedCached, cached)
			assert.NotNil(t, report)

			if tc.expectedCached {
				assert.False(t, callbackInvoked, "callback should not be invoked for cached values")
			} else {
				assert.True(t, callbackInvoked, "callback should be invoked for cache misses")
			}
		})
	}
}

func TestMemory_Expiration(t *testing.T) {
	t.Parallel()

	cache := NewMemory[string](100*time.Millisecond, 50*time.Millisecond)
	ctx := context.Background()

	// Set a value with short TTL
	cache.Set(ctx, "expiring_key", "expiring_value", fh.NewReport(fh.StatusSuccess, nil), 100*time.Millisecond)

	// Value should exist immediately
	value, _, ok := cache.Get(ctx, "expiring_key")
	require.True(t, ok)
	assert.Equal(t, "expiring_value", value)

	// Wait for expiration
	time.Sleep(200 * time.Millisecond)

	// Value should be gone
	_, _, ok = cache.Get(ctx, "expiring_key")
	assert.False(t, ok)
}

func TestMemory_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	cache := NewMemory[int](5*time.Minute, 10*time.Minute)
	ctx := context.Background()

	const goroutines = 100
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 3) // readers, writers, and getOrSet operations

	// Concurrent readers
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := "key" + string(rune(id%10))
				cache.Get(ctx, key)
			}
		}(i)
	}

	// Concurrent writers
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := "key" + string(rune(id%10))
				cache.Set(ctx, key, id*j, fh.NewReport(fh.StatusSuccess, nil), time.Minute)
			}
		}(i)
	}

	// Concurrent GetOrSet operations
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := "getOrSet_key" + string(rune(id%10))
				cache.GetOrSet(ctx, key, time.Minute, func() (int, fh.Report) {
					return id * j, fh.NewReport(fh.StatusSuccess, nil)
				})
			}
		}(i)
	}

	wg.Wait()
	// If we reach here without race conditions or panics, the test passes
}

func TestMemory_DifferentTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		test func(*testing.T)
	}{
		{
			name: "int cache",
			test: func(t *testing.T) {
				cache := NewMemory[int](time.Minute, time.Minute)
				ctx := context.Background()
				cache.Set(ctx, "num", 42, fh.NewReport(fh.StatusSuccess, nil), time.Minute)
				value, _, ok := cache.Get(ctx, "num")
				require.True(t, ok)
				assert.Equal(t, 42, value)
			},
		},
		{
			name: "struct cache",
			test: func(t *testing.T) {
				type TestStruct struct {
					Name  string
					Value int
				}
				cache := NewMemory[TestStruct](time.Minute, time.Minute)
				ctx := context.Background()
				testData := TestStruct{Name: "test", Value: 100}
				cache.Set(ctx, "struct", testData, fh.NewReport(fh.StatusSuccess, nil), time.Minute)
				value, _, ok := cache.Get(ctx, "struct")
				require.True(t, ok)
				assert.Equal(t, testData, value)
			},
		},
		{
			name: "pointer cache",
			test: func(t *testing.T) {
				cache := NewMemory[*string](time.Minute, time.Minute)
				ctx := context.Background()
				str := "pointer_value"
				cache.Set(ctx, "ptr", &str, fh.NewReport(fh.StatusSuccess, nil), time.Minute)
				value, _, ok := cache.Get(ctx, "ptr")
				require.True(t, ok)
				require.NotNil(t, value)
				assert.Equal(t, "pointer_value", *value)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.test(t)
		})
	}
}
