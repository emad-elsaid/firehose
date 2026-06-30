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
		{name: "standard TTL and cleanup", defaultTTL: 5 * time.Minute, cleanup: 10 * time.Minute},
		{name: "short TTL and cleanup", defaultTTL: 1 * time.Second, cleanup: 2 * time.Second},
		{name: "no expiration", defaultTTL: -1, cleanup: -1},
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

	customErr := errors.New("custom error")

	tests := []struct {
		name          string
		setup         func(*Memory[string])
		key           string
		expectedValue string
		expectedOK    bool
		checkReport   func(*testing.T, fh.Report)
	}{
		{
			name: "get existing key",
			setup: func(m *Memory[string]) {
				m.Set(context.Background(), "key1", "value1", fh.NewReport(nil), time.Minute)
			},
			key:           "key1",
			expectedValue: "value1",
			expectedOK:    true,
			checkReport: func(t *testing.T, r fh.Report) {
				assert.NoError(t, r.Err)
			},
		},
		{
			name: "get non-existent key",
			setup: func(*Memory[string]) {
				// No setup needed
			},
			key:           "nonexistent",
			expectedValue: "",
			expectedOK:    false,
			checkReport: func(t *testing.T, r fh.Report) {
				assert.ErrorIs(t, r.Err, ErrCacheMiss)
			},
		},
		{
			name: "get key with custom report",
			setup: func(m *Memory[string]) {
				m.Set(context.Background(), "key2", "value2", fh.NewReport(customErr), time.Minute)
			},
			key:           "key2",
			expectedValue: "value2",
			expectedOK:    true,
			checkReport: func(t *testing.T, r fh.Report) {
				assert.ErrorIs(t, r.Err, customErr)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewMemory[string](5*time.Minute, 10*time.Minute)
			tc.setup(&cache)

			value, report, ok := cache.Get(context.Background(), tc.key)

			assert.Equal(t, tc.expectedValue, value)
			assert.Equal(t, tc.expectedOK, ok)
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
		wantErr     error
	}{
		{name: "set with standard TTL", key: "key1", value: "value1", ttl: time.Minute, inputReport: fh.NewReport(nil), wantErr: nil},
		{name: "set with no expiration", key: "key2", value: "value2", ttl: -1, inputReport: fh.NewReport(nil), wantErr: nil},
		{name: "set with error report", key: "key3", value: "value3", ttl: time.Hour, inputReport: fh.NewReport(errors.New("test error")), wantErr: errors.New("test error")},
		{name: "overwrite existing key", key: "key1", value: "new_value", ttl: time.Minute, inputReport: fh.NewReport(nil), wantErr: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewMemory[string](5*time.Minute, 10*time.Minute)

			report := cache.Set(context.Background(), tc.key, tc.value, tc.inputReport, tc.ttl)
			require.NoError(t, report.Err)

			value, storedReport, ok := cache.Get(context.Background(), tc.key)
			require.True(t, ok)
			assert.Equal(t, tc.value, value)
			if tc.wantErr == nil {
				assert.NoError(t, storedReport.Err)
			} else {
				assert.EqualError(t, storedReport.Err, tc.wantErr.Error())
			}
		})
	}
}

func TestMemory_GetOrSet(t *testing.T) {
	t.Parallel()

	callbackErr := errors.New("callback error")

	tests := []struct {
		name           string
		setup          func(*Memory[string])
		key            string
		ttl            time.Duration
		callback       func() (string, fh.Report)
		expectedValue  string
		expectedCached bool
		expectedErr    error
	}{
		{
			name: "get existing cached value",
			setup: func(m *Memory[string]) {
				m.Set(context.Background(), "cached_key", "cached_value", fh.NewReport(nil), time.Minute)
			},
			key:            "cached_key",
			ttl:            time.Minute,
			callback:       func() (string, fh.Report) { return "new_value", fh.NewReport(nil) },
			expectedValue:  "cached_value",
			expectedCached: true,
			expectedErr:    nil,
		},
		{
			name: "set new value when cache miss",
			setup: func(*Memory[string]) {
				// No setup - cache miss
			},
			key:            "new_key",
			ttl:            time.Minute,
			callback:       func() (string, fh.Report) { return "computed_value", fh.NewReport(nil) },
			expectedValue:  "computed_value",
			expectedCached: false,
			expectedErr:    nil,
		},
		{
			name: "callback with error report",
			setup: func(*Memory[string]) {
				// No setup - cache miss
			},
			key:            "error_key",
			ttl:            time.Minute,
			callback:       func() (string, fh.Report) { return "error_value", fh.NewReport(callbackErr) },
			expectedValue:  "error_value",
			expectedCached: false,
			expectedErr:    callbackErr,
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

			value, report, cached := cache.GetOrSet(context.Background(), tc.key, tc.ttl, wrappedCallback)

			assert.Equal(t, tc.expectedValue, value)
			assert.Equal(t, tc.expectedCached, cached)
			if tc.expectedErr == nil {
				assert.NoError(t, report.Err)
			} else {
				assert.ErrorIs(t, report.Err, tc.expectedErr)
			}

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

	cache.Set(ctx, "expiring_key", "expiring_value", fh.NewReport(nil), 100*time.Millisecond)

	value, report, ok := cache.Get(ctx, "expiring_key")
	require.True(t, ok)
	assert.Equal(t, "expiring_value", value)
	assert.NoError(t, report.Err)

	time.Sleep(200 * time.Millisecond)

	_, report, ok = cache.Get(ctx, "expiring_key")
	assert.False(t, ok)
	assert.ErrorIs(t, report.Err, ErrCacheMiss)
}

func TestMemory_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	cache := NewMemory[int](5*time.Minute, 10*time.Minute)
	ctx := context.Background()

	const goroutines = 100
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 3)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := "key" + string(rune(id%10))
				cache.Get(ctx, key)
			}
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := "key" + string(rune(id%10))
				cache.Set(ctx, key, id*j, fh.NewReport(nil), time.Minute)
			}
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := "getOrSet_key" + string(rune(id%10))
				cache.GetOrSet(ctx, key, time.Minute, func() (int, fh.Report) {
					return id * j, fh.NewReport(nil)
				})
			}
		}(i)
	}

	wg.Wait()
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
				cache.Set(ctx, "num", 42, fh.NewReport(nil), time.Minute)
				value, report, ok := cache.Get(ctx, "num")
				require.True(t, ok)
				assert.Equal(t, 42, value)
				assert.NoError(t, report.Err)
			},
		},
		{
			name: "struct cache",
			test: func(t *testing.T) {
				type testStruct struct {
					Name  string
					Value int
				}
				cache := NewMemory[testStruct](time.Minute, time.Minute)
				ctx := context.Background()
				data := testStruct{Name: "test", Value: 100}
				cache.Set(ctx, "struct", data, fh.NewReport(nil), time.Minute)
				value, report, ok := cache.Get(ctx, "struct")
				require.True(t, ok)
				assert.Equal(t, data, value)
				assert.NoError(t, report.Err)
			},
		},
		{
			name: "pointer cache",
			test: func(t *testing.T) {
				cache := NewMemory[*string](time.Minute, time.Minute)
				ctx := context.Background()
				str := "pointer_value"
				cache.Set(ctx, "ptr", &str, fh.NewReport(nil), time.Minute)
				value, report, ok := cache.Get(ctx, "ptr")
				require.True(t, ok)
				require.NotNil(t, value)
				assert.Equal(t, "pointer_value", *value)
				assert.NoError(t, report.Err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, tc.test)
	}
}
