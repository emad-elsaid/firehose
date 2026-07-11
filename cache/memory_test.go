package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

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
		checkErr      func(*testing.T, error)
	}{
		{
			name: "get existing key",
			setup: func(m *Memory[string]) {
				m.Set(context.Background(), "key1", time.Minute, "value1")
			},
			key:           "key1",
			expectedValue: "value1",
			expectedOK:    true,
			checkErr: func(t *testing.T, err error) {
				assert.NoError(t, err)
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
			checkErr: func(t *testing.T, err error) {
				assert.ErrorIs(t, err, ErrCacheMiss)
			},
		},
		{
			name: "get key with custom error",
			setup: func(m *Memory[string]) {
				m.cache.Set("key2", MemoryItem[string]{Value: "value2", Err: customErr}, time.Minute)
			},
			key:           "key2",
			expectedValue: "value2",
			expectedOK:    true,
			checkErr: func(t *testing.T, err error) {
				assert.ErrorIs(t, err, customErr)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewMemory[string](5*time.Minute, 10*time.Minute)
			tc.setup(&cache)

			value, ok, err := cache.Get(context.Background(), tc.key)

			assert.Equal(t, tc.expectedValue, value)
			assert.Equal(t, tc.expectedOK, ok)
			tc.checkErr(t, err)
		})
	}
}

func TestMemory_Set(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		value   string
		ttl     time.Duration
		wantErr error
	}{
		{name: "set with standard TTL", key: "key1", value: "value1", ttl: time.Minute, wantErr: nil},
		{name: "set with no expiration", key: "key2", value: "value2", ttl: -1, wantErr: nil},
		{name: "overwrite existing key", key: "key1", value: "new_value", ttl: time.Minute, wantErr: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewMemory[string](5*time.Minute, 10*time.Minute)

			err := cache.Set(context.Background(), tc.key, tc.ttl, tc.value)
			require.NoError(t, err)

			value, ok, cachedErr := cache.Get(context.Background(), tc.key)
			require.True(t, ok)
			assert.Equal(t, tc.value, value)
			assert.NoError(t, cachedErr)
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
		callback       func() (string, error)
		expectedValue  string
		expectedCached bool
		expectedErr    error
	}{
		{
			name: "get existing cached value",
			setup: func(m *Memory[string]) {
				m.Set(context.Background(), "cached_key", time.Minute, "cached_value")
			},
			key:            "cached_key",
			ttl:            time.Minute,
			callback:       func() (string, error) { return "new_value", nil },
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
			callback:       func() (string, error) { return "computed_value", nil },
			expectedValue:  "computed_value",
			expectedCached: false,
			expectedErr:    nil,
		},
		{
			name: "callback with error",
			setup: func(*Memory[string]) {
				// No setup - cache miss
			},
			key:            "error_key",
			ttl:            time.Minute,
			callback:       func() (string, error) { return "error_value", callbackErr },
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
			wrappedCallback := func() (string, error) {
				callbackInvoked = true
				return tc.callback()
			}

			value, cached, err := cache.GetOrSet(context.Background(), tc.key, tc.ttl, wrappedCallback)

			assert.Equal(t, tc.expectedValue, value)
			assert.Equal(t, tc.expectedCached, cached)
			if tc.expectedErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tc.expectedErr)
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

	err := cache.Set(ctx, "expiring_key", 100*time.Millisecond, "expiring_value")
	require.NoError(t, err)

	value, ok, cacheErr := cache.Get(ctx, "expiring_key")
	require.True(t, ok)
	assert.Equal(t, "expiring_value", value)
	assert.NoError(t, cacheErr)

	time.Sleep(200 * time.Millisecond)

	_, ok, cacheErr = cache.Get(ctx, "expiring_key")
	assert.False(t, ok)
	assert.ErrorIs(t, cacheErr, ErrCacheMiss)
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
				cache.Set(ctx, key, time.Minute, id*j)
			}
		}(i)
	}

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := "getOrSet_key" + string(rune(id%10))
				cache.GetOrSet(ctx, key, time.Minute, func() (int, error) {
					return id * j, nil
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
				err := cache.Set(ctx, "num", time.Minute, 42)
				require.NoError(t, err)
				value, ok, cacheErr := cache.Get(ctx, "num")
				require.True(t, ok)
				assert.Equal(t, 42, value)
				assert.NoError(t, cacheErr)
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
				err := cache.Set(ctx, "struct", time.Minute, data)
				require.NoError(t, err)
				value, ok, cacheErr := cache.Get(ctx, "struct")
				require.True(t, ok)
				assert.Equal(t, data, value)
				assert.NoError(t, cacheErr)
			},
		},
		{
			name: "pointer cache",
			test: func(t *testing.T) {
				cache := NewMemory[*string](time.Minute, time.Minute)
				ctx := context.Background()
				str := "pointer_value"
				err := cache.Set(ctx, "ptr", time.Minute, &str)
				require.NoError(t, err)
				value, ok, cacheErr := cache.Get(ctx, "ptr")
				require.True(t, ok)
				require.NotNil(t, value)
				assert.Equal(t, "pointer_value", *value)
				assert.NoError(t, cacheErr)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, tc.test)
	}
}
