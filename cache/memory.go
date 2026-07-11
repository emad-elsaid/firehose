// Package cache provides in-memory caching implementations for firehose middleware.
package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	gocache "github.com/patrickmn/go-cache"
)

// ErrCacheMiss indicates the cache does not contain the requested key.
var ErrCacheMiss = errors.New("cache miss")

// ErrInvalidCachedItemType indicates a value exists in cache with an unexpected type.
var ErrInvalidCachedItemType = errors.New("invalid cached item type")

// NewMemory creates a new in-memory cache with the specified default TTL and cleanup interval.
func NewMemory[O any](defaultTTL, cleanup time.Duration) Memory[O] {
	return Memory[O]{
		cache: gocache.New(defaultTTL, cleanup),
	}
}

// Memory is an in-memory cache implementation using go-cache.
type Memory[O any] struct {
	cache *gocache.Cache
}

// MemoryItem wraps a cached value with its associated error.
type MemoryItem[O any] struct {
	Value O
	Err   error
}

// Get retrieves a value from the cache by key, returning the value, ok status, and error.
func (m Memory[O]) Get(_ context.Context, key string) (O, bool, error) {
	cachedValue, found := m.cache.Get(key)
	if !found {
		var zero O

		return zero, false, ErrCacheMiss
	}

	item, ok := cachedValue.(MemoryItem[O])
	if !ok {
		var zero O

		return zero, false, fmt.Errorf("%w: key %q", ErrInvalidCachedItemType, key)
	}

	return item.Value, true, item.Err
}

// Set stores a value in the cache with the given key and TTL.
func (m Memory[O]) Set(_ context.Context, key string, ttl time.Duration, value O) error {
	m.cache.Set(key, MemoryItem[O]{Value: value, Err: nil}, ttl)

	return nil
}

// GetOrSet retrieves a value from cache or sets it using the callback if not found.
func (m Memory[O]) GetOrSet(
	ctx context.Context,
	key string,
	ttl time.Duration,
	callback func() (O, error),
) (O, bool, error) {
	value, ok, err := m.Get(ctx, key)
	if ok {
		return value, true, err
	}

	output, computedErr := callback()

	err = m.Set(ctx, key, ttl, output)
	if err != nil {
		computedErr = err
	}

	return output, false, computedErr
}
