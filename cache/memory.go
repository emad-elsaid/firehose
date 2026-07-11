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

// Get retrieves a value from the cache by key, returning the value, error, and whether it was found.
func (m Memory[O]) Get(_ context.Context, key string) (O, error, bool) {
	cachedValue, found := m.cache.Get(key)
	if !found {
		var zero O

		return zero, ErrCacheMiss, false
	}

	item, ok := cachedValue.(MemoryItem[O])
	if !ok {
		var zero O

		return zero, fmt.Errorf("%w: key %q", ErrInvalidCachedItemType, key), false
	}

	return item.Value, item.Err, true
}

// Set stores a value in the cache with the given key and TTL.
func (m Memory[O]) Set(_ context.Context, key string, ttl time.Duration, value O) error {
	m.cache.Set(key, MemoryItem[O]{Value: value}, ttl)

	return nil
}

// GetOrSet retrieves a value from cache or sets it using the callback if not found.
func (m Memory[O]) GetOrSet(
	ctx context.Context,
	key string,
	ttl time.Duration,
	callback func() (O, error),
) (O, error, bool) {
	value, err, ok := m.Get(ctx, key)
	if ok {
		return value, err, true
	}

	output, computedErr := callback()
	m.Set(ctx, key, ttl, output)

	return output, computedErr, false
}
