// Package cache provides in-memory caching implementations for firehose middleware.
package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	fh "github.com/emad-elsaid/firehose"

	gocache "github.com/patrickmn/go-cache"
)

// ErrCacheMiss indicates the cache does not contain the requested key.
var ErrCacheMiss = errors.New("cache miss")

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

// MemoryItem wraps a cached value with its associated report.
type MemoryItem[O any] struct {
	Value  O
	Report fh.Report
}

// Get retrieves a value from the cache by key, returning the value, report, and whether it was found.
func (m Memory[O]) Get(_ context.Context, key string) (O, fh.Report, bool) {
	v, ok := m.cache.Get(key)
	if !ok {
		var zero O
		return zero, fh.NewReport(ErrCacheMiss), false
	}

	item, ok := v.(MemoryItem[O])
	if !ok {
		var zero O
		return zero, fh.NewReport(fmt.Errorf("invalid cached item type for key %q", key)), false
	}

	return item.Value, item.Report, true
}

// Set stores a value in the cache with the given key, report, and TTL.
func (m Memory[O]) Set(_ context.Context, key string, value O, report fh.Report, ttl time.Duration) fh.Report {
	m.cache.Set(key, MemoryItem[O]{Value: value, Report: report}, ttl)

	return fh.NewReport(nil)
}

// GetOrSet retrieves a value from cache or sets it using the callback if not found.
func (m Memory[O]) GetOrSet(
	ctx context.Context,
	key string,
	ttl time.Duration,
	callback func() (O, fh.Report),
) (O, fh.Report, bool) {
	v, report, ok := m.Get(ctx, key)
	if ok {
		return v, report, true
	}

	o, report := callback()
	m.Set(ctx, key, o, report, ttl)

	return o, report, false
}
