// Package actions provides reusable action implementations for firehose rules.
package actions

import (
	"context"
	"strconv"
	"time"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
)

// CacheStorage defines the interface for caching action results.
type CacheStorage[O any] interface {
	Get(ctx context.Context, key string) (O, bool, error)
	Set(ctx context.Context, key string, ttl time.Duration, value O) error
	GetOrSet(ctx context.Context, key string, ttl time.Duration, cb func() (O, error)) (O, bool, error)
}

// Cache is an action that caches the results of another action based on event IDs.
type Cache[I, O any] struct {
	// Action is the downstream action to cache
	Action fh.Action[I, O] `validate:"required"`
	// Cache is the storage backend for cached results
	Cache CacheStorage[O] `validate:"required"`
	// TTL is how long to cache results for
	TTL time.Duration `validate:"required"`
}

// Process retrieves cached results or executes the downstream action and caches the result.
func (c *Cache[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, error) {
	eventID, err := fh.EventID(event)
	if err != nil {
		var zero O

		return zero, fh.ActionError{Err: err}
	}

	out, _, err := c.Cache.GetOrSet(ctx, strconv.FormatUint(eventID, 10), c.TTL, func() (O, error) {
		out, err := c.Action.Process(ctx, event, syms)

		return out, err
	})

	return out, err
}
