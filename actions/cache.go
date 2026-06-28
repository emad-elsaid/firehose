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
	Get(ctx context.Context, key string) (O, fh.Report, bool)
	Set(ctx context.Context, key string, value O, report fh.Report, ttl time.Duration) fh.Report
	GetOrSet(ctx context.Context, key string, ttl time.Duration, cb func() (O, fh.Report)) (O, fh.Report, bool)
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
func (c *Cache[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, fh.Report) {
	eventID, err := fh.EventID(event)
	if err != nil {
		var zero O

		return zero, fh.NewAbortReport(fh.StatusActionError, err)
	}

	out, report, _ := c.Cache.GetOrSet(ctx, strconv.FormatUint(eventID, 10), c.TTL, func() (O, fh.Report) {
		return c.Action.Process(ctx, event, syms)
	})

	return out, report
}
