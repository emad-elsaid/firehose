package actions

import (
	"context"
	"strconv"
	"time"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
)

// CacheStorage defines the interface for caching action results.
type CacheStorage[O fh.Event] interface {
	Get(ctx context.Context, key string) (O, fh.Report, bool)
	Set(ctx context.Context, key string, value O, report fh.Report, ttl time.Duration) fh.Report
	GetOrSet(ctx context.Context, key string, ttl time.Duration, cb func() (O, fh.Report)) (O, fh.Report, bool)
}

// Cache is an action middleware that caches action results based on event IDs.
type Cache[I, O fh.Event] struct {
	Cache CacheStorage[O] `validate:"required"`

	downstream fh.Action[I, O]
	ttl        time.Duration
}

// Wrap wraps an action with caching if CacheFor is configured on the rule.
func (c *Cache[I, O]) Wrap(
	_ context.Context,
	rule fh.Rule[I, O],
	action fh.Action[I, O],
	_ I,
) (fh.Action[I, O], error) {
	if rule.CacheFor == 0 {
		return action, nil
	}

	c.ttl = rule.CacheFor
	c.downstream = action

	return c, nil
}

// Process retrieves cached results or executes the downstream action and caches the result.
func (c *Cache[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, fh.Report) {
	eventID, err := fh.EventID(event)
	if err != nil {
		var zero O

		return zero, fh.NewAbortReport(fh.StatusActionError, err)
	}

	out, report, _ := c.Cache.GetOrSet(ctx, strconv.FormatUint(eventID, 10), c.ttl, func() (O, fh.Report) {
		return c.downstream.Process(ctx, event, syms)
	})

	return out, report
}
