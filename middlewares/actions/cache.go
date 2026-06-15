package actions

import (
	"context"
	"strconv"
	"time"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
)

type CacheStorage[O fh.Event] interface {
	Get(ctx context.Context, key string) (O, fh.Report, bool)
	Set(ctx context.Context, key string, value O, report fh.Report, ttl time.Duration) fh.Report
	GetOrSet(ctx context.Context, key string, ttl time.Duration, cb func() (O, fh.Report)) (O, fh.Report, bool)
}

type Cache[I, O fh.Event] struct {
	Cache CacheStorage[O] `validate:"required"`

	downstream fh.Action[I, O]
	ttl        time.Duration
}

func (c *Cache[I, O]) Wrap(ctx context.Context, rule fh.Rule[I, O], action fh.Action[I, O], in I) (fh.Action[I, O], error) {
	if rule.CacheFor == 0 {
		return action, nil
	}

	c.ttl = rule.CacheFor
	c.downstream = action

	return c, nil
}

func (c *Cache[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, fh.Report) {
	id, err := fh.EventID(event)
	if err != nil {
		var zero O

		return zero, fh.NewAbortReport(fh.StatusActionError, err)
	}

	out, report, _ := c.Cache.GetOrSet(ctx, strconv.FormatUint(id, 10), c.ttl, func() (O, fh.Report) {
		return c.downstream.Process(ctx, event, syms)
	})

	// TODO report cache/hit/miss

	return out, report

}
