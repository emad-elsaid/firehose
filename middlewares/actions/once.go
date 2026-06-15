package actions

import (
	"context"
	"strconv"
	"time"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
)

var StatusOnceHit fh.Status = "once_hit"

type Once[I, O fh.Event] struct {
	Cache CacheStorage[string] `validate:"required"`

	downstream fh.Action[I, O]
	ttl        time.Duration
}

func (c *Once[I, O]) Wrap(ctx context.Context, rule fh.Rule[I, O], action fh.Action[I, O], in I) (fh.Action[I, O], error) {
	if rule.OnceEvery == 0 {
		return action, nil
	}

	c.ttl = rule.OnceEvery
	c.downstream = action

	return c, nil
}

func (c *Once[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, fh.Report) {
	id, err := fh.EventID(event)
	if err != nil {
		var zero O

		return zero, fh.NewAbortReport(fh.StatusActionError, err)
	}

	key := strconv.FormatUint(id, 10)

	_, _, ok := c.Cache.Get(ctx, key)
	if ok {
		var zero O

		return zero, fh.NewAbortReport(StatusOnceHit, nil)
	}

	out, report := c.downstream.Process(ctx, event, syms)
	c.Cache.Set(ctx, key, "1", fh.NewReport("", nil), c.ttl)

	return out, report

}
