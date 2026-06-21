package actions

import (
	"context"
	"strconv"
	"time"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
)

// StatusOnceHit indicates an event was skipped because it was already processed within the time window.
var StatusOnceHit fh.Status = "once_hit"

// Once is an action middleware that ensures actions are executed at most once per event within a time window.
type Once[I, O fh.Event] struct {
	Cache CacheStorage[string] `validate:"required"`

	downstream fh.Action[I, O]
	ttl        time.Duration
}

// Wrap wraps an action with once-per-event execution if OnceEvery is configured on the rule.
func (c *Once[I, O]) Wrap(
	_ context.Context,
	rule fh.Rule[I, O],
	action fh.Action[I, O],
	_ I,
) (fh.Action[I, O], error) {
	if rule.OnceEvery == 0 {
		return action, nil
	}

	c.ttl = rule.OnceEvery
	c.downstream = action

	return c, nil
}

// Process checks if the event was recently processed and executes the downstream action if not.
func (c *Once[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, fh.Report) {
	eventID, err := fh.EventID(event)
	if err != nil {
		var zero O

		return zero, fh.NewAbortReport(fh.StatusActionError, err)
	}

	key := strconv.FormatUint(eventID, 10)

	_, _, ok := c.Cache.Get(ctx, key)
	if ok {
		var zero O

		return zero, fh.NewAbortReport(StatusOnceHit, nil)
	}

	out, report := c.downstream.Process(ctx, event, syms)
	c.Cache.Set(ctx, key, "1", fh.NewReport("", nil), c.ttl)

	return out, report
}
