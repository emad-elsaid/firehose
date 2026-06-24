package actions

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
	"golang.org/x/time/rate"
)

// StatusRateLimitError indicates an action was aborted due to rate limiting.
const StatusRateLimitError fh.Status = "rate_limit_error"

// RateLimit is an action middleware that limits the rate of action execution.
type RateLimit[I, O fh.Event] struct {
	limiter    *rate.Limiter
	downstream fh.Action[I, O]
}

// Wrap wraps an action with rate limiting if RateLimit is configured on the rule.
func (a *RateLimit[I, O]) Wrap(
	_ context.Context,
	rule *fh.Rule[I, O],
	action fh.Action[I, O],
	_ I,
) (fh.Action[I, O], error) {
	if rule.RateLimit <= 0 {
		return action, nil
	}

	a.limiter = rate.NewLimiter(rule.RateLimit, 1)
	a.downstream = action

	return a, nil
}

// Process waits for rate limiter permission before executing the downstream action.
func (a *RateLimit[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, fh.Report) {
	err := a.limiter.Wait(ctx)
	if err != nil {
		var zero O

		return zero, fh.NewAbortReport(StatusRateLimitError, err)
	}

	return a.downstream.Process(ctx, event, syms)
}
