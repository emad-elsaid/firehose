package actions

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
	"golang.org/x/time/rate"
)

var StatusRateLimitError fh.Status = "rate_limit_error"

type RateLimit[I, O fh.Event] struct {
	limiter    *rate.Limiter
	downstream fh.Action[I, O]
}

func (a *RateLimit[I, O]) Wrap(ctx context.Context, rule fh.Rule[I, O], action fh.Action[I, O], in I) (fh.Action[I, O], error) {
	if rule.RateLimit <= 0 {
		return action, nil
	}

	a.limiter = rate.NewLimiter(rule.RateLimit, 1)
	a.downstream = action

	return a, nil
}

func (a *RateLimit[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, fh.Report) {
	if err := a.limiter.Wait(ctx); err != nil {
		var zero O

		return zero, fh.NewAbortReport(StatusRateLimitError, err)
	}

	return a.downstream.Process(ctx, event, syms)
}
