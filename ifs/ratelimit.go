package ifs

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	"golang.org/x/time/rate"
)

// RateLimit limits the rate at which events can be processed.
type RateLimit[I any] struct {
	// Limit is the maximum rate at which events can be processed.
	// For example, rate.Limit(10) allows 10 events per second.
	Limit rate.Limit
	// Burst is the maximum number of events that can be processed in a single burst.
	// If zero, defaults to 1.
	Burst int

	limiter *rate.Limiter
}

// Evaluate waits for rate limiter permission before allowing the event to be processed.
func (r *RateLimit[I]) Evaluate(ctx context.Context, _ I, _ boolexpr.Symbols) (bool, error) {
	if r.Limit <= 0 {
		return true, nil
	}

	if r.limiter == nil {
		burst := r.Burst
		if burst == 0 {
			burst = 1
		}

		r.limiter = rate.NewLimiter(r.Limit, burst)
	}

	err := r.limiter.Wait(ctx)
	if err != nil {
		return false, err
	}

	return true, nil
}
