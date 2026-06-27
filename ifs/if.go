// Package ifs provides condition implementations for rule evaluation.
package ifs

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
	"golang.org/x/time/rate"
)

// Event is the interface that events with attributes must implement.
type Event interface {
	Attributes(ctx context.Context) (map[string]any, error)
}

// CacheStorage is an interface for storing and retrieving cached values.
type CacheStorage[V any] interface {
	Get(ctx context.Context, key string) (value V, report firehose.Report, ok bool)
	Set(ctx context.Context, key string, value V, report firehose.Report, ttl time.Duration)
}

// Cond is a string-based condition that evaluates boolean expressions against event attributes.
type Cond[I Event] string

// Evaluate parses and evaluates the boolean expression against the provided symbols.
func (c Cond[I]) Evaluate(_ context.Context, _ I, syms boolexpr.Symbols) (bool, error) {
	if c == "" {
		return true, nil
	}

	expr, err := boolexpr.Parse(string(c))
	if err != nil {
		return false, err
	}

	return boolexpr.EvalExpression(expr, syms)
}

// RateLimit limits the rate at which events can be processed.
type RateLimit[I Event] struct {
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

// Once ensures events are processed at most once within a time window.
type Once[I Event] struct {
	// Duration is the time window within which an event with the same ID
	// will only be processed once. Zero means no deduplication.
	Duration time.Duration
	// Cache stores event IDs to track which events have been processed.
	Cache CacheStorage[string]

	initialized bool
}

// Evaluate checks if the event was recently processed and returns false if it was.
func (o *Once[I]) Evaluate(ctx context.Context, event I, _ boolexpr.Symbols) (bool, error) {
	if o.Duration == 0 {
		return true, nil
	}

	if !o.initialized {
		if o.Cache == nil {
			return false, fmt.Errorf("Once: Cache is required when Duration > 0")
		}
		o.initialized = true
	}

	id, err := firehose.EventID(event)
	if err != nil {
		return false, fmt.Errorf("failed to get event ID: %w", err)
	}

	key := strconv.FormatUint(id, 10)

	_, _, ok := o.Cache.Get(ctx, key)
	if ok {
		return false, nil
	}

	o.Cache.Set(ctx, key, "1", firehose.NewReport("", nil), o.Duration)

	return true, nil
}
