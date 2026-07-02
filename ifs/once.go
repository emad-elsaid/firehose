package ifs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
)

// ErrOnceCacheRequired is returned when Once.Duration is set but Cache is nil.
var ErrOnceCacheRequired = errors.New("Once: Cache is required when Duration > 0")

// Once ensures events are processed at most once within a time window.
type Once[I any] struct {
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
			return false, ErrOnceCacheRequired
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

	o.Cache.Set(ctx, key, "1", firehose.NewReport(nil), o.Duration)

	return true, nil
}
