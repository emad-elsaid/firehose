package condition

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
)

// Once ensures events are processed at most once within a time window.
type Once[I any] struct {
	// Duration is the time window within which an event with the same ID
	// will only be processed once.
	Duration time.Duration `validate:"required,gt=0"`
	// Cache stores event IDs to track which events have been processed. Cache
	// has to handle namespace isolation for different rules, so it is
	// recommended to use a namespaced cache implementation.
	Cache CacheStorage[string] `validate:"required"`
}

// Evaluate checks if the event was recently processed and returns false if it was.
func (o Once[I]) Evaluate(ctx context.Context, event I, _ boolexpr.Symbols) (bool, error) {
	id, err := firehose.EventID(event)
	if err != nil {
		return false, fmt.Errorf("failed to get event ID: %w", err)
	}

	key := strconv.FormatUint(id, 10)

	_, _, ok := o.Cache.Get(ctx, key)
	if ok {
		return false, nil
	}

	err = o.Cache.Set(ctx, key, o.Duration, "1")

	return true, err
}
