package sources

import (
	"context"
	"errors"
	"sync"

	"github.com/emad-elsaid/firehose"
)

// ErrNotStarted is returned when emitting before Start is called.
var ErrNotStarted = errors.New("source is not started")

// Manual is a source that emits events only when Emit is called.
type Manual[T any] struct {
	mutex    sync.RWMutex
	callback firehose.Callback[T]
}

// Start registers the callback used by Emit and returns the same context.
func (m *Manual[T]) Start(ctx context.Context, cb firehose.Callback[T]) (context.Context, error) {
	m.mutex.Lock()
	m.callback = cb
	m.mutex.Unlock()

	return ctx, nil
}

// Emit sends one event to the registered callback.
func (m *Manual[T]) Emit(ctx context.Context, event T) error {
	return m.EmitWithReport(ctx, event, nil)
}

// EmitWithReport sends one event with a custom report sink.
func (m *Manual[T]) EmitWithReport(ctx context.Context, event T, report firehose.ReportFunc) error {
	m.mutex.RLock()
	callback := m.callback
	m.mutex.RUnlock()

	if callback == nil {
		return ErrNotStarted
	}

	callback(ctx, event, report)

	return nil
}
