package destinations

import (
	"context"
	"sync"
)

// Accumulator stores all received events in memory.
type Accumulator[T any] struct {
	mutex sync.Mutex
	items []T
}

// Send appends the event to the in-memory slice.
func (a *Accumulator[T]) Send(_ context.Context, event T) error {
	a.mutex.Lock()
	a.items = append(a.items, event)
	a.mutex.Unlock()

	return nil
}

// Items returns a copy of the accumulated events.
func (a *Accumulator[T]) Items() []T {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	result := make([]T, len(a.items))
	copy(result, a.items)

	return result
}
