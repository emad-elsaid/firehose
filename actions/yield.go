// Package actions provides event processing action implementations.
package actions

import "context"

// Yield is an action that passes through events unchanged.
type Yield[T any] struct{}

// Process returns the input event unchanged.
func (Yield[T]) Process(_ context.Context, event T) (T, error) {
	return event, nil
}
