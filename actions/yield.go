// Package actions provides event processing action implementations.
package actions

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
)

// Yield is an action that passes through events unchanged.
type Yield[T any] struct{}

// Process returns the input event unchanged.
func (Yield[T]) Process(_ context.Context, event T, _ boolexpr.Symbols) (T, firehose.Report) {
	return event, firehose.NewReport(firehose.StatusSuccess, nil)
}
