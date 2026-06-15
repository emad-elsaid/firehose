package actions

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
)

// Event is an action that emits a static event when processed.
type Event[I, O firehose.Event] struct {
	Output O
}

// Process returns the configured event as output.
func (e Event[I, O]) Process(_ context.Context, _ I, _ boolexpr.Symbols) (O, firehose.Report) {
	return e.Output, firehose.NewReport(firehose.StatusSuccess, nil)
}
