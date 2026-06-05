package actions

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
)

// Event is an action that emits a static event when processed.
type Event[In, Out firehose.Event] struct {
	Output Out
}

// Process returns the configured event as output.
func (e Event[In, Out]) Process(_ context.Context, _ In, _ boolexpr.Symbols) (Out, firehose.Report) {
	return e.Output, firehose.NewReport(firehose.StatusSuccess, nil)
}
