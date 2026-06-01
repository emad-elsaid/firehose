package actions

import (
	"context"

	"github.com/emad-elsaid/firehose"
)

type Event[In, Out firehose.Event] struct {
	Output Out
}

func (e Event[In, Out]) Process(_ context.Context, event In) (Out, error) {
	return e.Output, nil
}
