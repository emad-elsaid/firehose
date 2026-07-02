package destinations

import (
	"context"

	fh "github.com/emad-elsaid/firehose"
)

// Func is an adapter that allows using ordinary functions as firehose.Destination.
type Func[T any] func(ctx context.Context, event T) fh.Report

// Send calls the underlying function.
func (f Func[T]) Send(ctx context.Context, event T) fh.Report {
	return f(ctx, event)
}
