package destinations

import (
	"context"
)

// Func is an adapter that allows using ordinary functions as firehose.Destination.
type Func[T any] func(ctx context.Context, event T) error

// Send calls the underlying function.
func (f Func[T]) Send(ctx context.Context, event T) error {
	return f(ctx, event)
}
