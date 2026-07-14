// Package sources provides reusable source implementations for firehose rules.
package sources

import (
	"context"

	"github.com/emad-elsaid/firehose"
)

// Func is an adapter that allows using ordinary functions as firehose.Source.
type Func[T any] func(ctx context.Context, cb firehose.Callback[T]) (done <-chan struct{}, err error)

// Start calls the underlying function.
func (f Func[T]) Start(ctx context.Context, cb firehose.Callback[T]) (<-chan struct{}, error) {
	return f(ctx, cb)
}
