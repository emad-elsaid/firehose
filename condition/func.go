package condition

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
)

// Func is an adapter that allows using ordinary functions as firehose.Where.
type Func[I any] func(ctx context.Context, event I, syms boolexpr.Symbols) (bool, error)

// Evaluate calls the underlying function.
func (f Func[I]) Evaluate(ctx context.Context, event I, syms boolexpr.Symbols) (bool, error) {
	return f(ctx, event, syms)
}
