package actions

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
)

// Func is an adapter that allows using ordinary functions as firehose.Action.
type Func[I, O any] func(ctx context.Context, event I, syms boolexpr.Symbols) (O, fh.Report)

// Process calls the underlying function.
func (f Func[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, fh.Report) {
	return f(ctx, event, syms)
}
