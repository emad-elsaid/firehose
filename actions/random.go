package actions

import (
	"context"
	cryptorand "crypto/rand"
	"math/big"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
)

// Random dispatches events to a random action.
type Random[I, O any] struct {
	Actions []fh.Action[I, O] `validate:"required,min=1,dive,required"`
}

// Process dispatches the event to a random action.
func (r *Random[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, fh.Report) {
	if len(r.Actions) == 0 {
		var zero O

		return zero, fh.NewReport(fh.ActionError{Err: ErrNoActionsConfigured})
	}

	index, err := r.nextIndex(len(r.Actions))
	if err != nil {
		var zero O

		return zero, fh.NewReport(fh.ActionError{Err: err})
	}

	return r.Actions[index].Process(ctx, event, syms)
}

func (r *Random[I, O]) nextIndex(size int) (int, error) {
	upperBound := big.NewInt(int64(size))

	index, err := cryptorand.Int(cryptorand.Reader, upperBound)
	if err != nil {
		return 0, err
	}

	return int(index.Int64()), nil
}
