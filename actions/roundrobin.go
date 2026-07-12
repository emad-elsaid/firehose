package actions

import (
	"context"
	"errors"
	"sync"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
)

// ErrNoActionsConfigured is returned when an action dispatcher has no
// downstream actions.
var ErrNoActionsConfigured = errors.New("no actions configured")

// RoundRobin dispatches events across actions in sequence.
type RoundRobin[I, O any] struct {
	Actions []fh.Action[I, O] `validate:"required,min=1,dive,required"`

	mutex sync.Mutex
	next  int
}

// Process dispatches the event to the next action.
func (r *RoundRobin[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, error) {
	if len(r.Actions) == 0 {
		var zero O

		return zero, fh.ActionError{Err: ErrNoActionsConfigured}
	}

	r.mutex.Lock()
	index := r.next % len(r.Actions)
	r.next = (r.next + 1) % len(r.Actions)
	r.mutex.Unlock()

	return r.Actions[index].Process(ctx, event, syms)
}
