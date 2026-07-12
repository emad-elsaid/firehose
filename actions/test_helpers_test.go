package actions

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	"github.com/stretchr/testify/mock"
)

// event is a simple test event type
type event struct{}

// action is a simple mock action type for testing that embeds mock.Mock
type action[I, O any] struct {
	mock.Mock
}

func (a *action[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, error) {
	args := a.Called(ctx, event, syms)

	var result O
	if args.Get(0) != nil {
		result = args.Get(0).(O)
	}

	return result, args.Error(1)
}
