package middlewares

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/mock"
)

// event is a simple test event type
type event struct {
	Value string
}

// action is a simple mock action type for testing
type action[I, O firehose.Event] struct {
	mock.Mock
}

func (a *action[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, firehose.Report) {
	args := a.Called(ctx, event, syms)

	var result O
	if args.Get(0) != nil {
		result = args.Get(0).(O)
	}

	return result, args.Get(1).(firehose.Report)
}

// mockDestination implements Destination interface with mock.Mock for use in tests
type mockDestination[T firehose.Event] struct {
	mock.Mock
}

func (d *mockDestination[T]) Send(ctx context.Context, event T) firehose.Report {
	args := d.Called(ctx, event)
	return args.Get(0).(firehose.Report)
}
