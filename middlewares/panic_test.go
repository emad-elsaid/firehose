package middlewares

import (
	"context"
	"errors"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPanic_WrapCallback(t *testing.T) {
	mw := &Panic[*event, *event]{}
	wrappedCallback, err := mw.WrapCallback(
		context.Background(),
		&firehose.SQLRule[*event, *event]{},
		func(context.Context, *event, firehose.ErrorHandler) {},
	)
	require.NoError(t, err)
	require.NotNil(t, wrappedCallback)
	require.NotNil(t, mw.downstreamCallback)
}

func TestPanic_WrapAction(t *testing.T) {
	mw := &Panic[*event, *event]{}
	mockAction := &panicAction{}

	wrappedAction, err := mw.WrapAction(
		context.Background(),
		&firehose.SQLRule[*event, *event]{},
		mockAction,
	)
	require.NoError(t, err)
	require.Same(t, mw, wrappedAction)
	require.Same(t, mockAction, mw.downstreamAction)
}

func TestPanic_WrapDestination(t *testing.T) {
	mw := &Panic[*event, *event]{}
	mockDest := &simpleDestination[*event]{returnReport: nil}

	wrappedDest, err := mw.WrapDestination(
		context.Background(),
		&firehose.SQLRule[*event, *event]{},
		mockDest,
	)
	require.NoError(t, err)
	require.Same(t, mw, wrappedDest)
	require.Same(t, mockDest, mw.downstreamDest)
}

func TestPanic_RecoverCallback(t *testing.T) {
	tests := []struct {
		name       string
		downstream firehose.Callback[*event]
		assertion  func(t *testing.T, reports []error)
	}{
		{
			name: "recovers from panic",
			downstream: func(_ context.Context, _ *event, _ firehose.ErrorHandler) {
				panic("callback panic!")
			},
			assertion: func(t *testing.T, reports []error) {
				require.Len(t, reports, 1)
				assert.ErrorIs(t, reports[0], ErrPanicRecovered)
				assert.Contains(t, reports[0].Error(), "callback panic!")
			},
		},
		{
			name: "passes through report",
			downstream: func(_ context.Context, _ *event, report firehose.ErrorHandler) {
				// Report nil - should not be collected since nil means success
				report(nil)
			},
			assertion: func(t *testing.T, reports []error) {
				// Nil errors are not reported
				require.Len(t, reports, 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mw := &Panic[*event, *event]{downstreamCallback: tc.downstream}
			collector := newReportCollector()

			mw.recoverCallback(context.Background(), &event{}, collector.Collect)
			tc.assertion(t, collector.Errors())
		})
	}
}

func TestPanic_Process(t *testing.T) {
	type customPanic struct {
		message string
		code    int
	}

	tests := []struct {
		name      string
		action    *panicAction
		assertion func(t *testing.T, out *event, report error)
	}{
		{
			name:   "recovers from panic string",
			action: &panicAction{shouldPanic: true, panicValue: "action panic!"},
			assertion: func(t *testing.T, out *event, report error) {
				assert.Nil(t, out)
				assert.ErrorIs(t, report, ErrPanicRecovered)
				assert.Contains(t, report.Error(), "action panic!")
			},
		},
		{
			name:   "recovers from panic custom type",
			action: &panicAction{shouldPanic: true, panicValue: customPanic{message: "boom", code: 500}},
			assertion: func(t *testing.T, out *event, report error) {
				assert.Nil(t, out)
				assert.ErrorIs(t, report, ErrPanicRecovered)
				assert.Contains(t, report.Error(), "boom")
			},
		},
		{
			name: "passes through",
			action: &panicAction{
				shouldPanic:  false,
				returnEvent:  &event{Value: "result"},
				returnReport: nil,
			},
			assertion: func(t *testing.T, out *event, report error) {
				require.NotNil(t, out)
				assert.Equal(t, "result", out.Value)
				assert.NoError(t, report)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mw := &Panic[*event, *event]{downstreamAction: tc.action}
			out, report := mw.Process(context.Background(), &event{}, boolexpr.NewCachedMap(nil))
			tc.assertion(t, out, report)
		})
	}
}

func TestPanic_Send(t *testing.T) {
	tests := []struct {
		name      string
		dest      firehose.Destination[*event]
		assertion func(t *testing.T, report error)
	}{
		{
			name: "recovers from panic string",
			dest: &panicDestination[*event]{panicValue: "destination panic!"},
			assertion: func(t *testing.T, report error) {
				assert.ErrorIs(t, report, ErrPanicRecovered)
				assert.Contains(t, report.Error(), "destination panic!")
			},
		},
		{
			name: "recovers from panic error",
			dest: &panicDestination[*event]{panicValue: errors.New("boom")},
			assertion: func(t *testing.T, report error) {
				assert.ErrorIs(t, report, ErrPanicRecovered)
				assert.Contains(t, report.Error(), "boom")
			},
		},
		{
			name: "passes through",
			dest: &simpleDestination[*event]{returnReport: nil},
			assertion: func(t *testing.T, report error) {
				assert.NoError(t, report)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mw := &Panic[*event, *event]{downstreamDest: tc.dest}
			report := mw.Send(context.Background(), &event{})
			tc.assertion(t, report)
		})
	}
}

type panicAction struct {
	panicValue   any
	shouldPanic  bool
	returnEvent  *event
	returnReport error
}

func (p *panicAction) Process(
	_ context.Context,
	_ *event,
	_ boolexpr.Symbols,
) (*event, error) {
	if p.shouldPanic {
		panic(p.panicValue)
	}

	return p.returnEvent, p.returnReport
}

type panicDestination[T any] struct{ panicValue any }

func (d *panicDestination[T]) Send(_ context.Context, _ T) error {
	panic(d.panicValue)
}

type simpleDestination[T any] struct{ returnReport error }

func (d *simpleDestination[T]) Send(_ context.Context, _ T) error {
	return d.returnReport
}
