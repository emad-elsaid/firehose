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
	t.Run("stores downstream callback and returns panic-wrapped callback", func(t *testing.T) {
		mw := &Panic[*event, *event]{}
		mockCallback := func(ctx context.Context, event *event, reports chan<- firehose.Report) {}
		in := &event{}
		rule := &firehose.Rule[*event, *event]{}

		wrappedCallback, err := mw.WrapCallback(context.Background(), rule, mockCallback, in)

		require.NoError(t, err)
		require.NotNil(t, wrappedCallback)
		require.NotNil(t, mw.downstreamCallback)
	})
}

func TestPanic_WrapAction(t *testing.T) {
	t.Run("stores downstream action and returns panic middleware", func(t *testing.T) {
		mw := &Panic[*event, *event]{}
		mockAction := new(action[*event, *event])
		in := &event{}
		rule := &firehose.Rule[*event, *event]{}

		wrappedAction, err := mw.WrapAction(context.Background(), rule, mockAction, in)

		require.NoError(t, err)
		require.NotNil(t, wrappedAction)
		require.IsType(t, (*Panic[*event, *event])(nil), wrappedAction)
		panicMw := wrappedAction.(*Panic[*event, *event])
		require.Equal(t, mockAction, panicMw.downstreamAction)
	})
}

func TestPanic_WrapDestination(t *testing.T) {
	t.Run("stores downstream destination and returns panic middleware", func(t *testing.T) {
		mw := &Panic[*event, *event]{}
		mockDest := &simpleDestination[*event]{}
		out := &event{}
		rule := &firehose.Rule[*event, *event]{}

		wrappedDest, err := mw.WrapDestination(context.Background(), rule, mockDest, out)

		require.NoError(t, err)
		require.NotNil(t, wrappedDest)
		require.IsType(t, (*Panic[*event, *event])(nil), wrappedDest)
		panicMw := wrappedDest.(*Panic[*event, *event])
		require.Equal(t, mockDest, panicMw.downstreamDest)
	})
}

func TestPanic_RecoverCallback(t *testing.T) {
	t.Run("recovers from callback panic and sends abort report", func(t *testing.T) {
		panicValue := "callback panic!"
		mw := &Panic[*event, *event]{
			downstreamCallback: func(ctx context.Context, event *event, reports chan<- firehose.Report) {
				panic(panicValue)
			},
		}

		reports := make(chan firehose.Report, 1)
		mw.recoverCallback(context.Background(), &event{}, reports)
		close(reports)

		report := <-reports
		assert.Equal(t, StatusPanicRecovered, report.Status)
		assert.True(t, report.Abort)
		assert.ErrorIs(t, report.Err, ErrPanicRecovered)
		assert.Contains(t, report.Err.Error(), panicValue)
	})

	t.Run("passes through normal callback execution", func(t *testing.T) {
		expectedReport := firehose.Report{Status: firehose.StatusSuccess}
		mw := &Panic[*event, *event]{
			downstreamCallback: func(ctx context.Context, event *event, reports chan<- firehose.Report) {
				reports <- expectedReport
			},
		}

		reports := make(chan firehose.Report, 1)
		mw.recoverCallback(context.Background(), &event{}, reports)
		close(reports)

		report := <-reports
		assert.Equal(t, expectedReport.Status, report.Status)
	})
}

func TestPanic_Process(t *testing.T) {
	t.Run("recovers from action panic and returns abort report", func(t *testing.T) {
		panicValue := "action panic!"
		mw := &Panic[*event, *event]{
			downstreamAction: &panicAction{
				shouldPanic: true,
				panicValue:  panicValue,
			},
		}

		out, report := mw.Process(context.Background(), &event{}, boolexpr.NewCachedMap(nil))

		assert.Nil(t, out)
		assert.Equal(t, StatusPanicRecovered, report.Status)
		assert.True(t, report.Abort)
		assert.ErrorIs(t, report.Err, ErrPanicRecovered)
		assert.Contains(t, report.Err.Error(), panicValue)
	})

	t.Run("passes through successful action execution", func(t *testing.T) {
		expectedOut := &event{Value: "result"}
		expectedReport := firehose.Report{Status: firehose.StatusSuccess}
		mw := &Panic[*event, *event]{
			downstreamAction: &panicAction{
				shouldPanic:  false,
				returnEvent:  expectedOut,
				returnReport: expectedReport,
			},
		}

		out, report := mw.Process(context.Background(), &event{}, boolexpr.NewCachedMap(nil))

		assert.Equal(t, expectedOut, out)
		assert.Equal(t, expectedReport.Status, report.Status)
	})

	t.Run("recovers from custom panic type", func(t *testing.T) {
		customPanic := &customPanicType{message: "custom error", code: 500}
		mw := &Panic[*event, *event]{
			downstreamAction: &panicAction{
				shouldPanic: true,
				panicValue:  customPanic,
			},
		}

		out, report := mw.Process(context.Background(), &event{}, boolexpr.NewCachedMap(nil))

		assert.Nil(t, out)
		assert.Equal(t, StatusPanicRecovered, report.Status)
		assert.True(t, report.Abort)
		assert.ErrorIs(t, report.Err, ErrPanicRecovered)
	})
}

func TestPanic_Send(t *testing.T) {
	t.Run("recovers from destination panic and returns abort report", func(t *testing.T) {
		panicValue := "destination panic!"
		mw := &Panic[*event, *event]{
			downstreamDest: &panicDestination[*event]{
				panicValue: panicValue,
			},
		}

		report := mw.Send(context.Background(), &event{})

		assert.Equal(t, StatusPanicRecovered, report.Status)
		assert.True(t, report.Abort)
		assert.ErrorIs(t, report.Err, ErrPanicRecovered)
		assert.Contains(t, report.Err.Error(), panicValue)
	})

	t.Run("passes through successful destination execution", func(t *testing.T) {
		expectedReport := firehose.Report{Status: firehose.StatusSuccess}
		mw := &Panic[*event, *event]{
			downstreamDest: &simpleDestination[*event]{
				returnReport: expectedReport,
			},
		}

		report := mw.Send(context.Background(), &event{})

		assert.Equal(t, expectedReport.Status, report.Status)
	})

	t.Run("recovers from error panic in destination", func(t *testing.T) {
		panicErr := errors.New("error panic")
		mw := &Panic[*event, *event]{
			downstreamDest: &panicDestination[*event]{
				panicValue: panicErr,
			},
		}

		report := mw.Send(context.Background(), &event{})

		assert.Equal(t, StatusPanicRecovered, report.Status)
		assert.True(t, report.Abort)
		assert.ErrorIs(t, report.Err, ErrPanicRecovered)
		assert.Contains(t, report.Err.Error(), panicErr.Error())
	})
}

// Test helpers

type customPanicType struct {
	message string
	code    int
}

type panicAction struct {
	panicValue   any
	shouldPanic  bool
	returnEvent  *event
	returnReport firehose.Report
}

func (p *panicAction) Process(ctx context.Context, event *event, syms boolexpr.Symbols) (*event, firehose.Report) {
	if p.shouldPanic {
		panic(p.panicValue)
	}
	return p.returnEvent, p.returnReport
}

type panicDestination[T any] struct {
	panicValue any
}

func (d *panicDestination[T]) Send(ctx context.Context, event T) firehose.Report {
	panic(d.panicValue)
}

type simpleDestination[T any] struct {
	returnReport firehose.Report
}

func (d *simpleDestination[T]) Send(ctx context.Context, event T) firehose.Report {
	return d.returnReport
}
