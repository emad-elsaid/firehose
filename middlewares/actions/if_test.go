package actions

import (
	"context"
	"os"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type event struct {
	mock.Mock
}

func (e *event) Attributes(ctx context.Context) (map[string]any, error) {
	args := e.Called(ctx)
	r1, ok := args.Get(0).(map[string]any)
	if !ok {
		return nil, nil
	}

	return r1, args.Error(1)
}

type action[I, O firehose.Event] struct {
	mock.Mock
}

func (a *action[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, firehose.Report) {
	args := a.Called(ctx, event, syms)
	r1, ok := args.Get(0).(O)
	if !ok {
		var zero O
		r1 = zero
	}
	r2, ok := args.Get(1).(firehose.Report)
	if !ok {
		r2 = firehose.Report{}
	}

	return r1, r2
}

func TestIf_Wrap(t *testing.T) {
	t.Run("returns same action when condition is empty", func(t *testing.T) {
		action := new(action[*event, *event])
		mw := new(If[*event, *event])
		in := new(event)
		rule := &firehose.Rule[*event, *event]{
			Then: action,
		}

		outAction, err := mw.Wrap(t.Context(), rule, action, in)

		require.NoError(t, err)
		require.Equal(t, action, outAction)
	})

	t.Run("return error when condition is invalid syntax", func(t *testing.T) {
		action := new(action[*event, *event])
		mw := new(If[*event, *event])
		in := new(event)
		rule := &firehose.Rule[*event, *event]{
			If:   `a =`,
			Then: action,
		}

		outAction, err := mw.Wrap(t.Context(), rule, action, in)

		require.Error(t, err)
		require.Nil(t, outAction)
	})

	t.Run("returns If action when condition is not empty", func(t *testing.T) {
		action := new(action[*event, *event])
		mw := new(If[*event, *event])

		in := new(event)
		defer in.AssertExpectations(t)

		rule := &firehose.Rule[*event, *event]{
			If:   `a = 1`,
			Then: action,
		}

		in.On("Attributes", t.Context()).Return(map[string]any{"a": 1}, nil).Once()

		outAction, err := mw.Wrap(t.Context(), rule, action, in)

		require.NoError(t, err)
		require.IsType(t, (*If[*event, *event])(nil), outAction)
	})
}

func TestIf_Process(t *testing.T) {
	t.Run("calls the upstream action when condition is true", func(t *testing.T) {
		action := new(action[*event, *event])
		defer action.AssertExpectations(t)

		mw := new(If[*event, *event])

		in := new(event)
		defer in.AssertExpectations(t)

		rule := &firehose.Rule[*event, *event]{
			If:   `a = 1`,
			Then: action,
		}

		attrs := map[string]any{"a": 1}
		in.On("Attributes", t.Context()).Return(attrs, nil).Once()

		outAction, err := mw.Wrap(t.Context(), rule, action, in)

		require.NoError(t, err)
		require.IsType(t, (*If[*event, *event])(nil), outAction)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, firehose.Report{}).Once()
		out, report := outAction.Process(t.Context(), in, boolexpr.NewSymbolsCached(attrs))

		require.Equal(t, in, out)
		require.Equal(t, firehose.Report{}, report)
	})
	t.Run("skips the upstream action when condition is false", func(t *testing.T) {
		action := new(action[*event, *event])
		defer action.AssertExpectations(t)

		mw := new(If[*event, *event])

		in := new(event)
		defer in.AssertExpectations(t)

		rule := &firehose.Rule[*event, *event]{
			If:   `a = 1`,
			Then: action,
		}

		attrs := map[string]any{"a": 0}
		in.On("Attributes", t.Context()).Return(attrs, nil).Once()

		outAction, err := mw.Wrap(t.Context(), rule, action, in)

		require.NoError(t, err)
		require.IsType(t, (*If[*event, *event])(nil), outAction)

		out, report := outAction.Process(t.Context(), in, boolexpr.NewSymbolsCached(attrs))

		require.Nil(t, out)
		require.Equal(t, firehose.StatusNoMatch, report.Status)
		require.True(t, report.Abort)
	})

	t.Run("return error report when condition returns error", func(t *testing.T) {
		action := new(action[*event, *event])
		defer action.AssertExpectations(t)

		mw := new(If[*event, *event])

		in := new(event)
		defer in.AssertExpectations(t)

		rule := &firehose.Rule[*event, *event]{
			If:   `a = 1`,
			Then: action,
		}

		attrs := map[string]any{"a": func() (any, error) { return nil, os.ErrClosed }}
		in.On("Attributes", t.Context()).Return(attrs, nil).Once()

		outAction, err := mw.Wrap(t.Context(), rule, action, in)

		require.NoError(t, err)
		require.IsType(t, (*If[*event, *event])(nil), outAction)

		out, report := outAction.Process(t.Context(), in, boolexpr.NewSymbolsCached(attrs))

		require.Nil(t, out)
		require.Equal(t, firehose.StatusConditionError, report.Status)
		require.True(t, report.Abort)
	})

	t.Run("evaluates condition dynamically when If field changes at runtime", func(t *testing.T) {
		action := new(action[*event, *event])
		defer action.AssertExpectations(t)

		mw := new(If[*event, *event])

		in := new(event)
		defer in.AssertExpectations(t)

		rule := &firehose.Rule[*event, *event]{
			If:   `a = 1`,
			Then: action,
		}

		attrs := map[string]any{"a": 1, "b": 2}
		in.On("Attributes", t.Context()).Return(attrs, nil).Once()

		outAction, err := mw.Wrap(t.Context(), rule, action, in)

		require.NoError(t, err)
		require.IsType(t, (*If[*event, *event])(nil), outAction)

		// First call with a = 1, should process
		action.On("Process", t.Context(), in, mock.Anything).Return(in, firehose.Report{}).Once()
		out, report := outAction.Process(t.Context(), in, boolexpr.NewSymbolsCached(attrs))

		require.Equal(t, in, out)
		require.Equal(t, firehose.Report{}, report)

		// Change the condition at runtime in the original rule
		rule.If = `b = 2`

		// Second call with new condition b = 2, should process
		action.On("Process", t.Context(), in, mock.Anything).Return(in, firehose.Report{}).Once()
		out, report = outAction.Process(t.Context(), in, boolexpr.NewSymbolsCached(attrs))

		require.Equal(t, in, out)
		require.Equal(t, firehose.Report{}, report)

		// Change to failing condition in the original rule
		rule.If = `a = 2`

		// Third call with failing condition, should not process
		out, report = outAction.Process(t.Context(), in, boolexpr.NewSymbolsCached(attrs))

		require.Nil(t, out)
		require.Equal(t, firehose.StatusNoMatch, report.Status)
		require.True(t, report.Abort)
	})

	t.Run("caches parsed condition and only reparses when condition changes", func(t *testing.T) {
		action := new(action[*event, *event])
		defer action.AssertExpectations(t)

		mw := new(If[*event, *event])

		in := new(event)
		defer in.AssertExpectations(t)

		rule := &firehose.Rule[*event, *event]{
			If:   `a = 1`,
			Then: action,
		}

		attrs := map[string]any{"a": 1}
		in.On("Attributes", t.Context()).Return(attrs, nil).Once()

		outAction, err := mw.Wrap(t.Context(), rule, action, in)

		require.NoError(t, err)
		ifMiddleware := outAction.(*If[*event, *event])

		// Verify condition was parsed during Wrap
		require.Equal(t, `a = 1`, ifMiddleware.lastCondition)
		require.NotNil(t, ifMiddleware.parsedCondition)
		initialParsed := ifMiddleware.parsedCondition

		// First process - should use cached parsed condition
		action.On("Process", t.Context(), in, mock.Anything).Return(in, firehose.Report{}).Once()
		_, _ = outAction.Process(t.Context(), in, boolexpr.NewSymbolsCached(attrs))

		// Verify same parsed condition (not re-parsed)
		require.Equal(t, initialParsed, ifMiddleware.parsedCondition)
		require.Equal(t, `a = 1`, ifMiddleware.lastCondition)

		// Second process with same condition - should still use cached
		action.On("Process", t.Context(), in, mock.Anything).Return(in, firehose.Report{}).Once()
		_, _ = outAction.Process(t.Context(), in, boolexpr.NewSymbolsCached(attrs))

		// Still the same parsed condition
		require.Equal(t, initialParsed, ifMiddleware.parsedCondition)
		require.Equal(t, `a = 1`, ifMiddleware.lastCondition)

		// Now change the condition
		rule.If = `a = 2`

		// Process with changed condition - should re-parse
		_, _ = outAction.Process(t.Context(), in, boolexpr.NewSymbolsCached(attrs))

		// Verify condition was re-parsed
		require.NotEqual(t, initialParsed, ifMiddleware.parsedCondition)
		require.Equal(t, `a = 2`, ifMiddleware.lastCondition)
	})
}

