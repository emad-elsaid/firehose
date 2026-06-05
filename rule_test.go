package firehose

import (
	"os"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestRuleCallback(t *testing.T) {
	t.Parallel()

	t.Run("successful callback with action and destination", func(t *testing.T) {
		source := &MockSource{}
		action := &MockAction{}
		defer action.AssertExpectations(t)
		destination := &MockDestination{}
		defer destination.AssertExpectations(t)

		rule := &MockRule{
			When: source,
			Then: action,
			To:   destination,
		}

		in := new(EventMock)
		defer in.AssertExpectations(t)

		in.On("Attributes", t.Context()).Return(map[string]any{}).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		reports := chanToSlice(rule.callback(t.Context(), in))
		require.NotNil(t, reports)
		require.Len(t, reports, 1)
		for _, report := range reports {
			require.NoError(t, report.Err)
		}
	})

	t.Run("callback with action error", func(t *testing.T) {
		source := &MockSource{}
		action := &MockAction{}
		defer action.AssertExpectations(t)
		destination := &MockDestination{}
		defer destination.AssertExpectations(t)
		rule := &MockRule{
			When: source,
			Then: action,
			To:   destination,
		}
		in := new(EventMock)
		defer in.AssertExpectations(t)

		in.On("Attributes", t.Context()).Return(map[string]any{}).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusActionError, os.ErrClosed)).Once()

		reports := chanToSlice(rule.callback(t.Context(), in))
		require.NotNil(t, reports)
		require.Len(t, reports, 1)

		for _, report := range reports {
			require.ErrorIs(t, report.Err, os.ErrClosed)
		}
	})

	t.Run("callback with destination error", func(t *testing.T) {
		source := &MockSource{}
		action := &MockAction{}
		defer action.AssertExpectations(t)
		destination := &MockDestination{}
		defer destination.AssertExpectations(t)
		rule := &MockRule{
			When: source,
			Then: action,
			To:   destination,
		}
		in := new(EventMock)
		defer in.AssertExpectations(t)

		in.On("Attributes", t.Context()).Return(map[string]any{}).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Once()
		destination.On("Send", t.Context(), in).Return(os.ErrClosed).Once()

		reports := chanToSlice(rule.callback(t.Context(), in))
		require.NotNil(t, reports)
		require.Len(t, reports, 1)

		for _, report := range reports {
			require.ErrorIs(t, report.Err, os.ErrClosed)
		}
	})

	t.Run("callback chains to next rule with same source", func(t *testing.T) {
		in := new(EventMock)
		defer in.AssertExpectations(t)
		source := &MockSource{}
		defer source.AssertExpectations(t)
		action := &MockAction{}
		defer action.AssertExpectations(t)
		destination := &MockDestination{}
		defer destination.AssertExpectations(t)

		rule1 := &MockRule{
			When: source,
			Then: action,
			To:   destination,
		}

		rule2 := &MockRule{
			When: source,
			Then: action,
			To:   destination,
		}

		registry, err := AddRule(t.Context(), nil, rule1, in, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, in, in)
		require.NoError(t, err)

		in.On("Attributes", t.Context()).Return(map[string]any{}).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Twice()
		destination.On("Send", t.Context(), in).Return(nil).Twice()

		reports := chanToSlice(rule1.callback(t.Context(), in))
		require.NotNil(t, reports)
		require.Len(t, reports, 2)

		for _, report := range reports {
			require.NoError(t, report.Err, in)
		}
	})

	t.Run("callback chain continue on action error in first rule", func(t *testing.T) {
		in := new(EventMock)
		defer in.AssertExpectations(t)
		source := &MockSource{}
		defer source.AssertExpectations(t)
		action := &MockAction{}
		defer action.AssertExpectations(t)
		destination := &MockDestination{}
		defer destination.AssertExpectations(t)

		rule1 := &MockRule{
			When: source,
			Then: action,
			To:   destination,
		}

		rule2 := &MockRule{
			When: source,
			Then: action,
			To:   destination,
		}

		registry, err := AddRule(t.Context(), nil, rule1, in, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, in, in)
		require.NoError(t, err)

		in.On("Attributes", t.Context()).Return(map[string]any{}).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusActionError, os.ErrClosed)).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		reports := chanToSlice(rule1.callback(t.Context(), in))
		require.NotNil(t, reports)
		require.Len(t, reports, 2)
		require.ErrorIs(t, reports[0].Err, os.ErrClosed)
		require.NoError(t, reports[1].Err)
	})

	t.Run("callback chain propagates error from second rule", func(t *testing.T) {
		in := new(EventMock)
		defer in.AssertExpectations(t)
		source := &MockSource{}
		defer source.AssertExpectations(t)
		action := &MockAction{}
		defer action.AssertExpectations(t)
		destination := &MockDestination{}
		defer destination.AssertExpectations(t)

		rule1 := &MockRule{
			When: source,
			Then: action,
			To:   destination,
		}

		rule2 := &MockRule{
			When: source,
			Then: action,
			To:   destination,
		}

		registry, err := AddRule(t.Context(), nil, rule1, in, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, in, in)
		require.NoError(t, err)

		in.On("Attributes", t.Context()).Return(map[string]any{}).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusActionError, os.ErrClosed)).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		reports := chanToSlice(rule1.callback(t.Context(), in))
		require.NotNil(t, reports)
		require.Len(t, reports, 2)
		require.NoError(t, reports[0].Err)
		require.ErrorIs(t, reports[1].Err, os.ErrClosed)
	})

	t.Run("callback with three rules in chain", func(t *testing.T) {
		in := new(EventMock)
		defer in.AssertExpectations(t)
		source := &MockSource{}
		defer source.AssertExpectations(t)
		action := &MockAction{}
		defer action.AssertExpectations(t)
		destination := &MockDestination{}
		defer destination.AssertExpectations(t)

		rule1 := &MockRule{
			When: source,
			Then: action,
			To:   destination,
		}

		rule2 := &MockRule{
			When: source,
			Then: action,
			To:   destination,
		}

		rule3 := &MockRule{
			When: source,
			Then: action,
			To:   destination,
		}

		registry, err := AddRule(t.Context(), nil, rule1, in, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, in, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule3, in, in)
		require.NoError(t, err)

		in.On("Attributes", t.Context()).Return(map[string]any{}).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Times(3)
		destination.On("Send", t.Context(), in).Return(nil).Times(3)

		reports := chanToSlice(rule1.callback(t.Context(), in))
		require.NotNil(t, reports)
		require.Len(t, reports, 3)

		for _, report := range reports {
			require.NoError(t, report.Err)
		}
	})

	t.Run("callback with incompatible next rule type", func(t *testing.T) {
		in := &EventMock{}
		defer in.AssertExpectations(t)
		source := &MockSource{}
		defer source.AssertExpectations(t)
		action := &MockAction{}
		defer action.AssertExpectations(t)
		destination := &MockDestination{}
		defer destination.AssertExpectations(t)

		rule := &MockRule{
			When: source,
			Then: action,
			To:   destination,
		}

		in.On("Attributes", t.Context()).Return(map[string]any{}).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		// Create a mock sourceRegistry with incompatible type
		rule.nextSameSource = &mockIncompatibleSourceRegistry{}

		reports := chanToSlice(rule.callback(t.Context(), in))
		require.NotNil(t, reports)
		require.Len(t, reports, 2)

		require.NoError(t, reports[0].Err)
		require.ErrorIs(t, reports[1].Err, ErrIncompatibleSource)
	})
}

func chanToSlice[T any](ch <-chan T) []T {
	var result []T
	for v := range ch {
		result = append(result, v)
	}

	return result
}
