package firehose

import (
	"os"
	"testing"

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

		action.On("Process", t.Context(), in).Return(in, nil).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		require.NoError(t, rule.callback(t.Context(), in))
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

		action.On("Process", t.Context(), in).Return(in, os.ErrClosed).Once()

		require.ErrorIs(t, rule.callback(t.Context(), in), os.ErrClosed)
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

		action.On("Process", t.Context(), in).Return(in, nil).Once()
		destination.On("Send", t.Context(), in).Return(os.ErrClosed).Once()

		require.ErrorIs(t, rule.callback(t.Context(), in), os.ErrClosed)
	})

	t.Run("callback chains to next rule with same source", func(t *testing.T) {
		in := new(EventMock)
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

		registry, err := AddRule(t.Context(), nil, rule1, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, in)
		require.NoError(t, err)

		action.On("Process", t.Context(), in).Return(in, nil).Twice()
		destination.On("Send", t.Context(), in).Return(nil).Twice()

		require.NoError(t, rule1.callback(t.Context(), in))
	})

	t.Run("callback chain stops on action error in first rule", func(t *testing.T) {
		in := new(EventMock)
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

		registry, err := AddRule(t.Context(), nil, rule1, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, in)
		require.NoError(t, err)

		action.On("Process", t.Context(), in).Return(in, os.ErrClosed).Once()

		require.Error(t, rule1.callback(t.Context(), in), os.ErrClosed)
	})

	t.Run("callback chain propagates error from second rule", func(t *testing.T) {
		in := new(EventMock)
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

		registry, err := AddRule(t.Context(), nil, rule1, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, in)
		require.NoError(t, err)

		action.On("Process", t.Context(), in).Return(in, nil).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		action.On("Process", t.Context(), in).Return(in, os.ErrClosed).Once()

		require.Error(t, rule1.callback(t.Context(), in), os.ErrClosed)
	})

	t.Run("callback with three rules in chain", func(t *testing.T) {
		in := new(EventMock)
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

		registry, err := AddRule(t.Context(), nil, rule1, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule3, in)
		require.NoError(t, err)

		action.On("Process", t.Context(), in).Return(in, nil).Times(3)
		destination.On("Send", t.Context(), in).Return(nil).Times(3)

		require.NoError(t, rule1.callback(t.Context(), in))
	})

	t.Run("callback with incompatible next rule type", func(t *testing.T) {
		in := &EventMock{}
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

		action.On("Process", t.Context(), in).Return(in, nil).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		// Create a mock sourceRegistry with incompatible type
		rule.nextSameSource = &mockIncompatibleSourceRegistry{}

		require.Error(t, rule.callback(t.Context(), in))
	})
}

func TestRuleWithCondition(t *testing.T) {
	t.Parallel()

	t.Run("callback with condition that evaluates to true", func(t *testing.T) {
		source := &MockSource{}
		defer source.AssertExpectations(t)
		action := &MockAction{}
		defer action.AssertExpectations(t)
		destination := &MockDestination{}
		defer destination.AssertExpectations(t)

		rule := &MockRule{
			When: source,
			If:   `attr1 = "value1"`,
			Then: action,
			To:   destination,
		}

		in := &EventMock{}
		defer in.AssertExpectations(t)

		in.On("Attributes", t.Context()).Return(map[string]any{
			"attr1": "value1",
		}).Twice()

		_, err := AddRule(t.Context(), nil, rule, in)
		require.NoError(t, err)

		action.On("Process", t.Context(), in).Return(in, nil).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		require.NoError(t, rule.callback(t.Context(), in))
	})

	t.Run("callback with condition that evaluates to false", func(t *testing.T) {
		source := &MockSource{}
		defer source.AssertExpectations(t)
		action := &MockAction{}
		defer action.AssertExpectations(t)
		destination := &MockDestination{}
		defer destination.AssertExpectations(t)

		rule := &MockRule{
			When: source,
			If:   `attr1 != "value1"`,
			Then: action,
			To:   destination,
		}

		in := &EventMock{}
		defer in.AssertExpectations(t)

		in.On("Attributes", t.Context()).Return(map[string]any{
			"attr1": "value1",
		}).Twice()

		_, err := AddRule(t.Context(), nil, rule, in)
		require.NoError(t, err)

		require.NoError(t, rule.callback(t.Context(), in))
	})

	t.Run("callback with condition that error while eval", func(t *testing.T) {
		source := &MockSource{}
		defer source.AssertExpectations(t)
		action := &MockAction{}
		defer action.AssertExpectations(t)
		destination := &MockDestination{}
		defer destination.AssertExpectations(t)

		rule := &MockRule{
			When: source,
			If:   `attr1 != "value1"`,
			Then: action,
			To:   destination,
		}

		in := &EventMock{}
		defer in.AssertExpectations(t)

		in.On("Attributes", t.Context()).Return(map[string]any{
			"attr1": func() (string, error) {
				return "", os.ErrClosed
			},
		}).Twice()

		_, err := AddRule(t.Context(), nil, rule, in)
		require.NoError(t, err)

		require.ErrorIs(t, rule.callback(t.Context(), in), os.ErrClosed)
	})

	t.Run("callback with faulty condition", func(t *testing.T) {
		source := &MockSource{}
		defer source.AssertExpectations(t)
		action := &MockAction{}
		defer action.AssertExpectations(t)
		destination := &MockDestination{}
		defer destination.AssertExpectations(t)

		rule := &MockRule{
			When: source,
			If:   `attr1 <> "value1"`,
			Then: action,
			To:   destination,
		}

		registry, err := AddRule(t.Context(), nil, rule, new(EventMock))
		require.Error(t, err)
		require.Nil(t, registry)
	})
}
