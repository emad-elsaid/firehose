package firehose

import (
	"os"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestConditionalMiddleware(t *testing.T) {
	t.Run("rule with condition that uses non-existing attribute is invalid", func(t *testing.T) {
		rule := &MockRule{
			When: newSourceMock[*EventMock]("source1"),
			If:   "NonExistingAttr == 'value'",
			Then: &MockAction{},
			To:   &MockDestination{},
		}

		middleware := ConditionalMiddleware[*EventMock, *EventMock]{}

		in := new(EventMock)
		in.On("Attributes", t.Context()).Return(nil).Once()

		_, err := middleware.Wrap(t.Context(), *rule, rule.Then, in)
		require.Error(t, err)
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

		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		reports := chanToSlice(rule.callback(t.Context(), in))
		require.NotNil(t, reports)
		require.Len(t, reports, 1)

		for _, report := range reports {
			require.NoError(t, report.Err)
		}
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

		reports := chanToSlice(rule.callback(t.Context(), in))
		require.NotNil(t, reports)
		require.Len(t, reports, 1)

		for _, report := range reports {
			require.NoError(t, report.Err)
		}
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

		reports := chanToSlice(rule.callback(t.Context(), in))
		require.NotNil(t, reports)
		require.Len(t, reports, 1)

		for _, report := range reports {
			require.ErrorIs(t, report.Err, os.ErrClosed)
			require.Equal(t, report.Status, StatusConditionError)
		}
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
