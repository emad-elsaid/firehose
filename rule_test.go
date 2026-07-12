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
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule := &MockRule{
			From:   source,
			Select: action,
			Into:   destination,
		}

		in := NewEventMock(nil)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		collector := newReportCollector()
		rule.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.NotNil(t, reports)
		require.Len(t, reports, 0) // Both action and dest return nil - no errors collected
	})

	t.Run("callback with action error", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)
		rule := &MockRule{
			From:   source,
			Select: action,
			Into:   destination,
		}
		in := NewEventMock(nil)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, os.ErrClosed).Once()

		collector := newReportCollector()
		rule.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.NotNil(t, reports)
		require.Len(t, reports, 1)

		for _, report := range reports {
			require.ErrorIs(t, report, os.ErrClosed)
		}
	})

	t.Run("callback with destination error", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)
		rule := &MockRule{
			From:   source,
			Select: action,
			Into:   destination,
		}
		in := NewEventMock(nil)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		destination.On("Send", t.Context(), in).Return(os.ErrClosed).Once()

		collector := newReportCollector()
		rule.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.NotNil(t, reports)
		require.Len(t, reports, 1)

		for _, report := range reports {
			require.ErrorIs(t, report, os.ErrClosed)
		}
	})

	t.Run("callback chains to next rule with same source", func(t *testing.T) {
		in := NewEventMock(nil)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule1 := &MockRule{
			ID:     "rule1",
			From:   source,
			Select: action,
			Into:   destination,
		}

		rule2 := &MockRule{
			ID:     "rule2",
			From:   source,
			Select: action,
			Into:   destination,
		}

		registry, err := Add(t.Context(), nil, rule1)
		require.NoError(t, err)

		registry, err = Add(t.Context(), registry, rule2)
		require.NoError(t, err)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Twice()
		destination.On("Send", t.Context(), in).Return(nil).Twice()

		collector := newReportCollector()
		rule1.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.NotNil(t, reports)
		require.Len(t, reports, 0) // Success case - no errors reported
	})

	t.Run("callback chain continue on action error in first rule", func(t *testing.T) {
		in := NewEventMock(nil)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule1 := &MockRule{
			ID:     "rule1",
			From:   source,
			Select: action,
			Into:   destination,
		}

		rule2 := &MockRule{
			ID:     "rule2",
			From:   source,
			Select: action,
			Into:   destination,
		}

		registry, err := Add(t.Context(), nil, rule1)
		require.NoError(t, err)

		registry, err = Add(t.Context(), registry, rule2)
		require.NoError(t, err)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, os.ErrClosed).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		collector := newReportCollector()
		rule1.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.NotNil(t, reports)
		require.Len(t, reports, 1) // Only error from rule1, rule2 succeeds (no report)
		require.ErrorIs(t, reports[0], os.ErrClosed)
	})

	t.Run("callback chain propagates error from second rule", func(t *testing.T) {
		in := NewEventMock(nil)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule1 := &MockRule{
			ID:     "rule1",
			From:   source,
			Select: action,
			Into:   destination,
		}

		rule2 := &MockRule{
			ID:     "rule2",
			From:   source,
			Select: action,
			Into:   destination,
		}

		registry, err := Add(t.Context(), nil, rule1)
		require.NoError(t, err)

		registry, err = Add(t.Context(), registry, rule2)
		require.NoError(t, err)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, os.ErrClosed).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		collector := newReportCollector()
		rule1.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.NotNil(t, reports)
		require.Len(t, reports, 1) // Only error from rule2, rule1 succeeds (no report)
		require.ErrorIs(t, reports[0], os.ErrClosed)
	})

	t.Run("callback with three rules in chain", func(t *testing.T) {
		in := NewEventMock(nil)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule1 := &MockRule{
			ID:     "rule1",
			From:   source,
			Select: action,
			Into:   destination,
		}

		rule2 := &MockRule{
			ID:     "rule2",
			From:   source,
			Select: action,
			Into:   destination,
		}

		rule3 := &MockRule{
			ID:     "rule3",
			From:   source,
			Select: action,
			Into:   destination,
		}

		registry, err := Add(t.Context(), nil, rule1)
		require.NoError(t, err)

		registry, err = Add(t.Context(), registry, rule2)
		require.NoError(t, err)

		registry, err = Add(t.Context(), registry, rule3)
		require.NoError(t, err)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Times(3)
		destination.On("Send", t.Context(), in).Return(nil).Times(3)

		collector := newReportCollector()
		rule1.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.NotNil(t, reports)
		require.Len(t, reports, 0) // All 3 rules succeed - no errors reported
	})

	t.Run("callback with incompatible next rule type", func(t *testing.T) {
		in := NewEventMock(nil)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule := &MockRule{
			From:   source,
			Select: action,
			Into:   destination,
		}

		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		// Create a Rule with a different type (string instead of *EventMock)
		// This will cause a panic when type-asserting to Runnable[*EventMock]
		incompatibleRule := &Rule[string, string]{}
		incompatibleSourceRegistry := newMocksourceRegistry(t)
		incompatibleSourceRegistry.On("getRegistry").Return(incompatibleRule).Once()
		rule.nextSameSource = incompatibleSourceRegistry

		collector := newReportCollector()
		require.Panics(t, func() { rule.callback(t.Context(), in, collector.Collect) })
		reports := collector.Errors()
		require.NotNil(t, reports)
		require.Len(t, reports, 0) // Panic happens during callback, success case produces no reports
	})

	t.Run("callback with nil reporter function", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule := &MockRule{
			From:   source,
			Select: action,
			Into:   destination,
		}

		in := NewEventMock(nil)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		require.NotPanics(t, func() { rule.callback(t.Context(), in, nil) })
	})
}

func TestRuleActionOverride(t *testing.T) {
	source := NewMockSource[*EventMock](t)
	oldAction := NewMockAction[*EventMock, *EventMock](t)
	newAction := NewMockAction[*EventMock, *EventMock](t)
	destination := NewMockDestination[*EventMock](t)
	event := NewEventMock(nil)

	rule := &Rule[*EventMock, *EventMock]{
		ID:     "test-rule",
		From:   source,
		Select: oldAction,
		Into:   destination,
	}

	// Register the rule
	_, err := Add(t.Context(), nil, rule)
	require.NoError(t, err)

	// Override the action after registration
	rule.Select = newAction

	newAction.On("Process", t.Context(), event, mock.Anything).
		Return(event, nil).Once()
	destination.On("Send", t.Context(), event).
		Return(nil).Once()

	collector := newReportCollector()
	rule.callback(t.Context(), event, collector.Collect)

	reports := collector.Errors()
	require.NotNil(t, reports)
	require.Len(t, reports, 0) // Success case - no errors reported
}

func TestRuleDestinationOverride(t *testing.T) {
	source := NewMockSource[*EventMock](t)
	action := NewMockAction[*EventMock, *EventMock](t)
	oldDestination := NewMockDestination[*EventMock](t)
	newDestination := NewMockDestination[*EventMock](t)
	event := NewEventMock(nil)

	rule := &Rule[*EventMock, *EventMock]{
		ID:     "test-rule",
		From:   source,
		Select: action,
		Into:   oldDestination,
	}

	// Register the rule
	_, err := Add(t.Context(), nil, rule)
	require.NoError(t, err)

	// Override the destination after registration
	rule.Into = newDestination

	action.On("Process", t.Context(), event, mock.Anything).
		Return(event, nil).Once()
	newDestination.On("Send", t.Context(), event).
		Return(nil).Once()

	collector := newReportCollector()
	rule.callback(t.Context(), event, collector.Collect)

	reports := collector.Errors()
	require.NotNil(t, reports)
	require.Len(t, reports, 0) // Success case - no errors reported
}

func TestRule_Process(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupAction    func() Action[*EventMock, *EventMock]
		event          *EventMock
		expectedError  bool
		validateOutput func(t *testing.T, output *EventMock, report error)
	}{
		{
			name: "successful action processing",
			setupAction: func() Action[*EventMock, *EventMock] {
				action := NewMockAction[*EventMock, *EventMock](t)
				output := NewEventMock(map[string]any{"processed": true})
				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(output, nil).Once()
				return action
			},
			event:         NewEventMock(nil),
			expectedError: false,
			validateOutput: func(t *testing.T, output *EventMock, report error) {
				require.NotNil(t, output)
				require.NoError(t, report)
			},
		},
		{
			name: "action returns error",
			setupAction: func() Action[*EventMock, *EventMock] {
				action := NewMockAction[*EventMock, *EventMock](t)
				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return((*EventMock)(nil), os.ErrClosed).Once()
				return action
			},
			event:         NewEventMock(nil),
			expectedError: true,
			validateOutput: func(t *testing.T, output *EventMock, report error) {
				require.ErrorIs(t, report, os.ErrClosed)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := &Rule[*EventMock, *EventMock]{
				Select: tc.setupAction(),
			}

			output, report := rule.Process(t.Context(), tc.event, nil)
			tc.validateOutput(t, output, report)
		})
	}
}

func TestRule_Send(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		setupDestination func() Destination[*EventMock]
		event            *EventMock
		expectedError    bool
		validateReport   func(t *testing.T, report error)
	}{
		{
			name: "successful send",
			setupDestination: func() Destination[*EventMock] {
				dest := NewMockDestination[*EventMock](t)
				dest.On("Send", mock.Anything, mock.Anything).
					Return(nil).Once()
				return dest
			},
			event:         NewEventMock(nil),
			expectedError: false,
			validateReport: func(t *testing.T, report error) {
				require.NoError(t, report)
			},
		},
		{
			name: "send returns error",
			setupDestination: func() Destination[*EventMock] {
				dest := NewMockDestination[*EventMock](t)
				dest.On("Send", mock.Anything, mock.Anything).
					Return(os.ErrPermission).Once()
				return dest
			},
			event:         NewEventMock(nil),
			expectedError: true,
			validateReport: func(t *testing.T, report error) {
				require.ErrorIs(t, report, os.ErrPermission)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := &Rule[*EventMock, *EventMock]{
				Into: tc.setupDestination(),
			}

			report := rule.Send(t.Context(), tc.event)
			tc.validateReport(t, report)
		})
	}
}

func TestRule_EvaluateCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupCondition func() Condition[*EventMock]
		event          *EventMock
		expectedPass   bool
		expectedError  bool
		validateReport func(t *testing.T, report error)
	}{
		{
			name: "nil condition passes",
			setupCondition: func() Condition[*EventMock] {
				return nil
			},
			event:         NewEventMock(nil),
			expectedPass:  true,
			expectedError: false,
			validateReport: func(t *testing.T, report error) {
				//				require.Equal(t, "", report.Rule)
				require.NoError(t, report)
			},
		},
		{
			name: "passing condition",
			setupCondition: func() Condition[*EventMock] {
				cond := NewMockCondition[*EventMock](t)
				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				return cond
			},
			event:         NewEventMock(nil),
			expectedPass:  true,
			expectedError: false,
			validateReport: func(t *testing.T, report error) {
				//				require.Equal(t, "", report.Rule)
				require.NoError(t, report)
			},
		},
		{
			name: "failing condition returns ErrNoMatch",
			setupCondition: func() Condition[*EventMock] {
				cond := NewMockCondition[*EventMock](t)
				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()
				return cond
			},
			event:         NewEventMock(nil),
			expectedPass:  false,
			expectedError: false,
			validateReport: func(t *testing.T, report error) {
				//				require.Equal(t, "test-rule", report.Rule)
				require.ErrorIs(t, report, ErrInputNoMatch)
			},
		},
		{
			name: "condition evaluation error",
			setupCondition: func() Condition[*EventMock] {
				cond := NewMockCondition[*EventMock](t)
				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, os.ErrInvalid).Once()
				return cond
			},
			event:         NewEventMock(nil),
			expectedPass:  false,
			expectedError: true,
			validateReport: func(t *testing.T, report error) {
				//				require.Equal(t, "test-rule", report.Rule)
				var condErr ConditionError
				require.ErrorAs(t, report, &condErr)
				require.ErrorIs(t, report, os.ErrInvalid)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := &Rule[*EventMock, *EventMock]{
				ID:    "test-rule",
				Where: tc.setupCondition(),
			}

			pass, report := rule.Evaluate(t.Context(), tc.event, nil)

			require.Equal(t, tc.expectedPass, pass)
			tc.validateReport(t, report)
		})
	}
}

func TestRule_NextRunnable(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() *MockRule
		expected bool
	}{
		{
			name: "returns nil when nextSameSource is nil",
			setup: func() *MockRule {
				return &MockRule{ID: "rule1"}
			},
			expected: false,
		},
		{
			name: "returns next runnable when nextSameSource is set",
			setup: func() *MockRule {
				rule1 := &MockRule{ID: "rule1"}
				rule2 := &MockRule{ID: "rule2"}
				rule1.setNextSameSource(rule2)
				return rule1
			},
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := tc.setup()
			nextRunnable := rule.NextRunnable()

			if tc.expected {
				require.NotNil(t, nextRunnable)
			} else {
				require.Nil(t, nextRunnable)
			}
		})
	}
}

func TestRule_Run(t *testing.T) {
	tests := []struct {
		name            string
		setupMocks      func() (*MockRule, *EventMock)
		expectedReports int
		validateReport  func(t *testing.T, report error)
	}{
		{
			name: "successful action and destination",
			setupMocks: func() (*MockRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockRule{
					ID:     "test-rule",
					Select: action,
					Into:   destination,
				}

				action.On("Process", mock.Anything, event, mock.Anything).
					Return(event, nil).Once()
				destination.On("Send", mock.Anything, event).
					Return(nil).Once()

				return rule, event
			},
			expectedReports: 0, // Success case - no errors reported
			validateReport: func(t *testing.T, report error) {
				// No reports expected on success
			},
		},
		{
			name: "action error stops destination call",
			setupMocks: func() (*MockRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockRule{
					ID:     "test-rule",
					Select: action,
					Into:   destination,
				}

				action.On("Process", mock.Anything, event, mock.Anything).
					Return(event, os.ErrClosed).Once()

				return rule, event
			},
			expectedReports: 1,
			validateReport: func(t *testing.T, report error) {
				//				require.Equal(t, "test-rule", report.Rule)
				var actionErr ActionError
				require.ErrorAs(t, report, &actionErr)
				require.ErrorIs(t, report, os.ErrClosed)
			},
		},
		{
			name: "destination error is reported",
			setupMocks: func() (*MockRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockRule{
					ID:     "test-rule",
					Select: action,
					Into:   destination,
				}

				action.On("Process", mock.Anything, event, mock.Anything).
					Return(event, nil).Once()
				destination.On("Send", mock.Anything, event).
					Return(os.ErrPermission).Once()

				return rule, event
			},
			expectedReports: 1,
			validateReport: func(t *testing.T, report error) {
				//				require.Equal(t, "test-rule", report.Rule)
				var destinationErr DestinationError
				require.ErrorAs(t, report, &destinationErr)
				require.ErrorIs(t, report, os.ErrPermission)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule, event := tc.setupMocks()

			collector := newReportCollector()

			// Use nil symbols for this test
			rule.Run(t.Context(), event, nil, collector.Collect)

			reports := collector.Errors()

			require.Len(t, reports, tc.expectedReports)
			if tc.expectedReports > 0 {
				tc.validateReport(t, reports[0])
			}
		})
	}
}

func TestRule_Start(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() (*MockRule, *MockSource[*EventMock])
		expectStart bool
		expectError bool
	}{
		{
			name: "starts when prevSameSource is nil (first in chain)",
			setup: func() (*MockRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule := &MockRule{
					ID:   "test-rule",
					From: source,
				}
				source.On("Start", mock.Anything, mock.Anything).
					Return(t.Context(), nil).Once()
				return rule, source
			},
			expectStart: true,
			expectError: false,
		},
		{
			name: "does not start when prevSameSource is set (not first in chain)",
			setup: func() (*MockRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule1 := &MockRule{ID: "rule1", From: source}
				rule2 := &MockRule{ID: "rule2", From: source}
				rule2.setPrevSameSource(rule1)
				return rule2, source
			},
			expectStart: false,
			expectError: false,
		},
		{
			name: "returns error when source fails to start",
			setup: func() (*MockRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule := &MockRule{
					ID:   "test-rule",
					From: source,
				}
				source.On("Start", mock.Anything, mock.Anything).
					Return(t.Context(), os.ErrClosed).Once()
				return rule, source
			},
			expectStart: true,
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule, _ := tc.setup()

			err := rule.start(t.Context())

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tc.expectStart && !tc.expectError {
				require.NotNil(t, rule.ctx)
			}
		})
	}
}

func TestRule_CombineIf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setupParent   func() Condition[*EventMock]
		setupChild    func() Condition[*EventMock]
		expectNil     bool
		expectedCount int
	}{
		{
			name: "both nil returns nil",
			setupParent: func() Condition[*EventMock] {
				return nil
			},
			setupChild: func() Condition[*EventMock] {
				return nil
			},
			expectNil:     true,
			expectedCount: 0,
		},
		{
			name: "parent nil returns child",
			setupParent: func() Condition[*EventMock] {
				return nil
			},
			setupChild: func() Condition[*EventMock] {
				return NewMockCondition[*EventMock](t)
			},
			expectNil:     false,
			expectedCount: 1,
		},
		{
			name: "child nil returns parent",
			setupParent: func() Condition[*EventMock] {
				return NewMockCondition[*EventMock](t)
			},
			setupChild: func() Condition[*EventMock] {
				return nil
			},
			expectNil:     false,
			expectedCount: 1,
		},
		{
			name: "both non-nil combines into slice",
			setupParent: func() Condition[*EventMock] {
				return NewMockCondition[*EventMock](t)
			},
			setupChild: func() Condition[*EventMock] {
				return NewMockCondition[*EventMock](t)
			},
			expectNil:     false,
			expectedCount: 2,
		},
		{
			name: "flattens nested ifSlice from parent",
			setupParent: func() Condition[*EventMock] {
				cond1 := NewMockCondition[*EventMock](t)
				cond2 := NewMockCondition[*EventMock](t)
				return conditionSlice[*EventMock]{cond1, cond2}
			},
			setupChild: func() Condition[*EventMock] {
				return NewMockCondition[*EventMock](t)
			},
			expectNil:     false,
			expectedCount: 3,
		},
		{
			name: "flattens nested ifSlice from child",
			setupParent: func() Condition[*EventMock] {
				return NewMockCondition[*EventMock](t)
			},
			setupChild: func() Condition[*EventMock] {
				cond1 := NewMockCondition[*EventMock](t)
				cond2 := NewMockCondition[*EventMock](t)
				return conditionSlice[*EventMock]{cond1, cond2}
			},
			expectNil:     false,
			expectedCount: 3,
		},
		{
			name: "flattens nested ifSlice from both",
			setupParent: func() Condition[*EventMock] {
				cond1 := NewMockCondition[*EventMock](t)
				cond2 := NewMockCondition[*EventMock](t)
				return conditionSlice[*EventMock]{cond1, cond2}
			},
			setupChild: func() Condition[*EventMock] {
				cond3 := NewMockCondition[*EventMock](t)
				cond4 := NewMockCondition[*EventMock](t)
				return conditionSlice[*EventMock]{cond3, cond4}
			},
			expectNil:     false,
			expectedCount: 4,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parent := tc.setupParent()
			child := tc.setupChild()

			result := combineConditions(parent, child)

			if tc.expectNil {
				require.Nil(t, result)
				return
			}

			require.NotNil(t, result)

			// Verify count by flattening
			flattened := flattenCondition(result)
			require.Len(t, flattened, tc.expectedCount)
		})
	}
}

func TestRule_CombineHaving(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setupParent   func() Condition[*EventMock]
		setupChild    func() Condition[*EventMock]
		expectNil     bool
		expectedCount int
	}{
		{
			name: "both nil returns nil",
			setupParent: func() Condition[*EventMock] {
				return nil
			},
			setupChild: func() Condition[*EventMock] {
				return nil
			},
			expectNil:     true,
			expectedCount: 0,
		},
		{
			name: "parent nil returns child",
			setupParent: func() Condition[*EventMock] {
				return nil
			},
			setupChild: func() Condition[*EventMock] {
				return NewMockCondition[*EventMock](t)
			},
			expectNil:     false,
			expectedCount: 1,
		},
		{
			name: "child nil returns parent",
			setupParent: func() Condition[*EventMock] {
				return NewMockCondition[*EventMock](t)
			},
			setupChild: func() Condition[*EventMock] {
				return nil
			},
			expectNil:     false,
			expectedCount: 1,
		},
		{
			name: "both non-nil combines into slice",
			setupParent: func() Condition[*EventMock] {
				return NewMockCondition[*EventMock](t)
			},
			setupChild: func() Condition[*EventMock] {
				return NewMockCondition[*EventMock](t)
			},
			expectNil:     false,
			expectedCount: 2,
		},
		{
			name: "flattens nested ifSlice from parent",
			setupParent: func() Condition[*EventMock] {
				cond1 := NewMockCondition[*EventMock](t)
				cond2 := NewMockCondition[*EventMock](t)
				return conditionSlice[*EventMock]{cond1, cond2}
			},
			setupChild: func() Condition[*EventMock] {
				return NewMockCondition[*EventMock](t)
			},
			expectNil:     false,
			expectedCount: 3,
		},
		{
			name: "flattens nested ifSlice from child",
			setupParent: func() Condition[*EventMock] {
				return NewMockCondition[*EventMock](t)
			},
			setupChild: func() Condition[*EventMock] {
				cond1 := NewMockCondition[*EventMock](t)
				cond2 := NewMockCondition[*EventMock](t)
				return conditionSlice[*EventMock]{cond1, cond2}
			},
			expectNil:     false,
			expectedCount: 3,
		},
		{
			name: "flattens nested ifSlice from both",
			setupParent: func() Condition[*EventMock] {
				cond1 := NewMockCondition[*EventMock](t)
				cond2 := NewMockCondition[*EventMock](t)
				return conditionSlice[*EventMock]{cond1, cond2}
			},
			setupChild: func() Condition[*EventMock] {
				cond3 := NewMockCondition[*EventMock](t)
				cond4 := NewMockCondition[*EventMock](t)
				return conditionSlice[*EventMock]{cond3, cond4}
			},
			expectNil:     false,
			expectedCount: 4,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			parent := tc.setupParent()
			child := tc.setupChild()

			result := combineConditions(parent, child)

			if tc.expectNil {
				require.Nil(t, result)
				return
			}

			require.NotNil(t, result)

			// Verify count by flattening
			flattened := flattenCondition(result)
			require.Len(t, flattened, tc.expectedCount)
		})
	}
}

func TestFlattenIf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setupIf       func() Condition[*EventMock]
		expectNil     bool
		expectedCount int
	}{
		{
			name: "nil returns nil",
			setupIf: func() Condition[*EventMock] {
				return nil
			},
			expectNil:     true,
			expectedCount: 0,
		},
		{
			name: "single condition returns slice with one element",
			setupIf: func() Condition[*EventMock] {
				return NewMockCondition[*EventMock](t)
			},
			expectNil:     false,
			expectedCount: 1,
		},
		{
			name: "ifSlice returns all elements",
			setupIf: func() Condition[*EventMock] {
				cond1 := NewMockCondition[*EventMock](t)
				cond2 := NewMockCondition[*EventMock](t)
				cond3 := NewMockCondition[*EventMock](t)
				return conditionSlice[*EventMock]{cond1, cond2, cond3}
			},
			expectNil:     false,
			expectedCount: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ifVal := tc.setupIf()
			result := flattenCondition(ifVal)

			if tc.expectNil {
				require.Nil(t, result)
			} else {
				require.NotNil(t, result)
				require.Len(t, result, tc.expectedCount)
			}
		})
	}
}

func TestIfSlice_Evaluate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setupSlice    func() conditionSlice[*EventMock]
		event         *EventMock
		expectedPass  bool
		expectedError bool
	}{
		{
			name: "empty slice passes",
			setupSlice: func() conditionSlice[*EventMock] {
				return conditionSlice[*EventMock]{}
			},
			event:         NewEventMock(nil),
			expectedPass:  true,
			expectedError: false,
		},
		{
			name: "all conditions pass",
			setupSlice: func() conditionSlice[*EventMock] {
				cond1 := NewMockCondition[*EventMock](t)
				cond2 := NewMockCondition[*EventMock](t)
				cond1.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				cond2.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				return conditionSlice[*EventMock]{cond1, cond2}
			},
			event:         NewEventMock(nil),
			expectedPass:  true,
			expectedError: false,
		},
		{
			name: "first condition fails",
			setupSlice: func() conditionSlice[*EventMock] {
				cond1 := NewMockCondition[*EventMock](t)
				cond2 := NewMockCondition[*EventMock](t)
				cond1.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()
				return conditionSlice[*EventMock]{cond1, cond2}
			},
			event:         NewEventMock(nil),
			expectedPass:  false,
			expectedError: false,
		},
		{
			name: "second condition fails",
			setupSlice: func() conditionSlice[*EventMock] {
				cond1 := NewMockCondition[*EventMock](t)
				cond2 := NewMockCondition[*EventMock](t)
				cond1.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				cond2.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()
				return conditionSlice[*EventMock]{cond1, cond2}
			},
			event:         NewEventMock(nil),
			expectedPass:  false,
			expectedError: false,
		},
		{
			name: "first condition returns error",
			setupSlice: func() conditionSlice[*EventMock] {
				cond1 := NewMockCondition[*EventMock](t)
				cond2 := NewMockCondition[*EventMock](t)
				cond1.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, os.ErrInvalid).Once()
				return conditionSlice[*EventMock]{cond1, cond2}
			},
			event:         NewEventMock(nil),
			expectedPass:  false,
			expectedError: true,
		},
		{
			name: "second condition returns error",
			setupSlice: func() conditionSlice[*EventMock] {
				cond1 := NewMockCondition[*EventMock](t)
				cond2 := NewMockCondition[*EventMock](t)
				cond1.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				cond2.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, os.ErrPermission).Once()
				return conditionSlice[*EventMock]{cond1, cond2}
			},
			event:         NewEventMock(nil),
			expectedPass:  false,
			expectedError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			slice := tc.setupSlice()

			pass, err := slice.Evaluate(t.Context(), tc.event, nil)

			require.Equal(t, tc.expectedPass, pass)
			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRule_Run_WithConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		setupRule       func() *MockRule
		event           *EventMock
		expectedReports int
		validateReport  func(t *testing.T, report error)
	}{
		{
			name: "Condition fails stops execution",
			setupRule: func() *MockRule {
				cond := NewMockCondition[*EventMock](t)
				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()

				return &MockRule{
					ID:    "test-rule",
					Where: cond,
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 1,
			validateReport: func(t *testing.T, report error) {
				//				require.Equal(t, "test-rule", report.Rule)
				require.ErrorIs(t, report, ErrInputNoMatch)
			},
		},
		{
			name: "Condition passes, action executes",
			setupRule: func() *MockRule {
				cond := NewMockCondition[*EventMock](t)
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)

				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				output := NewEventMock(nil)
				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(output, nil).Once()
				destination.On("Send", mock.Anything, output).
					Return(nil).Once()

				return &MockRule{
					ID:     "test-rule",
					Where:  cond,
					Select: action,
					Into:   destination,
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 0, // Success - no errors reported
			validateReport: func(t *testing.T, report error) {
				// No reports expected on success
			},
		},
		{
			name: "Having condition fails stops destination",
			setupRule: func() *MockRule {
				action := NewMockAction[*EventMock, *EventMock](t)
				postCond := NewMockCondition[*EventMock](t)

				output := NewEventMock(nil)
				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(output, nil).Once()
				postCond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()

				return &MockRule{
					ID:     "test-rule",
					Select: action,
					Having: postCond,
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 1,
			validateReport: func(t *testing.T, report error) {
				//				require.Equal(t, "test-rule", report.Rule)
				require.ErrorIs(t, report, ErrOutputNoMatch)
			},
		},
		{
			name: "Having condition passes, destination executes",
			setupRule: func() *MockRule {
				action := NewMockAction[*EventMock, *EventMock](t)
				postCond := NewMockCondition[*EventMock](t)
				destination := NewMockDestination[*EventMock](t)

				output := NewEventMock(nil)
				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(output, nil).Once()
				postCond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				destination.On("Send", mock.Anything, output).
					Return(nil).Once()

				return &MockRule{
					ID:     "test-rule",
					Select: action,
					Having: postCond,
					Into:   destination,
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 0, // Success - no errors reported
			validateReport: func(t *testing.T, report error) {
				// No reports expected on success
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := tc.setupRule()

			collector := newReportCollector()
			rule.Run(t.Context(), tc.event, nil, collector.Collect)

			reports := collector.Errors()
			require.Len(t, reports, tc.expectedReports)
			if tc.expectedReports > 0 {
				tc.validateReport(t, reports[0])
			}
		})
	}
}

func TestRule_ImplementsConditionInterface(t *testing.T) {
	var rule *Rule[int, string]
	require.Implements(t, (*Condition[int])(nil), rule)
}
