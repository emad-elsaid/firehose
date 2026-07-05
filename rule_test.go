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
			On:   source,
			Then: action,
			To:   destination,
		}

		in := NewEventMock(nil)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(nil)).Once()
		destination.On("Send", t.Context(), in).Return(NewReport(nil)).Once()

		collector := newReportCollector()
		rule.callback(t.Context(), in, collector.Collect)
		reports := collector.Reports()
		require.NotNil(t, reports)
		require.Len(t, reports, 1)
		for _, report := range reports {
			require.NoError(t, report.Err)
		}
	})

	t.Run("callback with action error", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)
		rule := &MockRule{
			On:   source,
			Then: action,
			To:   destination,
		}
		in := NewEventMock(nil)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(os.ErrClosed)).Once()

		collector := newReportCollector()
		rule.callback(t.Context(), in, collector.Collect)
		reports := collector.Reports()
		require.NotNil(t, reports)
		require.Len(t, reports, 1)

		for _, report := range reports {
			require.ErrorIs(t, report.Err, os.ErrClosed)
		}
	})

	t.Run("callback with destination error", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)
		rule := &MockRule{
			On:   source,
			Then: action,
			To:   destination,
		}
		in := NewEventMock(nil)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(nil)).Once()
		destination.On("Send", t.Context(), in).Return(NewReport(os.ErrClosed)).Once()

		collector := newReportCollector()
		rule.callback(t.Context(), in, collector.Collect)
		reports := collector.Reports()
		require.NotNil(t, reports)
		require.Len(t, reports, 1)

		for _, report := range reports {
			require.ErrorIs(t, report.Err, os.ErrClosed)
		}
	})

	t.Run("callback chains to next rule with same source", func(t *testing.T) {
		in := NewEventMock(nil)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule1 := &MockRule{
			ID:   "rule1",
			On:   source,
			Then: action,
			To:   destination,
		}

		rule2 := &MockRule{
			ID:   "rule2",
			On:   source,
			Then: action,
			To:   destination,
		}

		registry, err := AddRule(t.Context(), nil, rule1)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2)
		require.NoError(t, err)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(nil)).Twice()
		destination.On("Send", t.Context(), in).Return(NewReport(nil)).Twice()

		collector := newReportCollector()
		rule1.callback(t.Context(), in, collector.Collect)
		reports := collector.Reports()
		require.NotNil(t, reports)
		require.Len(t, reports, 2)

		for _, report := range reports {
			require.NoError(t, report.Err, in)
		}
	})

	t.Run("callback chain continue on action error in first rule", func(t *testing.T) {
		in := NewEventMock(nil)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule1 := &MockRule{
			ID:   "rule1",
			On:   source,
			Then: action,
			To:   destination,
		}

		rule2 := &MockRule{
			ID:   "rule2",
			On:   source,
			Then: action,
			To:   destination,
		}

		registry, err := AddRule(t.Context(), nil, rule1)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2)
		require.NoError(t, err)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(os.ErrClosed)).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(nil)).Once()
		destination.On("Send", t.Context(), in).Return(NewReport(nil)).Once()

		collector := newReportCollector()
		rule1.callback(t.Context(), in, collector.Collect)
		reports := collector.Reports()
		require.NotNil(t, reports)
		require.Len(t, reports, 2)
		require.ErrorIs(t, reports[0].Err, os.ErrClosed)
		require.NoError(t, reports[1].Err)
	})

	t.Run("callback chain propagates error from second rule", func(t *testing.T) {
		in := NewEventMock(nil)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule1 := &MockRule{
			ID:   "rule1",
			On:   source,
			Then: action,
			To:   destination,
		}

		rule2 := &MockRule{
			ID:   "rule2",
			On:   source,
			Then: action,
			To:   destination,
		}

		registry, err := AddRule(t.Context(), nil, rule1)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2)
		require.NoError(t, err)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(nil)).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(os.ErrClosed)).Once()
		destination.On("Send", t.Context(), in).Return(NewReport(nil)).Once()

		collector := newReportCollector()
		rule1.callback(t.Context(), in, collector.Collect)
		reports := collector.Reports()
		require.NotNil(t, reports)
		require.Len(t, reports, 2)
		require.NoError(t, reports[0].Err)
		require.ErrorIs(t, reports[1].Err, os.ErrClosed)
	})

	t.Run("callback with three rules in chain", func(t *testing.T) {
		in := NewEventMock(nil)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule1 := &MockRule{
			ID:   "rule1",
			On:   source,
			Then: action,
			To:   destination,
		}

		rule2 := &MockRule{
			ID:   "rule2",
			On:   source,
			Then: action,
			To:   destination,
		}

		rule3 := &MockRule{
			ID:   "rule3",
			On:   source,
			Then: action,
			To:   destination,
		}

		registry, err := AddRule(t.Context(), nil, rule1)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule3)
		require.NoError(t, err)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(nil)).Times(3)
		destination.On("Send", t.Context(), in).Return(NewReport(nil)).Times(3)

		collector := newReportCollector()
		rule1.callback(t.Context(), in, collector.Collect)
		reports := collector.Reports()
		require.NotNil(t, reports)
		require.Len(t, reports, 3)

		for _, report := range reports {
			require.NoError(t, report.Err)
		}
	})

	t.Run("callback with incompatible next rule type", func(t *testing.T) {
		in := NewEventMock(nil)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule := &MockRule{
			On:   source,
			Then: action,
			To:   destination,
		}

		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(nil)).Once()
		destination.On("Send", t.Context(), in).Return(NewReport(nil)).Once()

		// Create a Rule with a different type (string instead of *EventMock)
		// This will cause a panic when type-asserting to Runnable[*EventMock]
		incompatibleRule := &Rule[string, string]{}
		incompatibleSourceRegistry := newMocksourceRegistry(t)
		incompatibleSourceRegistry.On("getRegistry").Return(incompatibleRule).Once()
		rule.nextSameSource = incompatibleSourceRegistry

		collector := newReportCollector()
		require.Panics(t, func() { rule.callback(t.Context(), in, collector.Collect) })
		reports := collector.Reports()
		require.NotNil(t, reports)
		require.Len(t, reports, 1)
	})

	t.Run("callback with nil reporter function", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule := &MockRule{
			On:   source,
			Then: action,
			To:   destination,
		}

		in := NewEventMock(nil)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(nil)).Once()
		destination.On("Send", t.Context(), in).Return(NewReport(nil)).Once()

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
		ID:   "test-rule",
		On:   source,
		Then: oldAction,
		To:   destination,
	}

	// Register the rule
	_, err := AddRule(t.Context(), nil, rule)
	require.NoError(t, err)

	// Override the action after registration
	rule.Then = newAction

	newAction.On("Process", t.Context(), event, mock.Anything).
		Return(event, NewReport(nil)).Once()
	destination.On("Send", t.Context(), event).
		Return(NewReport(nil)).Once()

	collector := newReportCollector()
	rule.callback(t.Context(), event, collector.Collect)

	reports := collector.Reports()
	require.NotNil(t, reports)
	require.Len(t, reports, 1)
	require.NoError(t, reports[0].Err)
}

func TestRuleDestinationOverride(t *testing.T) {
	source := NewMockSource[*EventMock](t)
	action := NewMockAction[*EventMock, *EventMock](t)
	oldDestination := NewMockDestination[*EventMock](t)
	newDestination := NewMockDestination[*EventMock](t)
	event := NewEventMock(nil)

	rule := &Rule[*EventMock, *EventMock]{
		ID:   "test-rule",
		On:   source,
		Then: action,
		To:   oldDestination,
	}

	// Register the rule
	_, err := AddRule(t.Context(), nil, rule)
	require.NoError(t, err)

	// Override the destination after registration
	rule.To = newDestination

	action.On("Process", t.Context(), event, mock.Anything).
		Return(event, NewReport(nil)).Once()
	newDestination.On("Send", t.Context(), event).
		Return(NewReport(nil)).Once()

	collector := newReportCollector()
	rule.callback(t.Context(), event, collector.Collect)

	reports := collector.Reports()
	require.NotNil(t, reports)
	require.Len(t, reports, 1)
	require.NoError(t, reports[0].Err)
}

func TestRule_Process(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupAction    func() Action[*EventMock, *EventMock]
		event          *EventMock
		expectedError  bool
		validateOutput func(t *testing.T, output *EventMock, report Report)
	}{
		{
			name: "successful action processing",
			setupAction: func() Action[*EventMock, *EventMock] {
				action := NewMockAction[*EventMock, *EventMock](t)
				output := NewEventMock(map[string]any{"processed": true})
				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(output, NewReport(nil)).Once()
				return action
			},
			event:         NewEventMock(nil),
			expectedError: false,
			validateOutput: func(t *testing.T, output *EventMock, report Report) {
				require.NotNil(t, output)
				require.NoError(t, report.Err)
			},
		},
		{
			name: "action returns error",
			setupAction: func() Action[*EventMock, *EventMock] {
				action := NewMockAction[*EventMock, *EventMock](t)
				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return((*EventMock)(nil), NewReport(os.ErrClosed)).Once()
				return action
			},
			event:         NewEventMock(nil),
			expectedError: true,
			validateOutput: func(t *testing.T, output *EventMock, report Report) {
				require.ErrorIs(t, report.Err, os.ErrClosed)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := &Rule[*EventMock, *EventMock]{
				Then: tc.setupAction(),
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
		validateReport   func(t *testing.T, report Report)
	}{
		{
			name: "successful send",
			setupDestination: func() Destination[*EventMock] {
				dest := NewMockDestination[*EventMock](t)
				dest.On("Send", mock.Anything, mock.Anything).
					Return(NewReport(nil)).Once()
				return dest
			},
			event:         NewEventMock(nil),
			expectedError: false,
			validateReport: func(t *testing.T, report Report) {
				require.NoError(t, report.Err)
			},
		},
		{
			name: "send returns error",
			setupDestination: func() Destination[*EventMock] {
				dest := NewMockDestination[*EventMock](t)
				dest.On("Send", mock.Anything, mock.Anything).
					Return(NewReport(os.ErrPermission)).Once()
				return dest
			},
			event:         NewEventMock(nil),
			expectedError: true,
			validateReport: func(t *testing.T, report Report) {
				require.ErrorIs(t, report.Err, os.ErrPermission)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := &Rule[*EventMock, *EventMock]{
				To: tc.setupDestination(),
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
		setupCondition func() If[*EventMock]
		event          *EventMock
		expectedPass   bool
		expectedError  bool
		validateReport func(t *testing.T, report Report)
	}{
		{
			name: "nil condition passes",
			setupCondition: func() If[*EventMock] {
				return nil
			},
			event:         NewEventMock(nil),
			expectedPass:  true,
			expectedError: false,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "", report.Rule)
				require.NoError(t, report.Err)
			},
		},
		{
			name: "passing condition",
			setupCondition: func() If[*EventMock] {
				cond := NewMockIf[*EventMock](t)
				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				return cond
			},
			event:         NewEventMock(nil),
			expectedPass:  true,
			expectedError: false,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "", report.Rule)
				require.NoError(t, report.Err)
			},
		},
		{
			name: "failing condition returns ErrNoMatch",
			setupCondition: func() If[*EventMock] {
				cond := NewMockIf[*EventMock](t)
				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()
				return cond
			},
			event:         NewEventMock(nil),
			expectedPass:  false,
			expectedError: false,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				require.ErrorIs(t, report.Err, ErrInputNoMatch)
			},
		},
		{
			name: "condition evaluation error",
			setupCondition: func() If[*EventMock] {
				cond := NewMockIf[*EventMock](t)
				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, os.ErrInvalid).Once()
				return cond
			},
			event:         NewEventMock(nil),
			expectedPass:  false,
			expectedError: true,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				var condErr ConditionError
				require.ErrorAs(t, report.Err, &condErr)
				require.ErrorIs(t, report.Err, os.ErrInvalid)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := &Rule[*EventMock, *EventMock]{
				ID: "test-rule",
				If: tc.setupCondition(),
			}

			pass, report := rule.evaluateCondition(t.Context(), tc.event, nil)

			require.Equal(t, tc.expectedPass, pass)
			tc.validateReport(t, report)
		})
	}
}

func TestRule_EvaluateOutputCondition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupCondition func() If[*EventMock]
		event          *EventMock
		expectedPass   bool
		expectedError  bool
		validateReport func(t *testing.T, report Report)
	}{
		{
			name: "nil output condition passes",
			setupCondition: func() If[*EventMock] {
				return nil
			},
			event:         NewEventMock(nil),
			expectedPass:  true,
			expectedError: false,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "", report.Rule)
				require.NoError(t, report.Err)
			},
		},
		{
			name: "passing output condition",
			setupCondition: func() If[*EventMock] {
				cond := NewMockIf[*EventMock](t)
				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				return cond
			},
			event:         NewEventMock(nil),
			expectedPass:  true,
			expectedError: false,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "", report.Rule)
				require.NoError(t, report.Err)
			},
		},
		{
			name: "failing output condition returns ErrNoMatch",
			setupCondition: func() If[*EventMock] {
				cond := NewMockIf[*EventMock](t)
				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()
				return cond
			},
			event:         NewEventMock(nil),
			expectedPass:  false,
			expectedError: false,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				require.ErrorIs(t, report.Err, ErrOutputNoMatch)
			},
		},
		{
			name: "output condition evaluation error",
			setupCondition: func() If[*EventMock] {
				cond := NewMockIf[*EventMock](t)
				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, os.ErrNotExist).Once()
				return cond
			},
			event:         NewEventMock(nil),
			expectedPass:  false,
			expectedError: true,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				var condErr ConditionError
				require.ErrorAs(t, report.Err, &condErr)
				require.ErrorIs(t, report.Err, os.ErrNotExist)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := &Rule[*EventMock, *EventMock]{
				ID:       "test-rule",
				IfOutput: tc.setupCondition(),
			}

			pass, report := rule.evaluateOutputCondition(t.Context(), tc.event, nil)

			require.Equal(t, tc.expectedPass, pass)
			tc.validateReport(t, report)
		})
	}
}

func TestRule_ProcessAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupAction    func() Action[*EventMock, *EventMock]
		event          *EventMock
		expectedError  bool
		validateReport func(t *testing.T, output *EventMock, report Report)
	}{
		{
			name: "successful action",
			setupAction: func() Action[*EventMock, *EventMock] {
				action := NewMockAction[*EventMock, *EventMock](t)
				output := NewEventMock(map[string]any{"result": "ok"})
				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(output, NewReport(nil)).Once()
				return action
			},
			event:         NewEventMock(nil),
			expectedError: false,
			validateReport: func(t *testing.T, output *EventMock, report Report) {
				require.NotNil(t, output)
				require.Equal(t, "test-rule", report.Rule)
				require.NoError(t, report.Err)
			},
		},
		{
			name: "action error wrapped as ActionError",
			setupAction: func() Action[*EventMock, *EventMock] {
				action := NewMockAction[*EventMock, *EventMock](t)
				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return((*EventMock)(nil), NewReport(os.ErrClosed)).Once()
				return action
			},
			event:         NewEventMock(nil),
			expectedError: true,
			validateReport: func(t *testing.T, output *EventMock, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				var actionErr ActionError
				require.ErrorAs(t, report.Err, &actionErr)
				require.ErrorIs(t, report.Err, os.ErrClosed)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := &Rule[*EventMock, *EventMock]{
				ID:   "test-rule",
				Then: tc.setupAction(),
			}

			output, report := rule.processAction(t.Context(), tc.event, nil)
			tc.validateReport(t, output, report)
		})
	}
}

func TestRule_ProcessDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		setupDestination func() Destination[*EventMock]
		event            *EventMock
		expectedError    bool
		validateReport   func(t *testing.T, report Report)
	}{
		{
			name: "successful destination",
			setupDestination: func() Destination[*EventMock] {
				dest := NewMockDestination[*EventMock](t)
				dest.On("Send", mock.Anything, mock.Anything).
					Return(NewReport(nil)).Once()
				return dest
			},
			event:         NewEventMock(nil),
			expectedError: false,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				require.NoError(t, report.Err)
			},
		},
		{
			name: "destination error wrapped as DestinationError",
			setupDestination: func() Destination[*EventMock] {
				dest := NewMockDestination[*EventMock](t)
				dest.On("Send", mock.Anything, mock.Anything).
					Return(NewReport(os.ErrPermission)).Once()
				return dest
			},
			event:         NewEventMock(nil),
			expectedError: true,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				var destErr DestinationError
				require.ErrorAs(t, report.Err, &destErr)
				require.ErrorIs(t, report.Err, os.ErrPermission)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := &Rule[*EventMock, *EventMock]{
				ID: "test-rule",
				To: tc.setupDestination(),
			}

			report := rule.processDestination(t.Context(), tc.event)
			tc.validateReport(t, report)
		})
	}
}

func TestRule_ResolveAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setupRule     func() *Rule[*EventMock, *EventMock]
		expectWrapped bool
	}{
		{
			name: "returns action when no wrappers",
			setupRule: func() *Rule[*EventMock, *EventMock] {
				action := NewMockAction[*EventMock, *EventMock](t)
				return &Rule[*EventMock, *EventMock]{
					Then: action,
				}
			},
			expectWrapped: false,
		},
		{
			name: "returns wrapper when wrappers exist",
			setupRule: func() *Rule[*EventMock, *EventMock] {
				action := NewMockAction[*EventMock, *EventMock](t)
				wrapper := NewMockAction[*EventMock, *EventMock](t)
				return &Rule[*EventMock, *EventMock]{
					Then:           action,
					actionWrappers: wrapper,
				}
			},
			expectWrapped: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := tc.setupRule()
			resolved := rule.resolveAction()
			require.NotNil(t, resolved)

			if tc.expectWrapped {
				require.Equal(t, rule.actionWrappers, resolved)
			} else {
				require.Equal(t, rule.Then, resolved)
			}
		})
	}
}

func TestRule_ResolveDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setupRule     func() *Rule[*EventMock, *EventMock]
		expectWrapped bool
	}{
		{
			name: "returns destination when no wrappers",
			setupRule: func() *Rule[*EventMock, *EventMock] {
				dest := NewMockDestination[*EventMock](t)
				return &Rule[*EventMock, *EventMock]{
					To: dest,
				}
			},
			expectWrapped: false,
		},
		{
			name: "returns wrapper when wrappers exist",
			setupRule: func() *Rule[*EventMock, *EventMock] {
				dest := NewMockDestination[*EventMock](t)
				wrapper := NewMockDestination[*EventMock](t)
				return &Rule[*EventMock, *EventMock]{
					To:                  dest,
					destinationWrappers: wrapper,
				}
			},
			expectWrapped: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := tc.setupRule()
			resolved := rule.resolveDestination()
			require.NotNil(t, resolved)

			if tc.expectWrapped {
				require.Equal(t, rule.destinationWrappers, resolved)
			} else {
				require.Equal(t, rule.To, resolved)
			}
		})
	}
}

// Getter/setter methods are trivial one-line property accessors - testing omitted

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
		validateReport  func(t *testing.T, report Report)
	}{
		{
			name: "successful action and destination",
			setupMocks: func() (*MockRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockRule{
					ID:   "test-rule",
					Then: action,
					To:   destination,
				}

				action.On("Process", mock.Anything, event, mock.Anything).
					Return(event, NewReport(nil)).Once()
				destination.On("Send", mock.Anything, event).
					Return(NewReport(nil)).Once()

				return rule, event
			},
			expectedReports: 1,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				require.NoError(t, report.Err)
			},
		},
		{
			name: "action error stops destination call",
			setupMocks: func() (*MockRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockRule{
					ID:   "test-rule",
					Then: action,
					To:   destination,
				}

				action.On("Process", mock.Anything, event, mock.Anything).
					Return(event, NewReport(os.ErrClosed)).Once()

				return rule, event
			},
			expectedReports: 1,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				var actionErr ActionError
				require.ErrorAs(t, report.Err, &actionErr)
				require.ErrorIs(t, report.Err, os.ErrClosed)
			},
		},
		{
			name: "destination error is reported",
			setupMocks: func() (*MockRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockRule{
					ID:   "test-rule",
					Then: action,
					To:   destination,
				}

				action.On("Process", mock.Anything, event, mock.Anything).
					Return(event, NewReport(nil)).Once()
				destination.On("Send", mock.Anything, event).
					Return(NewReport(os.ErrPermission)).Once()

				return rule, event
			},
			expectedReports: 1,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				var destinationErr DestinationError
				require.ErrorAs(t, report.Err, &destinationErr)
				require.ErrorIs(t, report.Err, os.ErrPermission)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule, event := tc.setupMocks()

			collector := newReportCollector()

			// Use nil symbols for this test
			rule.Run(t.Context(), event, nil, collector.Collect)

			reports := collector.Reports()

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
					ID: "test-rule",
					On: source,
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
				rule1 := &MockRule{ID: "rule1", On: source}
				rule2 := &MockRule{ID: "rule2", On: source}
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
					ID: "test-rule",
					On: source,
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

func TestAsActionError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		input         error
		expectWrapped bool
	}{
		{
			name:          "wraps non-ActionError",
			input:         os.ErrClosed,
			expectWrapped: true,
		},
		{
			name:          "does not double-wrap ActionError",
			input:         ActionError{Err: os.ErrClosed},
			expectWrapped: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := asActionError(tc.input)

			var actionErr ActionError
			require.ErrorAs(t, result, &actionErr)

			if tc.expectWrapped {
				require.NotEqual(t, tc.input, result)
				require.ErrorIs(t, result, tc.input)
			} else {
				require.Equal(t, tc.input, result)
			}
		})
	}
}

func TestAsDestinationError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		input         error
		expectWrapped bool
	}{
		{
			name:          "wraps non-DestinationError",
			input:         os.ErrPermission,
			expectWrapped: true,
		},
		{
			name:          "does not double-wrap DestinationError",
			input:         DestinationError{Err: os.ErrPermission},
			expectWrapped: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := asDestinationError(tc.input)

			var destErr DestinationError
			require.ErrorAs(t, result, &destErr)

			if tc.expectWrapped {
				require.NotEqual(t, tc.input, result)
				require.ErrorIs(t, result, tc.input)
			} else {
				require.Equal(t, tc.input, result)
			}
		})
	}
}

func TestReportIfNeeded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		reportFn   ReportFunc
		report     Report
		expectCall bool
	}{
		{
			name:       "calls reportFn when not nil",
			reportFn:   func(r Report) {},
			report:     NewReport(nil),
			expectCall: true,
		},
		{
			name:       "does not panic when reportFn is nil",
			reportFn:   nil,
			report:     NewReport(nil),
			expectCall: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			var reportFn ReportFunc
			if tc.reportFn != nil {
				reportFn = func(r Report) {
					called = true
				}
			}

			require.NotPanics(t, func() {
				reportIfNeeded(reportFn, tc.report)
			})

			require.Equal(t, tc.expectCall, called)
		})
	}
}

func TestRule_CombineIf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setupParent   func() If[*EventMock]
		setupChild    func() If[*EventMock]
		expectNil     bool
		expectedCount int
	}{
		{
			name: "both nil returns nil",
			setupParent: func() If[*EventMock] {
				return nil
			},
			setupChild: func() If[*EventMock] {
				return nil
			},
			expectNil:     true,
			expectedCount: 0,
		},
		{
			name: "parent nil returns child",
			setupParent: func() If[*EventMock] {
				return nil
			},
			setupChild: func() If[*EventMock] {
				return NewMockIf[*EventMock](t)
			},
			expectNil:     false,
			expectedCount: 1,
		},
		{
			name: "child nil returns parent",
			setupParent: func() If[*EventMock] {
				return NewMockIf[*EventMock](t)
			},
			setupChild: func() If[*EventMock] {
				return nil
			},
			expectNil:     false,
			expectedCount: 1,
		},
		{
			name: "both non-nil combines into slice",
			setupParent: func() If[*EventMock] {
				return NewMockIf[*EventMock](t)
			},
			setupChild: func() If[*EventMock] {
				return NewMockIf[*EventMock](t)
			},
			expectNil:     false,
			expectedCount: 2,
		},
		{
			name: "flattens nested ifSlice from parent",
			setupParent: func() If[*EventMock] {
				cond1 := NewMockIf[*EventMock](t)
				cond2 := NewMockIf[*EventMock](t)
				return ifSlice[*EventMock]{cond1, cond2}
			},
			setupChild: func() If[*EventMock] {
				return NewMockIf[*EventMock](t)
			},
			expectNil:     false,
			expectedCount: 3,
		},
		{
			name: "flattens nested ifSlice from child",
			setupParent: func() If[*EventMock] {
				return NewMockIf[*EventMock](t)
			},
			setupChild: func() If[*EventMock] {
				cond1 := NewMockIf[*EventMock](t)
				cond2 := NewMockIf[*EventMock](t)
				return ifSlice[*EventMock]{cond1, cond2}
			},
			expectNil:     false,
			expectedCount: 3,
		},
		{
			name: "flattens nested ifSlice from both",
			setupParent: func() If[*EventMock] {
				cond1 := NewMockIf[*EventMock](t)
				cond2 := NewMockIf[*EventMock](t)
				return ifSlice[*EventMock]{cond1, cond2}
			},
			setupChild: func() If[*EventMock] {
				cond3 := NewMockIf[*EventMock](t)
				cond4 := NewMockIf[*EventMock](t)
				return ifSlice[*EventMock]{cond3, cond4}
			},
			expectNil:     false,
			expectedCount: 4,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := &Rule[*EventMock, *EventMock]{}
			parent := tc.setupParent()
			child := tc.setupChild()

			result := rule.combineIf(parent, child)

			if tc.expectNil {
				require.Nil(t, result)
				return
			}

			require.NotNil(t, result)

			// Verify count by flattening
			flattened := flattenIf(result)
			require.Len(t, flattened, tc.expectedCount)
		})
	}
}

func TestFlattenIf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setupIf       func() If[*EventMock]
		expectNil     bool
		expectedCount int
	}{
		{
			name: "nil returns nil",
			setupIf: func() If[*EventMock] {
				return nil
			},
			expectNil:     true,
			expectedCount: 0,
		},
		{
			name: "single condition returns slice with one element",
			setupIf: func() If[*EventMock] {
				return NewMockIf[*EventMock](t)
			},
			expectNil:     false,
			expectedCount: 1,
		},
		{
			name: "ifSlice returns all elements",
			setupIf: func() If[*EventMock] {
				cond1 := NewMockIf[*EventMock](t)
				cond2 := NewMockIf[*EventMock](t)
				cond3 := NewMockIf[*EventMock](t)
				return ifSlice[*EventMock]{cond1, cond2, cond3}
			},
			expectNil:     false,
			expectedCount: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ifVal := tc.setupIf()
			result := flattenIf(ifVal)

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
		setupSlice    func() ifSlice[*EventMock]
		event         *EventMock
		expectedPass  bool
		expectedError bool
	}{
		{
			name: "empty slice passes",
			setupSlice: func() ifSlice[*EventMock] {
				return ifSlice[*EventMock]{}
			},
			event:         NewEventMock(nil),
			expectedPass:  true,
			expectedError: false,
		},
		{
			name: "all conditions pass",
			setupSlice: func() ifSlice[*EventMock] {
				cond1 := NewMockIf[*EventMock](t)
				cond2 := NewMockIf[*EventMock](t)
				cond1.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				cond2.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				return ifSlice[*EventMock]{cond1, cond2}
			},
			event:         NewEventMock(nil),
			expectedPass:  true,
			expectedError: false,
		},
		{
			name: "first condition fails",
			setupSlice: func() ifSlice[*EventMock] {
				cond1 := NewMockIf[*EventMock](t)
				cond2 := NewMockIf[*EventMock](t)
				cond1.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()
				return ifSlice[*EventMock]{cond1, cond2}
			},
			event:         NewEventMock(nil),
			expectedPass:  false,
			expectedError: false,
		},
		{
			name: "second condition fails",
			setupSlice: func() ifSlice[*EventMock] {
				cond1 := NewMockIf[*EventMock](t)
				cond2 := NewMockIf[*EventMock](t)
				cond1.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				cond2.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()
				return ifSlice[*EventMock]{cond1, cond2}
			},
			event:         NewEventMock(nil),
			expectedPass:  false,
			expectedError: false,
		},
		{
			name: "first condition returns error",
			setupSlice: func() ifSlice[*EventMock] {
				cond1 := NewMockIf[*EventMock](t)
				cond2 := NewMockIf[*EventMock](t)
				cond1.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, os.ErrInvalid).Once()
				return ifSlice[*EventMock]{cond1, cond2}
			},
			event:         NewEventMock(nil),
			expectedPass:  false,
			expectedError: true,
		},
		{
			name: "second condition returns error",
			setupSlice: func() ifSlice[*EventMock] {
				cond1 := NewMockIf[*EventMock](t)
				cond2 := NewMockIf[*EventMock](t)
				cond1.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				cond2.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, os.ErrPermission).Once()
				return ifSlice[*EventMock]{cond1, cond2}
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
		validateReport  func(t *testing.T, report Report)
	}{
		{
			name: "If condition fails stops execution",
			setupRule: func() *MockRule {
				cond := NewMockIf[*EventMock](t)
				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()

				return &MockRule{
					ID: "test-rule",
					If: cond,
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 1,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				require.ErrorIs(t, report.Err, ErrInputNoMatch)
			},
		},
		{
			name: "If condition passes, action executes",
			setupRule: func() *MockRule {
				cond := NewMockIf[*EventMock](t)
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)

				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				output := NewEventMock(nil)
				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(output, NewReport(nil)).Once()
				destination.On("Send", mock.Anything, output).
					Return(NewReport(nil)).Once()

				return &MockRule{
					ID:   "test-rule",
					If:   cond,
					Then: action,
					To:   destination,
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 1,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				require.NoError(t, report.Err)
			},
		},
		{
			name: "IfOutput condition fails stops destination",
			setupRule: func() *MockRule {
				action := NewMockAction[*EventMock, *EventMock](t)
				postCond := NewMockIf[*EventMock](t)

				output := NewEventMock(nil)
				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(output, NewReport(nil)).Once()
				postCond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()

				return &MockRule{
					ID:       "test-rule",
					Then:     action,
					IfOutput: postCond,
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 1,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				require.ErrorIs(t, report.Err, ErrOutputNoMatch)
			},
		},
		{
			name: "IfOutput condition passes, destination executes",
			setupRule: func() *MockRule {
				action := NewMockAction[*EventMock, *EventMock](t)
				postCond := NewMockIf[*EventMock](t)
				destination := NewMockDestination[*EventMock](t)

				output := NewEventMock(nil)
				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(output, NewReport(nil)).Once()
				postCond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				destination.On("Send", mock.Anything, output).
					Return(NewReport(nil)).Once()

				return &MockRule{
					ID:       "test-rule",
					Then:     action,
					IfOutput: postCond,
					To:       destination,
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 1,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				require.NoError(t, report.Err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := tc.setupRule()

			collector := newReportCollector()
			rule.Run(t.Context(), tc.event, nil, collector.Collect)

			reports := collector.Reports()
			require.Len(t, reports, tc.expectedReports)
			if tc.expectedReports > 0 {
				tc.validateReport(t, reports[0])
			}
		})
	}
}
