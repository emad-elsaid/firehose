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

		registry, err := AddRule(t.Context(), nil, rule1, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, in)
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

		registry, err := AddRule(t.Context(), nil, rule1, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, in)
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

		registry, err := AddRule(t.Context(), nil, rule1, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, in)
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

		registry, err := AddRule(t.Context(), nil, rule1, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule3, in)
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
	_, err := AddRule(t.Context(), nil, rule, event)
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
	_, err := AddRule(t.Context(), nil, rule, event)
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
					Return(event, NewReport(os.ErrInvalid)).Once()

				return rule, event
			},
			expectedReports: 1,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				var actionErr ActionError
				require.ErrorAs(t, report.Err, &actionErr)
				require.ErrorIs(t, report.Err, os.ErrInvalid)
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
