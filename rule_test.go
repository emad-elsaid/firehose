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

		rule := &MockSQLRule{
			ID:     "test-rule",
			From:   source,
			Select: action,
			Into:   destination,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

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
		rule := &MockSQLRule{
			ID:     "test-rule",
			From:   source,
			Select: action,
			Into:   destination,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

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
		rule := &MockSQLRule{
			ID:     "test-rule",
			From:   source,
			Select: action,
			Into:   destination,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

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

		rule1 := &MockSQLRule{
			ID:     "rule1",
			From:   source,
			Select: action,
			Into:   destination,
		}

		rule2 := &MockSQLRule{
			ID:     "rule2",
			From:   source,
			Select: action,
			Into:   destination,
		}

		head, err := Add(t.Context(), nil, rule1)
		require.NoError(t, err)

		head, err = Add(t.Context(), head, rule2)
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

		rule1 := &MockSQLRule{
			ID:     "rule1",
			From:   source,
			Select: action,
			Into:   destination,
		}

		rule2 := &MockSQLRule{
			ID:     "rule2",
			From:   source,
			Select: action,
			Into:   destination,
		}

		head, err := Add(t.Context(), nil, rule1)
		require.NoError(t, err)

		head, err = Add(t.Context(), head, rule2)
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

		rule1 := &MockSQLRule{
			ID:     "rule1",
			From:   source,
			Select: action,
			Into:   destination,
		}

		rule2 := &MockSQLRule{
			ID:     "rule2",
			From:   source,
			Select: action,
			Into:   destination,
		}

		head, err := Add(t.Context(), nil, rule1)
		require.NoError(t, err)

		head, err = Add(t.Context(), head, rule2)
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

		rule1 := &MockSQLRule{
			ID:     "rule1",
			From:   source,
			Select: action,
			Into:   destination,
		}

		rule2 := &MockSQLRule{
			ID:     "rule2",
			From:   source,
			Select: action,
			Into:   destination,
		}

		rule3 := &MockSQLRule{
			ID:     "rule3",
			From:   source,
			Select: action,
			Into:   destination,
		}

		head, err := Add(t.Context(), nil, rule1)
		require.NoError(t, err)

		head, err = Add(t.Context(), head, rule2)
		require.NoError(t, err)

		head, err = Add(t.Context(), head, rule3)
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

		rule := &MockSQLRule{
			ID:     "test-rule",
			From:   source,
			Select: action,
			Into:   destination,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		// Create a Rule with a different type (string instead of *EventMock)
		// This will cause a panic when type-asserting to Runnable[*EventMock]
		incompatibleRule := &SQLRule[string, string]{}
		rule.nextSameSource = incompatibleRule

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

		rule := &MockSQLRule{
			ID:     "test-rule",
			From:   source,
			Select: action,
			Into:   destination,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		in := NewEventMock(nil)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		require.NotPanics(t, func() { rule.callback(t.Context(), in, nil) })
	})
}

func TestRule_NextRunnable(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() *MockSQLRule
		expected bool
	}{
		{
			name: "returns nil when nextSameSource is nil",
			setup: func() *MockSQLRule {
				return &MockSQLRule{ID: "rule1"}
			},
			expected: false,
		},
		{
			name: "returns next runnable when nextSameSource is set",
			setup: func() *MockSQLRule {
				rule1 := &MockSQLRule{ID: "rule1"}
				rule2 := &MockSQLRule{ID: "rule2"}
				rule1.SetNextSameSource(rule2)
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
		setupMocks      func() (*MockSQLRule, *EventMock)
		expectedReports int
		validateReport  func(t *testing.T, report error)
	}{
		{
			name: "successful action and destination",
			setupMocks: func() (*MockSQLRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockSQLRule{
					ID:     "test-rule",
					From:   NewMockSource[*EventMock](t),
					Select: action,
					Into:   destination,
				}

				_, err := Add(t.Context(), nil, rule)
				require.NoError(t, err)

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
			setupMocks: func() (*MockSQLRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockSQLRule{
					ID:     "test-rule",
					From:   NewMockSource[*EventMock](t),
					Select: action,
					Into:   destination,
				}

				_, err := Add(t.Context(), nil, rule)
				require.NoError(t, err)

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
			setupMocks: func() (*MockSQLRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockSQLRule{
					ID:     "test-rule",
					From:   NewMockSource[*EventMock](t),
					Select: action,
					Into:   destination,
				}

				_, err := Add(t.Context(), nil, rule)
				require.NoError(t, err)

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
			err := rule.Run(t.Context(), event, nil)
			if err != nil {
				collector.Collect(err)
			}

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
		setup       func() (*MockSQLRule, *MockSource[*EventMock])
		expectStart bool
		expectError bool
	}{
		{
			name: "starts when prevSameSource is nil (first in chain)",
			setup: func() (*MockSQLRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule := &MockSQLRule{
					ID:   "test-rule",
					From: source,
				}
				done := make(chan struct{})
				source.On("Start", mock.Anything, mock.Anything).
					Return(recvChan(done), nil).Once()
				return rule, source
			},
			expectStart: true,
			expectError: false,
		},
		{
			name: "does not start when prevSameSource is set (not first in chain)",
			setup: func() (*MockSQLRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule1 := &MockSQLRule{ID: "rule1", From: source}
				rule2 := &MockSQLRule{ID: "rule2", From: source}
				rule2.SetPrevSameSource(rule1)
				return rule2, source
			},
			expectStart: false,
			expectError: false,
		},
		{
			name: "returns error when source fails to start",
			setup: func() (*MockSQLRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule := &MockSQLRule{
					ID:   "test-rule",
					From: source,
				}
				source.On("Start", mock.Anything, mock.Anything).
					Return(nil, os.ErrClosed).Once()
				return rule, source
			},
			expectStart: true,
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule, _ := tc.setup()

			done, err := rule.Start(t.Context())

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tc.expectStart && !tc.expectError {
				require.NotNil(t, done)
			}
		})
	}
}

func TestRule_Run_WithConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		setupRule       func() *MockSQLRule
		event           *EventMock
		expectedReports int
		validateReport  func(t *testing.T, report error)
	}{
		{
			name: "Condition fails stops execution",
			setupRule: func() *MockSQLRule {
				cond := NewMockCondition[*EventMock](t)
				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()

				return &MockSQLRule{
					ID:     "test-rule",
					From:   NewMockSource[*EventMock](t),
					Select: NewMockAction[*EventMock, *EventMock](t),
					Into:   NewMockDestination[*EventMock](t),
					Where:  cond,
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
			setupRule: func() *MockSQLRule {
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

				return &MockSQLRule{
					ID:     "test-rule",
					From:   NewMockSource[*EventMock](t),
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
			setupRule: func() *MockSQLRule {
				action := NewMockAction[*EventMock, *EventMock](t)
				postCond := NewMockCondition[*EventMock](t)

				output := NewEventMock(nil)
				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(output, nil).Once()
				postCond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()

				return &MockSQLRule{
					ID:     "test-rule",
					From:   NewMockSource[*EventMock](t),
					Select: action,
					Having: postCond,
					Into:   NewMockDestination[*EventMock](t),
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
			setupRule: func() *MockSQLRule {
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

				return &MockSQLRule{
					ID:     "test-rule",
					From:   NewMockSource[*EventMock](t),
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

			_, err := Add(t.Context(), nil, rule)
			require.NoError(t, err)

			collector := newReportCollector()
			err = rule.Run(t.Context(), tc.event, nil)
			if err != nil {
				collector.Collect(err)
			}

			reports := collector.Errors()
			require.Len(t, reports, tc.expectedReports)
			if tc.expectedReports > 0 {
				tc.validateReport(t, reports[0])
			}
		})
	}
}
