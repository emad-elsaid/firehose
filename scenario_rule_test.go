package firehose

import (
	"os"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestScenarioRuleCallback(t *testing.T) {
	t.Parallel()

	t.Run("successful callback with action and destination", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule := &MockScenarioRule{
			ID:   "test-rule",
			When: source,
			Then: action,
			To:   destination,
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
		require.Len(t, reports, 0)
	})

	t.Run("callback with action error", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)
		rule := &MockScenarioRule{
			ID:   "test-rule",
			When: source,
			Then: action,
			To:   destination,
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
		rule := &MockScenarioRule{
			ID:   "test-rule",
			When: source,
			Then: action,
			To:   destination,
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

		rule1 := &MockScenarioRule{
			ID:   "rule1",
			When: source,
			Then: action,
			To:   destination,
		}

		rule2 := &MockScenarioRule{
			ID:   "rule2",
			When: source,
			Then: action,
			To:   destination,
		}

		head, err := Add(t.Context(), nil, rule1)
		require.NoError(t, err)

		head, err = Add(t.Context(), head, rule2)
		require.NoError(t, err)
		_ = head

		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Twice()
		destination.On("Send", t.Context(), in).Return(nil).Twice()

		collector := newReportCollector()
		rule1.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.NotNil(t, reports)
		require.Len(t, reports, 0)
	})

	t.Run("callback chain continue on action error in first rule", func(t *testing.T) {
		in := NewEventMock(nil)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule1 := &MockScenarioRule{
			ID:   "rule1",
			When: source,
			Then: action,
			To:   destination,
		}

		rule2 := &MockScenarioRule{
			ID:   "rule2",
			When: source,
			Then: action,
			To:   destination,
		}

		head, err := Add(t.Context(), nil, rule1)
		require.NoError(t, err)

		head, err = Add(t.Context(), head, rule2)
		require.NoError(t, err)
		_ = head

		action.On("Process", t.Context(), in, mock.Anything).Return(in, os.ErrClosed).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		collector := newReportCollector()
		rule1.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.NotNil(t, reports)
		require.Len(t, reports, 1)
		require.ErrorIs(t, reports[0], os.ErrClosed)
	})

	t.Run("callback with nil reporter function", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule := &MockScenarioRule{
			ID:   "test-rule",
			When: source,
			Then: action,
			To:   destination,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		in := NewEventMock(nil)

		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		destination.On("Send", t.Context(), in).Return(nil).Once()

		require.NotPanics(t, func() { rule.callback(t.Context(), in, nil) })
	})
}

func TestScenarioRule_NextRunnable(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() *MockScenarioRule
		expected bool
	}{
		{
			name: "returns nil when nextSameSource is nil",
			setup: func() *MockScenarioRule {
				return &MockScenarioRule{ID: "rule1"}
			},
			expected: false,
		},
		{
			name: "returns next runnable when nextSameSource is set",
			setup: func() *MockScenarioRule {
				rule1 := &MockScenarioRule{ID: "rule1"}
				rule2 := &MockScenarioRule{ID: "rule2"}
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

func TestScenarioRule_Run(t *testing.T) {
	tests := []struct {
		name            string
		setupMocks      func() (*MockScenarioRule, *EventMock)
		expectedReports int
		validateReport  func(t *testing.T, report error)
	}{
		{
			name: "successful action and destination",
			setupMocks: func() (*MockScenarioRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockScenarioRule{
					ID:   "test-rule",
					When: NewMockSource[*EventMock](t),
					Then: action,
					To:   destination,
				}

				_, err := Add(t.Context(), nil, rule)
				require.NoError(t, err)

				action.On("Process", mock.Anything, event, mock.Anything).
					Return(event, nil).Once()
				destination.On("Send", mock.Anything, event).
					Return(nil).Once()

				return rule, event
			},
			expectedReports: 0,
			validateReport: func(t *testing.T, report error) {
			},
		},
		{
			name: "action error stops destination call",
			setupMocks: func() (*MockScenarioRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockScenarioRule{
					ID:   "test-rule",
					When: NewMockSource[*EventMock](t),
					Then: action,
					To:   destination,
				}

				_, err := Add(t.Context(), nil, rule)
				require.NoError(t, err)

				action.On("Process", mock.Anything, event, mock.Anything).
					Return(event, os.ErrClosed).Once()

				return rule, event
			},
			expectedReports: 1,
			validateReport: func(t *testing.T, report error) {
				var actionErr ActionError
				require.ErrorAs(t, report, &actionErr)
				require.ErrorIs(t, report, os.ErrClosed)
			},
		},
		{
			name: "destination error is reported",
			setupMocks: func() (*MockScenarioRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockScenarioRule{
					ID:   "test-rule",
					When: NewMockSource[*EventMock](t),
					Then: action,
					To:   destination,
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

func TestScenarioRule_Start(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() (*MockScenarioRule, *MockSource[*EventMock])
		expectStart bool
		expectError bool
	}{
		{
			name: "starts when prevSameSource is nil (first in chain)",
			setup: func() (*MockScenarioRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule := &MockScenarioRule{
					ID:   "test-rule",
					When: source,
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
			setup: func() (*MockScenarioRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule1 := &MockScenarioRule{ID: "rule1", When: source}
				rule2 := &MockScenarioRule{ID: "rule2", When: source}
				rule2.SetPrevSameSource(rule1)
				return rule2, source
			},
			expectStart: false,
			expectError: false,
		},
		{
			name: "returns error when source fails to start",
			setup: func() (*MockScenarioRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule := &MockScenarioRule{
					ID:   "test-rule",
					When: source,
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

func TestScenarioRule_Run_WithConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		setupRule       func() *MockScenarioRule
		event           *EventMock
		expectedReports int
		validateReport  func(t *testing.T, report error)
	}{
		{
			name: "Given condition fails stops execution",
			setupRule: func() *MockScenarioRule {
				cond := NewMockCondition[*EventMock](t)
				cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()

				return &MockScenarioRule{
					ID:    "test-rule",
					When:  NewMockSource[*EventMock](t),
					Then:  NewMockAction[*EventMock, *EventMock](t),
					To:    NewMockDestination[*EventMock](t),
					Given: cond,
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 1,
			validateReport: func(t *testing.T, report error) {
				require.ErrorIs(t, report, ErrInputNoMatch)
			},
		},
		{
			name: "Given condition passes, Then executes",
			setupRule: func() *MockScenarioRule {
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

				return &MockScenarioRule{
					ID:    "test-rule",
					When:  NewMockSource[*EventMock](t),
					Given: cond,
					Then:  action,
					To:    destination,
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 0,
			validateReport: func(t *testing.T, report error) {
			},
		},
		{
			name: "GivenOutput condition fails stops destination",
			setupRule: func() *MockScenarioRule {
				action := NewMockAction[*EventMock, *EventMock](t)
				postCond := NewMockCondition[*EventMock](t)

				output := NewEventMock(nil)
				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(output, nil).Once()
				postCond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()

				return &MockScenarioRule{
					ID:          "test-rule",
					When:        NewMockSource[*EventMock](t),
					Then:        action,
					GivenOutput: postCond,
					To:          NewMockDestination[*EventMock](t),
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 1,
			validateReport: func(t *testing.T, report error) {
				require.ErrorIs(t, report, ErrOutputNoMatch)
			},
		},
		{
			name: "GivenOutput condition passes, To executes",
			setupRule: func() *MockScenarioRule {
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

				return &MockScenarioRule{
					ID:          "test-rule",
					When:        NewMockSource[*EventMock](t),
					Then:        action,
					GivenOutput: postCond,
					To:          destination,
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 0,
			validateReport: func(t *testing.T, report error) {
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

func TestScenarioRule_IntegrationWithAdd(t *testing.T) {
	t.Parallel()

	t.Run("mixed SQLRule and ScenarioRule with same source", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		sqlRule := &MockSQLRule{
			ID:     "sql-rule",
			From:   source,
			Select: action,
			Into:   destination,
		}

		scenarioRule := &MockScenarioRule{
			ID:   "scenario-rule",
			When: source,
			Then: action,
			To:   destination,
		}

		head, err := Add(t.Context(), nil, sqlRule)
		require.NoError(t, err)

		head, err = Add(t.Context(), head, scenarioRule)
		require.NoError(t, err)
		_ = head

		in := NewEventMock(nil)
		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Twice()
		destination.On("Send", t.Context(), in).Return(nil).Twice()

		// Trigger via SQLRule's callback (it's the first in the chain)
		collector := newReportCollector()
		sqlRule.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.Len(t, reports, 0)

		// Check same-source linking: SQLRule's next should be ScenarioRule
		nextRunnable := sqlRule.NextRunnable()
		require.NotNil(t, nextRunnable)

		// The second in chain should have no further next
		_, ok := nextRunnable.(*MockScenarioRule)
		require.True(t, ok, "next runnable should be a ScenarioRule")
		require.Nil(t, nextRunnable.NextRunnable())
	})
}

func TestScenarioRule_GetSetMethods(t *testing.T) {
	t.Parallel()

	rule1 := &MockScenarioRule{ID: "rule1"}
	rule2 := &MockScenarioRule{ID: "rule2"}

	t.Run("next/prev circular list linkage", func(t *testing.T) {
		rule1.SetNext(rule2)
		rule2.SetPrev(rule1)

		require.Same(t, rule2, rule1.GetNext())
		require.Same(t, rule1, rule2.GetPrev())
	})

	t.Run("same source linkage", func(t *testing.T) {
		rule1.SetNextSameSource(rule2)
		rule2.SetPrevSameSource(rule1)

		require.Same(t, rule2, rule1.GetNextSameSource())
		require.Same(t, rule1, rule2.GetPrevSameSource())
	})

	t.Run("GetSource returns When", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		rule := &MockScenarioRule{ID: "rule1", When: source}
		require.Same(t, source, rule.GetSource())
	})

	t.Run("GetID returns ID", func(t *testing.T) {
		rule := &MockScenarioRule{ID: "my-rule"}
		require.Equal(t, "my-rule", rule.GetID())
	})

	t.Run("GetEnvironments returns Environments", func(t *testing.T) {
		envs := []string{"prod", "staging"}
		rule := &MockScenarioRule{ID: "rule1", Environments: envs}
		require.Equal(t, envs, rule.GetEnvironments())
	})
}

func TestIsValid_ScenarioRule(t *testing.T) {
	t.Run("empty rule is invalid", func(t *testing.T) {
		rule := &MockScenarioRule{}

		require.Error(t, isValid(rule))
	})

	t.Run("rule missing When is invalid", func(t *testing.T) {
		rule := &MockScenarioRule{
			ID:   "rule",
			When: nil,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}
		require.Error(t, isValid(rule))
	})

	t.Run("rule missing Then is invalid", func(t *testing.T) {
		rule := &MockScenarioRule{
			ID:   "rule",
			When: NewMockSource[*EventMock](t),
			Then: nil,
			To:   &MockDestination[*EventMock]{},
		}
		require.Error(t, isValid(rule))
	})

	t.Run("rule missing To is invalid", func(t *testing.T) {
		rule := &MockScenarioRule{
			ID:   "rule",
			When: NewMockSource[*EventMock](t),
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   nil,
		}
		require.Error(t, isValid(rule))
	})

	t.Run("valid ScenarioRule passes validation", func(t *testing.T) {
		rule := &MockScenarioRule{
			ID:   "valid-rule",
			When: NewMockSource[*EventMock](t),
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}
		require.NoError(t, isValid(rule))
	})
}

func TestScenarioRule_Run_GivenConditionError(t *testing.T) {
	cond := NewMockCondition[*EventMock](t)
	cond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
		Return(false, os.ErrInvalid).Once()

	rule := &MockScenarioRule{
		ID:    "test-rule",
		When:  NewMockSource[*EventMock](t),
		Given: cond,
		Then:  NewMockAction[*EventMock, *EventMock](t),
		To:    NewMockDestination[*EventMock](t),
	}

	_, err := Add(t.Context(), nil, rule)
	require.NoError(t, err)

	err = rule.Run(t.Context(), NewEventMock(nil), nil)
	require.Error(t, err)

	var ruleErr RuleError
	require.ErrorAs(t, err, &ruleErr)
	require.Equal(t, "test-rule", ruleErr.Rule)

	var condErr ConditionError
	require.ErrorAs(t, err, &condErr)
	require.ErrorIs(t, err, os.ErrInvalid)
}

func TestScenarioRule_Run_GivenOutputConditionError(t *testing.T) {
	action := NewMockAction[*EventMock, *EventMock](t)
	postCond := NewMockCondition[*EventMock](t)

	output := NewEventMock(nil)
	action.On("Process", mock.Anything, mock.Anything, mock.Anything).
		Return(output, nil).Once()
	postCond.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
		Return(false, os.ErrInvalid).Once()

	rule := &MockScenarioRule{
		ID:          "test-rule",
		When:        NewMockSource[*EventMock](t),
		Then:        action,
		GivenOutput: postCond,
		To:          NewMockDestination[*EventMock](t),
	}

	_, err := Add(t.Context(), nil, rule)
	require.NoError(t, err)

	err = rule.Run(t.Context(), NewEventMock(nil), nil)
	require.Error(t, err)

	var ruleErr RuleError
	require.ErrorAs(t, err, &ruleErr)
	require.Equal(t, "test-rule", ruleErr.Rule)

	var condErr ConditionError
	require.ErrorAs(t, err, &condErr)
	require.ErrorIs(t, err, os.ErrInvalid)
}
