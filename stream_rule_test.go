package firehose

import (
	"os"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestStreamRuleCallback(t *testing.T) {
	t.Parallel()

	t.Run("successful callback with map and sink", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		mapper := NewMockAction[*EventMock, *EventMock](t)
		sink := NewMockDestination[*EventMock](t)

		rule := &MockStreamRule{
			ID:     "test-rule",
			Source: source,
			Map:    mapper,
			Sink:   sink,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		in := NewEventMock(nil)

		mapper.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		sink.On("Send", t.Context(), in).Return(nil).Once()

		collector := newReportCollector()
		rule.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.Len(t, reports, 0)
	})

	t.Run("callback with map error", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		mapper := NewMockAction[*EventMock, *EventMock](t)
		sink := NewMockDestination[*EventMock](t)

		rule := &MockStreamRule{
			ID:     "test-rule",
			Source: source,
			Map:    mapper,
			Sink:   sink,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		in := NewEventMock(nil)

		mapper.On("Process", t.Context(), in, mock.Anything).Return(in, os.ErrClosed).Once()

		collector := newReportCollector()
		rule.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.Len(t, reports, 1)
		require.ErrorIs(t, reports[0], os.ErrClosed)
	})

	t.Run("callback with sink error", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		mapper := NewMockAction[*EventMock, *EventMock](t)
		sink := NewMockDestination[*EventMock](t)

		rule := &MockStreamRule{
			ID:     "test-rule",
			Source: source,
			Map:    mapper,
			Sink:   sink,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		in := NewEventMock(nil)

		mapper.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		sink.On("Send", t.Context(), in).Return(os.ErrClosed).Once()

		collector := newReportCollector()
		rule.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.Len(t, reports, 1)
		require.ErrorIs(t, reports[0], os.ErrClosed)
	})

	t.Run("callback chains to next rule with same source", func(t *testing.T) {
		in := NewEventMock(nil)
		source := NewMockSource[*EventMock](t)
		mapper := NewMockAction[*EventMock, *EventMock](t)
		sink := NewMockDestination[*EventMock](t)

		rule1 := &MockStreamRule{
			ID:     "rule1",
			Source: source,
			Map:    mapper,
			Sink:   sink,
		}

		rule2 := &MockStreamRule{
			ID:     "rule2",
			Source: source,
			Map:    mapper,
			Sink:   sink,
		}

		head, err := Add(t.Context(), nil, rule1)
		require.NoError(t, err)

		head, err = Add(t.Context(), head, rule2)
		require.NoError(t, err)
		_ = head

		mapper.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Twice()
		sink.On("Send", t.Context(), in).Return(nil).Twice()

		collector := newReportCollector()
		rule1.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.Len(t, reports, 0)
	})

	t.Run("callback with nil reporter function", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		mapper := NewMockAction[*EventMock, *EventMock](t)
		sink := NewMockDestination[*EventMock](t)

		rule := &MockStreamRule{
			ID:     "test-rule",
			Source: source,
			Map:    mapper,
			Sink:   sink,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		in := NewEventMock(nil)

		mapper.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		sink.On("Send", t.Context(), in).Return(nil).Once()

		require.NotPanics(t, func() { rule.callback(t.Context(), in, nil) })
	})
}

func TestStreamRule_NextRunnable(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() *MockStreamRule
		expected bool
	}{
		{
			name: "returns nil when nextSameSource is nil",
			setup: func() *MockStreamRule {
				return &MockStreamRule{ID: "rule1"}
			},
			expected: false,
		},
		{
			name: "returns next runnable when nextSameSource is set",
			setup: func() *MockStreamRule {
				rule1 := &MockStreamRule{ID: "rule1"}
				rule2 := &MockStreamRule{ID: "rule2"}
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

func TestStreamRule_Run(t *testing.T) {
	tests := []struct {
		name            string
		setupMocks      func() (*MockStreamRule, *EventMock)
		expectedReports int
		validateReport  func(t *testing.T, report error)
	}{
		{
			name: "successful map and sink",
			setupMocks: func() (*MockStreamRule, *EventMock) {
				mapper := NewMockAction[*EventMock, *EventMock](t)
				sink := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockStreamRule{
					ID:     "test-rule",
					Source: NewMockSource[*EventMock](t),
					Map:    mapper,
					Sink:   sink,
				}

				_, err := Add(t.Context(), nil, rule)
				require.NoError(t, err)

				mapper.On("Process", mock.Anything, event, mock.Anything).
					Return(event, nil).Once()
				sink.On("Send", mock.Anything, event).
					Return(nil).Once()

				return rule, event
			},
			expectedReports: 0,
			validateReport:  func(t *testing.T, report error) {},
		},
		{
			name: "map error stops sink call",
			setupMocks: func() (*MockStreamRule, *EventMock) {
				mapper := NewMockAction[*EventMock, *EventMock](t)
				sink := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockStreamRule{
					ID:     "test-rule",
					Source: NewMockSource[*EventMock](t),
					Map:    mapper,
					Sink:   sink,
				}

				_, err := Add(t.Context(), nil, rule)
				require.NoError(t, err)

				mapper.On("Process", mock.Anything, event, mock.Anything).
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
			name: "sink error is reported",
			setupMocks: func() (*MockStreamRule, *EventMock) {
				mapper := NewMockAction[*EventMock, *EventMock](t)
				sink := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockStreamRule{
					ID:     "test-rule",
					Source: NewMockSource[*EventMock](t),
					Map:    mapper,
					Sink:   sink,
				}

				_, err := Add(t.Context(), nil, rule)
				require.NoError(t, err)

				mapper.On("Process", mock.Anything, event, mock.Anything).
					Return(event, nil).Once()
				sink.On("Send", mock.Anything, event).
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

func TestStreamRule_Start(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() (*MockStreamRule, *MockSource[*EventMock])
		expectStart bool
		expectError bool
	}{
		{
			name: "starts when prevSameSource is nil (first in chain)",
			setup: func() (*MockStreamRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule := &MockStreamRule{
					ID:     "test-rule",
					Source: source,
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
			setup: func() (*MockStreamRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule1 := &MockStreamRule{ID: "rule1", Source: source}
				rule2 := &MockStreamRule{ID: "rule2", Source: source}
				rule2.SetPrevSameSource(rule1)
				return rule2, source
			},
			expectStart: false,
			expectError: false,
		},
		{
			name: "returns error when source fails to start",
			setup: func() (*MockStreamRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule := &MockStreamRule{
					ID:     "test-rule",
					Source: source,
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

func TestStreamRule_Run_WithFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		setupRule       func() *MockStreamRule
		event           *EventMock
		expectedReports int
		validateReport  func(t *testing.T, report error)
	}{
		{
			name: "Filter fails stops execution",
			setupRule: func() *MockStreamRule {
				filter := NewMockCondition[*EventMock](t)
				filter.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()

				return &MockStreamRule{
					ID:     "test-rule",
					Source: NewMockSource[*EventMock](t),
					Filter: filter,
					Map:    NewMockAction[*EventMock, *EventMock](t),
					Sink:   NewMockDestination[*EventMock](t),
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 1,
			validateReport: func(t *testing.T, report error) {
				require.ErrorIs(t, report, ErrInputNoMatch)
			},
		},
		{
			name: "Filter passes, Map executes",
			setupRule: func() *MockStreamRule {
				filter := NewMockCondition[*EventMock](t)
				mapper := NewMockAction[*EventMock, *EventMock](t)
				sink := NewMockDestination[*EventMock](t)

				filter.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				output := NewEventMock(nil)
				mapper.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(output, nil).Once()
				sink.On("Send", mock.Anything, output).
					Return(nil).Once()

				return &MockStreamRule{
					ID:     "test-rule",
					Source: NewMockSource[*EventMock](t),
					Filter: filter,
					Map:    mapper,
					Sink:   sink,
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 0,
			validateReport:  func(t *testing.T, report error) {},
		},
		{
			name: "FilterOutput fails stops sink",
			setupRule: func() *MockStreamRule {
				mapper := NewMockAction[*EventMock, *EventMock](t)
				outputFilter := NewMockCondition[*EventMock](t)

				output := NewEventMock(nil)
				mapper.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(output, nil).Once()
				outputFilter.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(false, nil).Once()

				return &MockStreamRule{
					ID:           "test-rule",
					Source:       NewMockSource[*EventMock](t),
					Map:          mapper,
					FilterOutput: outputFilter,
					Sink:         NewMockDestination[*EventMock](t),
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 1,
			validateReport: func(t *testing.T, report error) {
				require.ErrorIs(t, report, ErrOutputNoMatch)
			},
		},
		{
			name: "FilterOutput passes, Sink executes",
			setupRule: func() *MockStreamRule {
				mapper := NewMockAction[*EventMock, *EventMock](t)
				outputFilter := NewMockCondition[*EventMock](t)
				sink := NewMockDestination[*EventMock](t)

				output := NewEventMock(nil)
				mapper.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(output, nil).Once()
				outputFilter.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
					Return(true, nil).Once()
				sink.On("Send", mock.Anything, output).
					Return(nil).Once()

				return &MockStreamRule{
					ID:           "test-rule",
					Source:       NewMockSource[*EventMock](t),
					Map:          mapper,
					FilterOutput: outputFilter,
					Sink:         sink,
				}
			},
			event:           NewEventMock(nil),
			expectedReports: 0,
			validateReport:  func(t *testing.T, report error) {},
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

func TestStreamRule_IntegrationWithOtherRules(t *testing.T) {
	t.Parallel()

	t.Run("mixed SQLRule, ScenarioRule, and Stream with same source", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		dest := NewMockDestination[*EventMock](t)

		sqlRule := &MockSQLRule{
			ID:     "sql-rule",
			From:   source,
			Select: action,
			Into:   dest,
		}

		scenarioRule := &MockScenarioRule{
			ID:   "scenario-rule",
			When: source,
			Then: action,
			To:   dest,
		}

		streamRule := &MockStreamRule{
			ID:     "stream-rule",
			Source: source,
			Map:    action,
			Sink:   dest,
		}

		head, err := Add(t.Context(), nil, sqlRule)
		require.NoError(t, err)

		head, err = Add(t.Context(), head, scenarioRule)
		require.NoError(t, err)

		head, err = Add(t.Context(), head, streamRule)
		require.NoError(t, err)
		_ = head

		in := NewEventMock(nil)
		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Times(3)
		dest.On("Send", t.Context(), in).Return(nil).Times(3)

		collector := newReportCollector()
		sqlRule.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.Len(t, reports, 0)

		r1 := sqlRule.NextRunnable()
		require.NotNil(t, r1)
		_, ok1 := r1.(*MockScenarioRule)
		require.True(t, ok1, "second should be ScenarioRule")

		r2 := r1.NextRunnable()
		require.NotNil(t, r2)
		_, ok2 := r2.(*MockStreamRule)
		require.True(t, ok2, "third should be Stream")
		require.Nil(t, r2.NextRunnable())
	})
}

func TestStreamRule_GetSetMethods(t *testing.T) {
	t.Parallel()

	rule1 := &MockStreamRule{ID: "rule1"}
	rule2 := &MockStreamRule{ID: "rule2"}

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

	t.Run("GetSource returns Source", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		rule := &MockStreamRule{ID: "rule1", Source: source}
		require.Same(t, source, rule.GetSource())
	})

	t.Run("GetID returns ID", func(t *testing.T) {
		rule := &MockStreamRule{ID: "my-rule"}
		require.Equal(t, "my-rule", rule.GetID())
	})

	t.Run("GetEnvironments returns Environments", func(t *testing.T) {
		envs := []string{"prod", "staging"}
		rule := &MockStreamRule{ID: "rule1", Environments: envs}
		require.Equal(t, envs, rule.GetEnvironments())
	})
}

func TestIsValid_Stream(t *testing.T) {
	t.Run("empty rule is invalid", func(t *testing.T) {
		rule := &MockStreamRule{}
		require.Error(t, isValid(rule))
	})

	t.Run("rule missing Source is invalid", func(t *testing.T) {
		rule := &MockStreamRule{
			ID:     "rule",
			Source: nil,
			Map:    &MockAction[*EventMock, *EventMock]{},
			Sink:   &MockDestination[*EventMock]{},
		}
		require.Error(t, isValid(rule))
	})

	t.Run("rule missing Map is invalid", func(t *testing.T) {
		rule := &MockStreamRule{
			ID:     "rule",
			Source: NewMockSource[*EventMock](t),
			Map:    nil,
			Sink:   &MockDestination[*EventMock]{},
		}
		require.Error(t, isValid(rule))
	})

	t.Run("rule missing Sink is invalid", func(t *testing.T) {
		rule := &MockStreamRule{
			ID:     "rule",
			Source: NewMockSource[*EventMock](t),
			Map:    &MockAction[*EventMock, *EventMock]{},
			Sink:   nil,
		}
		require.Error(t, isValid(rule))
	})

	t.Run("valid Stream passes validation", func(t *testing.T) {
		rule := &MockStreamRule{
			ID:     "valid-rule",
			Source: NewMockSource[*EventMock](t),
			Map:    &MockAction[*EventMock, *EventMock]{},
			Sink:   &MockDestination[*EventMock]{},
		}
		require.NoError(t, isValid(rule))
	})
}

func TestStreamRule_Run_FilterConditionError(t *testing.T) {
	filter := NewMockCondition[*EventMock](t)
	filter.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
		Return(false, os.ErrInvalid).Once()

	rule := &MockStreamRule{
		ID:     "test-rule",
		Source: NewMockSource[*EventMock](t),
		Filter: filter,
		Map:    NewMockAction[*EventMock, *EventMock](t),
		Sink:   NewMockDestination[*EventMock](t),
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

func TestStreamRule_Run_FilterOutputConditionError(t *testing.T) {
	mapper := NewMockAction[*EventMock, *EventMock](t)
	outputFilter := NewMockCondition[*EventMock](t)

	output := NewEventMock(nil)
	mapper.On("Process", mock.Anything, mock.Anything, mock.Anything).
		Return(output, nil).Once()
	outputFilter.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
		Return(false, os.ErrInvalid).Once()

	rule := &MockStreamRule{
		ID:           "test-rule",
		Source:       NewMockSource[*EventMock](t),
		Map:          mapper,
		FilterOutput: outputFilter,
		Sink:         NewMockDestination[*EventMock](t),
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
