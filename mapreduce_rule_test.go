package firehose

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestMapReduceRuleCallback(t *testing.T) {
	t.Parallel()

	t.Run("successful callback with map, reduce, and sink", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		mapper := NewMockAction[*EventMock, *EventMock](t)
		reducer := NewMockReducer[*EventMock, *EventMock](t)
		sink := NewMockDestination[*EventMock](t)

		rule := &MockMapReduceRule{
			ID:     "test-rule",
			Source: source,
			Map:    mapper,
			Reduce: reducer,
			Sink:   sink,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		in := NewEventMock(nil)
		out := NewEventMock(nil)

		mapper.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		reducer.On("Reduce", t.Context(), in, mock.Anything).Return(out, nil).Once()
		sink.On("Send", t.Context(), out).Return(nil).Once()

		collector := newReportCollector()
		rule.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.Len(t, reports, 0)
	})

	t.Run("callback with map error", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		mapper := NewMockAction[*EventMock, *EventMock](t)
		reducer := NewMockReducer[*EventMock, *EventMock](t)
		sink := NewMockDestination[*EventMock](t)

		rule := &MockMapReduceRule{
			ID:     "test-rule",
			Source: source,
			Map:    mapper,
			Reduce: reducer,
			Sink:   sink,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		in := NewEventMock(nil)

		mapper.On("Process", t.Context(), in, mock.Anything).
			Return(nil, os.ErrClosed).Once()

		collector := newReportCollector()
		rule.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.Len(t, reports, 1)
		require.ErrorIs(t, reports[0], os.ErrClosed)
	})

	t.Run("callback with reduce error", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		mapper := NewMockAction[*EventMock, *EventMock](t)
		reducer := NewMockReducer[*EventMock, *EventMock](t)
		sink := NewMockDestination[*EventMock](t)

		rule := &MockMapReduceRule{
			ID:     "test-rule",
			Source: source,
			Map:    mapper,
			Reduce: reducer,
			Sink:   sink,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		in := NewEventMock(nil)

		mapper.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		reducer.On("Reduce", t.Context(), in, mock.Anything).
			Return(nil, os.ErrClosed).Once()

		collector := newReportCollector()
		rule.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.Len(t, reports, 1)
		require.ErrorIs(t, reports[0], os.ErrClosed)
	})

	t.Run("callback with sink error", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		mapper := NewMockAction[*EventMock, *EventMock](t)
		reducer := NewMockReducer[*EventMock, *EventMock](t)
		sink := NewMockDestination[*EventMock](t)

		rule := &MockMapReduceRule{
			ID:     "test-rule",
			Source: source,
			Map:    mapper,
			Reduce: reducer,
			Sink:   sink,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		in := NewEventMock(nil)
		out := NewEventMock(nil)

		mapper.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		reducer.On("Reduce", t.Context(), in, mock.Anything).Return(out, nil).Once()
		sink.On("Send", t.Context(), out).Return(os.ErrClosed).Once()

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
		reducer := NewMockReducer[*EventMock, *EventMock](t)
		sink := NewMockDestination[*EventMock](t)
		out := NewEventMock(nil)

		rule1 := &MockMapReduceRule{
			ID:     "rule1",
			Source: source,
			Map:    mapper,
			Reduce: reducer,
			Sink:   sink,
		}

		rule2 := &MockMapReduceRule{
			ID:     "rule2",
			Source: source,
			Map:    mapper,
			Reduce: reducer,
			Sink:   sink,
		}

		head, err := Add(t.Context(), nil, rule1)
		require.NoError(t, err)

		head, err = Add(t.Context(), head, rule2)
		require.NoError(t, err)
		_ = head

		mapper.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Twice()
		reducer.On("Reduce", t.Context(), in, mock.Anything).Return(out, nil).Twice()
		sink.On("Send", t.Context(), out).Return(nil).Twice()

		collector := newReportCollector()
		rule1.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.Len(t, reports, 0)
	})

	t.Run("callback with nil reporter function", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		mapper := NewMockAction[*EventMock, *EventMock](t)
		reducer := NewMockReducer[*EventMock, *EventMock](t)
		sink := NewMockDestination[*EventMock](t)

		rule := &MockMapReduceRule{
			ID:     "test-rule",
			Source: source,
			Map:    mapper,
			Reduce: reducer,
			Sink:   sink,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		in := NewEventMock(nil)
		out := NewEventMock(nil)

		mapper.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Once()
		reducer.On("Reduce", t.Context(), in, mock.Anything).Return(out, nil).Once()
		sink.On("Send", t.Context(), out).Return(nil).Once()

		require.NotPanics(t, func() { rule.callback(t.Context(), in, nil) })
	})
}

func TestMapReduceRule_NextRunnable(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() *MockMapReduceRule
		expected bool
	}{
		{
			name: "returns nil when nextSameSource is nil",
			setup: func() *MockMapReduceRule {
				return &MockMapReduceRule{ID: "rule1"}
			},
			expected: false,
		},
		{
			name: "returns next runnable when nextSameSource is set",
			setup: func() *MockMapReduceRule {
				rule1 := &MockMapReduceRule{ID: "rule1"}
				rule2 := &MockMapReduceRule{ID: "rule2"}
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

func TestMapReduceRule_Run(t *testing.T) {
	tests := []struct {
		name            string
		setupMocks      func() (*MockMapReduceRule, *EventMock)
		expectedReports int
		validateReport  func(t *testing.T, report error)
	}{
		{
			name: "successful map, reduce, and sink",
			setupMocks: func() (*MockMapReduceRule, *EventMock) {
				mapper := NewMockAction[*EventMock, *EventMock](t)
				reducer := NewMockReducer[*EventMock, *EventMock](t)
				sink := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)
				output := NewEventMock(nil)

				rule := &MockMapReduceRule{
					ID:     "test-rule",
					Source: NewMockSource[*EventMock](t),
					Map:    mapper,
					Reduce: reducer,
					Sink:   sink,
				}

				_, err := Add(t.Context(), nil, rule)
				require.NoError(t, err)

				mapper.On("Process", mock.Anything, event, mock.Anything).
					Return(event, nil).Once()
				reducer.On("Reduce", mock.Anything, event, mock.Anything).
					Return(output, nil).Once()
				sink.On("Send", mock.Anything, output).
					Return(nil).Once()

				return rule, event
			},
			expectedReports: 0,
			validateReport:  func(t *testing.T, report error) {},
		},
		{
			name: "map error stops processing",
			setupMocks: func() (*MockMapReduceRule, *EventMock) {
				mapper := NewMockAction[*EventMock, *EventMock](t)
				reducer := NewMockReducer[*EventMock, *EventMock](t)
				sink := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockMapReduceRule{
					ID:     "test-rule",
					Source: NewMockSource[*EventMock](t),
					Map:    mapper,
					Reduce: reducer,
					Sink:   sink,
				}

				_, err := Add(t.Context(), nil, rule)
				require.NoError(t, err)

				mapper.On("Process", mock.Anything, event, mock.Anything).
					Return(nil, os.ErrClosed).Once()

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
			name: "reduce error is reported",
			setupMocks: func() (*MockMapReduceRule, *EventMock) {
				mapper := NewMockAction[*EventMock, *EventMock](t)
				reducer := NewMockReducer[*EventMock, *EventMock](t)
				sink := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)

				rule := &MockMapReduceRule{
					ID:     "test-rule",
					Source: NewMockSource[*EventMock](t),
					Map:    mapper,
					Reduce: reducer,
					Sink:   sink,
				}

				_, err := Add(t.Context(), nil, rule)
				require.NoError(t, err)

				mapper.On("Process", mock.Anything, event, mock.Anything).
					Return(event, nil).Once()
				reducer.On("Reduce", mock.Anything, event, mock.Anything).
					Return(nil, os.ErrClosed).Once()

				return rule, event
			},
			expectedReports: 1,
			validateReport: func(t *testing.T, report error) {
				var reduceErr ReduceError
				require.ErrorAs(t, report, &reduceErr)
				require.ErrorIs(t, report, os.ErrClosed)
			},
		},
		{
			name: "sink error is reported",
			setupMocks: func() (*MockMapReduceRule, *EventMock) {
				mapper := NewMockAction[*EventMock, *EventMock](t)
				reducer := NewMockReducer[*EventMock, *EventMock](t)
				sink := NewMockDestination[*EventMock](t)
				event := NewEventMock(nil)
				output := NewEventMock(nil)

				rule := &MockMapReduceRule{
					ID:     "test-rule",
					Source: NewMockSource[*EventMock](t),
					Map:    mapper,
					Reduce: reducer,
					Sink:   sink,
				}

				_, err := Add(t.Context(), nil, rule)
				require.NoError(t, err)

				mapper.On("Process", mock.Anything, event, mock.Anything).
					Return(event, nil).Once()
				reducer.On("Reduce", mock.Anything, event, mock.Anything).
					Return(output, nil).Once()
				sink.On("Send", mock.Anything, output).
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

func TestMapReduceRule_Accumulator(t *testing.T) {
	t.Parallel()

	t.Run("accumulates across multiple run calls", func(t *testing.T) {
		mapper := NewMockAction[*EventMock, *EventMock](t)
		reducer := NewMockReducer[*EventMock, *EventMock](t)
		sink := NewMockDestination[*EventMock](t)

		rule := &MockMapReduceRule{
			ID:     "test-rule",
			Source: NewMockSource[*EventMock](t),
			Map:    mapper,
			Reduce: reducer,
			Sink:   sink,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		event1 := NewEventMock(map[string]any{"val": 1})
		event2 := NewEventMock(map[string]any{"val": 2})
		inter1 := NewEventMock(map[string]any{"m": 1})
		inter2 := NewEventMock(map[string]any{"m": 2})
		accum1 := NewEventMock(map[string]any{"sum": 1})
		accum2 := NewEventMock(map[string]any{"sum": 3})

		mapper.On("Process", mock.Anything, event1, mock.Anything).
			Return(inter1, nil).Once()
		mapper.On("Process", mock.Anything, event2, mock.Anything).
			Return(inter2, nil).Once()

		reducer.On("Reduce", mock.Anything, inter1, mock.Anything).
			Return(accum1, nil).Once()
		// Second reduce should receive accum1 as accumulator
		reducer.On("Reduce", mock.Anything, inter2, accum1).
			Return(accum2, nil).Once()

		sink.On("Send", mock.Anything, accum1).Return(nil).Once()
		sink.On("Send", mock.Anything, accum2).Return(nil).Once()

		require.NoError(t, rule.Run(t.Context(), event1, nil))
		require.NoError(t, rule.Run(t.Context(), event2, nil))
	})

	t.Run("accumulator is updated even when FilterOutput fails", func(t *testing.T) {
		mapper := NewMockAction[*EventMock, *EventMock](t)
		reducer := NewMockReducer[*EventMock, *EventMock](t)
		outputFilter := NewMockCondition[*EventMock](t)

		rule := &MockMapReduceRule{
			ID:           "test-rule",
			Source:       NewMockSource[*EventMock](t),
			Map:          mapper,
			Reduce:       reducer,
			FilterOutput: outputFilter,
			Sink:         NewMockDestination[*EventMock](t),
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		event := NewEventMock(nil)
		inter := NewEventMock(map[string]any{"m": 1})
		accum := NewEventMock(map[string]any{"sum": 1})

		mapper.On("Process", mock.Anything, event, mock.Anything).
			Return(inter, nil).Once()
		reducer.On("Reduce", mock.Anything, inter, mock.Anything).
			Return(accum, nil).Once()
		outputFilter.On("Evaluate", mock.Anything, accum, mock.Anything).
			Return(false, nil).Once()
		outputFilter.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
			Return(false, nil).Once()

		err = rule.Run(t.Context(), event, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrOutputNoMatch)

		// Second event receives the updated accumulator (accum) because
		// accumulator is updated before FilterOutput check
		event2 := NewEventMock(nil)
		inter2 := NewEventMock(map[string]any{"m": 2})
		accum2 := NewEventMock(map[string]any{"sum": 2})

		mapper.On("Process", mock.Anything, event2, mock.Anything).
			Return(inter2, nil).Once()
		reducer.On("Reduce", mock.Anything, inter2, accum).
			Return(accum2, nil).Once()

		err = rule.Run(t.Context(), event2, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrOutputNoMatch)
	})
}

func TestMapReduceRule_Start(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() (*MockMapReduceRule, *MockSource[*EventMock])
		expectStart bool
		expectError bool
	}{
		{
			name: "starts when prevSameSource is nil (first in chain)",
			setup: func() (*MockMapReduceRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule := &MockMapReduceRule{
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
			setup: func() (*MockMapReduceRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule1 := &MockMapReduceRule{ID: "rule1", Source: source}
				rule2 := &MockMapReduceRule{ID: "rule2", Source: source}
				rule2.SetPrevSameSource(rule1)
				return rule2, source
			},
			expectStart: false,
			expectError: false,
		},
		{
			name: "returns error when source fails to start",
			setup: func() (*MockMapReduceRule, *MockSource[*EventMock]) {
				source := NewMockSource[*EventMock](t)
				rule := &MockMapReduceRule{
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

func TestMapReduceRule_Run_WithFilters(t *testing.T) {
	t.Parallel()

	t.Run("Filter fails stops execution", func(t *testing.T) {
		filter := NewMockCondition[*EventMock](t)
		filter.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
			Return(false, nil).Once()

		rule := &MockMapReduceRule{
			ID:     "test-rule",
			Source: NewMockSource[*EventMock](t),
			Filter: filter,
			Map:    NewMockAction[*EventMock, *EventMock](t),
			Reduce: NewMockReducer[*EventMock, *EventMock](t),
			Sink:   NewMockDestination[*EventMock](t),
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		err = rule.Run(t.Context(), NewEventMock(nil), nil)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInputNoMatch)
	})

	t.Run("Filter passes, processing continues", func(t *testing.T) {
		filter := NewMockCondition[*EventMock](t)
		mapper := NewMockAction[*EventMock, *EventMock](t)
		reducer := NewMockReducer[*EventMock, *EventMock](t)
		sink := NewMockDestination[*EventMock](t)
		output := NewEventMock(nil)

		filter.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
			Return(true, nil).Once()
		mapper.On("Process", mock.Anything, mock.Anything, mock.Anything).
			Return(NewEventMock(nil), nil).Once()
		reducer.On("Reduce", mock.Anything, mock.Anything, mock.Anything).
			Return(output, nil).Once()
		sink.On("Send", mock.Anything, output).Return(nil).Once()

		rule := &MockMapReduceRule{
			ID:     "test-rule",
			Source: NewMockSource[*EventMock](t),
			Filter: filter,
			Map:    mapper,
			Reduce: reducer,
			Sink:   sink,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		require.NoError(t, rule.Run(t.Context(), NewEventMock(nil), nil))
	})

	t.Run("FilterOutput fails stops sink", func(t *testing.T) {
		mapper := NewMockAction[*EventMock, *EventMock](t)
		reducer := NewMockReducer[*EventMock, *EventMock](t)
		outputFilter := NewMockCondition[*EventMock](t)
		output := NewEventMock(nil)

		mapper.On("Process", mock.Anything, mock.Anything, mock.Anything).
			Return(NewEventMock(nil), nil).Once()
		reducer.On("Reduce", mock.Anything, mock.Anything, mock.Anything).
			Return(output, nil).Once()
		outputFilter.On("Evaluate", mock.Anything, output, mock.Anything).
			Return(false, nil).Once()

		rule := &MockMapReduceRule{
			ID:           "test-rule",
			Source:       NewMockSource[*EventMock](t),
			Map:          mapper,
			Reduce:       reducer,
			FilterOutput: outputFilter,
			Sink:         NewMockDestination[*EventMock](t),
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		err = rule.Run(t.Context(), NewEventMock(nil), nil)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrOutputNoMatch)
	})

	t.Run("FilterOutput passes, sink executes", func(t *testing.T) {
		mapper := NewMockAction[*EventMock, *EventMock](t)
		reducer := NewMockReducer[*EventMock, *EventMock](t)
		outputFilter := NewMockCondition[*EventMock](t)
		sink := NewMockDestination[*EventMock](t)
		output := NewEventMock(nil)

		mapper.On("Process", mock.Anything, mock.Anything, mock.Anything).
			Return(NewEventMock(nil), nil).Once()
		reducer.On("Reduce", mock.Anything, mock.Anything, mock.Anything).
			Return(output, nil).Once()
		outputFilter.On("Evaluate", mock.Anything, output, mock.Anything).
			Return(true, nil).Once()
		sink.On("Send", mock.Anything, output).Return(nil).Once()

		rule := &MockMapReduceRule{
			ID:           "test-rule",
			Source:       NewMockSource[*EventMock](t),
			Map:          mapper,
			Reduce:       reducer,
			FilterOutput: outputFilter,
			Sink:         sink,
		}

		_, err := Add(t.Context(), nil, rule)
		require.NoError(t, err)

		require.NoError(t, rule.Run(t.Context(), NewEventMock(nil), nil))
	})
}

func TestMapReduceRule_ConcurrentAccumulator(t *testing.T) {
	mapper := NewMockAction[*EventMock, *EventMock](t)
	reducer := NewMockReducer[*EventMock, *EventMock](t)
	sink := NewMockDestination[*EventMock](t)

	rule := &MockMapReduceRule{
		ID:     "test-rule",
		Source: NewMockSource[*EventMock](t),
		Map:    mapper,
		Reduce: reducer,
		Sink:   sink,
	}

	_, err := Add(t.Context(), nil, rule)
	require.NoError(t, err)

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			event := NewEventMock(nil)
			inter := NewEventMock(nil)
			out := NewEventMock(nil)

			mapper.On("Process", mock.Anything, event, mock.Anything).
				Return(inter, nil).Once()
			reducer.On("Reduce", mock.Anything, inter, mock.Anything).
				Return(out, nil).Once()
			sink.On("Send", mock.Anything, out).Return(nil).Once()

			require.NoError(t, rule.Run(t.Context(), event, nil))
		}()
	}

	wg.Wait()
}

func TestMapReduceRule_IntegrationWithOtherRules(t *testing.T) {
	t.Parallel()

	t.Run("mixed with SQLRule and ScenarioRule and same source", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		dest := NewMockDestination[*EventMock](t)
		reducer := NewMockReducer[*EventMock, *EventMock](t)
		out := NewEventMock(nil)

		head, err := Add(t.Context(), nil, &MockSQLRule{
			ID:     "sql-rule",
			From:   source,
			Select: action,
			Into:   dest,
		})
		require.NoError(t, err)

		head, err = Add(t.Context(), head, &MockScenarioRule{
			ID:   "scenario-rule",
			Give: source,
			Then: action,
			To:   dest,
		})
		require.NoError(t, err)

		head, err = Add(t.Context(), head, &MockMapReduceRule{
			ID:     "mr-rule",
			Source: source,
			Map:    action,
			Reduce: reducer,
			Sink:   dest,
		})
		require.NoError(t, err)
		_ = head

		in := NewEventMock(nil)
		action.On("Process", t.Context(), in, mock.Anything).Return(in, nil).Times(3)
		reducer.On("Reduce", t.Context(), in, mock.Anything).Return(out, nil).Once()
		dest.On("Send", t.Context(), in).Return(nil).Times(2)
		dest.On("Send", t.Context(), out).Return(nil).Once()

		f := head.(*MockSQLRule)
		collector := newReportCollector()
		f.callback(t.Context(), in, collector.Collect)
		reports := collector.Errors()
		require.Len(t, reports, 0)
	})
}

func TestMapReduceRule_GetSetMethods(t *testing.T) {
	t.Parallel()

	rule1 := &MockMapReduceRule{ID: "rule1"}
	rule2 := &MockMapReduceRule{ID: "rule2"}

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
		rule := &MockMapReduceRule{ID: "rule1", Source: source}
		require.Same(t, source, rule.GetSource())
	})

	t.Run("GetID returns ID", func(t *testing.T) {
		rule := &MockMapReduceRule{ID: "my-rule"}
		require.Equal(t, "my-rule", rule.GetID())
	})

	t.Run("GetEnvironments returns Environments", func(t *testing.T) {
		envs := []string{"prod", "staging"}
		rule := &MockMapReduceRule{ID: "rule1", Environments: envs}
		require.Equal(t, envs, rule.GetEnvironments())
	})
}

func TestIsValid_MapReduceRule(t *testing.T) {
	t.Run("empty rule is invalid", func(t *testing.T) {
		rule := &MockMapReduceRule{}
		require.Error(t, isValid(rule))
	})

	t.Run("rule missing Source is invalid", func(t *testing.T) {
		rule := &MockMapReduceRule{
			ID:     "rule",
			Source: nil,
			Map:    &MockAction[*EventMock, *EventMock]{},
			Reduce: NewMockReducer[*EventMock, *EventMock](t),
			Sink:   &MockDestination[*EventMock]{},
		}
		require.Error(t, isValid(rule))
	})

	t.Run("rule missing Map is invalid", func(t *testing.T) {
		rule := &MockMapReduceRule{
			ID:     "rule",
			Source: NewMockSource[*EventMock](t),
			Map:    nil,
			Reduce: NewMockReducer[*EventMock, *EventMock](t),
			Sink:   &MockDestination[*EventMock]{},
		}
		require.Error(t, isValid(rule))
	})

	t.Run("rule missing Reduce is invalid", func(t *testing.T) {
		rule := &MockMapReduceRule{
			ID:     "rule",
			Source: NewMockSource[*EventMock](t),
			Map:    &MockAction[*EventMock, *EventMock]{},
			Reduce: nil,
			Sink:   &MockDestination[*EventMock]{},
		}
		require.Error(t, isValid(rule))
	})

	t.Run("rule missing Sink is invalid", func(t *testing.T) {
		rule := &MockMapReduceRule{
			ID:     "rule",
			Source: NewMockSource[*EventMock](t),
			Map:    &MockAction[*EventMock, *EventMock]{},
			Reduce: NewMockReducer[*EventMock, *EventMock](t),
			Sink:   nil,
		}
		require.Error(t, isValid(rule))
	})

	t.Run("valid MapReduceRule passes validation", func(t *testing.T) {
		rule := &MockMapReduceRule{
			ID:     "valid-rule",
			Source: NewMockSource[*EventMock](t),
			Map:    &MockAction[*EventMock, *EventMock]{},
			Reduce: NewMockReducer[*EventMock, *EventMock](t),
			Sink:   &MockDestination[*EventMock]{},
		}
		require.NoError(t, isValid(rule))
	})
}
