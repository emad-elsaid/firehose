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
			When: source,
			Then: action,
			To:   destination,
		}

		in := NewMockAttributer(t)

		in.On("Attributes", t.Context()).Return(map[string]any{}, nil).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Once()
		destination.On("Send", t.Context(), in).Return(NewReport(StatusSuccess, nil)).Once()

		reportsChan := make(chan Report, 10)
		rule.callback(t.Context(), in, reportsChan)
		close(reportsChan)
		reports := chanToSlice(reportsChan)
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
			When: source,
			Then: action,
			To:   destination,
		}
		in := NewMockAttributer(t)

		in.On("Attributes", t.Context()).Return(map[string]any{}, nil).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusActionError, os.ErrClosed)).Once()

		reportsChan := make(chan Report, 10)
		rule.callback(t.Context(), in, reportsChan)
		close(reportsChan)
		reports := chanToSlice(reportsChan)
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
			When: source,
			Then: action,
			To:   destination,
		}
		in := NewMockAttributer(t)

		in.On("Attributes", t.Context()).Return(map[string]any{}, nil).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Once()
		destination.On("Send", t.Context(), in).Return(NewReport(StatusDestinationError, os.ErrClosed)).Once()

		reportsChan := make(chan Report, 10)
		rule.callback(t.Context(), in, reportsChan)
		close(reportsChan)
		reports := chanToSlice(reportsChan)
		require.NotNil(t, reports)
		require.Len(t, reports, 1)

		for _, report := range reports {
			require.ErrorIs(t, report.Err, os.ErrClosed)
		}
	})

	t.Run("callback chains to next rule with same source", func(t *testing.T) {
		in := NewMockAttributer(t)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule1 := &MockRule{
			ID:   "rule1",
			When: source,
			Then: action,
			To:   destination,
		}

		rule2 := &MockRule{
			ID:   "rule2",
			When: source,
			Then: action,
			To:   destination,
		}

		registry, err := AddRule(t.Context(), nil, rule1, nil, nil, nil, in, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, nil, nil, nil, in, in)
		require.NoError(t, err)

		in.On("Attributes", t.Context()).Return(map[string]any{}, nil).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Twice()
		destination.On("Send", t.Context(), in).Return(NewReport(StatusSuccess, nil)).Twice()

		reportsChan := make(chan Report, 10)
		rule1.callback(t.Context(), in, reportsChan)
		close(reportsChan)
		reports := chanToSlice(reportsChan)
		require.NotNil(t, reports)
		require.Len(t, reports, 2)

		for _, report := range reports {
			require.NoError(t, report.Err, in)
		}
	})

	t.Run("callback chain continue on action error in first rule", func(t *testing.T) {
		in := NewMockAttributer(t)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule1 := &MockRule{
			ID:   "rule1",
			When: source,
			Then: action,
			To:   destination,
		}

		rule2 := &MockRule{
			ID:   "rule2",
			When: source,
			Then: action,
			To:   destination,
		}

		registry, err := AddRule(t.Context(), nil, rule1, nil, nil, nil, in, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, nil, nil, nil, in, in)
		require.NoError(t, err)

		in.On("Attributes", t.Context()).Return(map[string]any{}, nil).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusActionError, os.ErrClosed)).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Once()
		destination.On("Send", t.Context(), in).Return(NewReport(StatusSuccess, nil)).Once()

		reportsChan := make(chan Report, 10)
		rule1.callback(t.Context(), in, reportsChan)
		close(reportsChan)
		reports := chanToSlice(reportsChan)
		require.NotNil(t, reports)
		require.Len(t, reports, 2)
		require.ErrorIs(t, reports[0].Err, os.ErrClosed)
		require.NoError(t, reports[1].Err)
	})

	t.Run("callback chain propagates error from second rule", func(t *testing.T) {
		in := NewMockAttributer(t)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule1 := &MockRule{
			ID:   "rule1",
			When: source,
			Then: action,
			To:   destination,
		}

		rule2 := &MockRule{
			ID:   "rule2",
			When: source,
			Then: action,
			To:   destination,
		}

		registry, err := AddRule(t.Context(), nil, rule1, nil, nil, nil, in, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, nil, nil, nil, in, in)
		require.NoError(t, err)

		in.On("Attributes", t.Context()).Return(map[string]any{}, nil).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusActionError, os.ErrClosed)).Once()
		destination.On("Send", t.Context(), in).Return(NewReport(StatusSuccess, nil)).Once()

		reportsChan := make(chan Report, 10)
		rule1.callback(t.Context(), in, reportsChan)
		close(reportsChan)
		reports := chanToSlice(reportsChan)
		require.NotNil(t, reports)
		require.Len(t, reports, 2)
		require.NoError(t, reports[0].Err)
		require.ErrorIs(t, reports[1].Err, os.ErrClosed)
	})

	t.Run("callback with three rules in chain", func(t *testing.T) {
		in := NewMockAttributer(t)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule1 := &MockRule{
			ID:   "rule1",
			When: source,
			Then: action,
			To:   destination,
		}

		rule2 := &MockRule{
			ID:   "rule2",
			When: source,
			Then: action,
			To:   destination,
		}

		rule3 := &MockRule{
			ID:   "rule3",
			When: source,
			Then: action,
			To:   destination,
		}

		registry, err := AddRule(t.Context(), nil, rule1, nil, nil, nil, in, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule2, nil, nil, nil, in, in)
		require.NoError(t, err)

		registry, err = AddRule(t.Context(), registry, rule3, nil, nil, nil, in, in)
		require.NoError(t, err)

		in.On("Attributes", t.Context()).Return(map[string]any{}, nil).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Times(3)
		destination.On("Send", t.Context(), in).Return(NewReport(StatusSuccess, nil)).Times(3)

		reportsChan := make(chan Report, 10)
		rule1.callback(t.Context(), in, reportsChan)
		close(reportsChan)
		reports := chanToSlice(reportsChan)
		require.NotNil(t, reports)
		require.Len(t, reports, 3)

		for _, report := range reports {
			require.NoError(t, report.Err)
		}
	})

	t.Run("callback with incompatible next rule type", func(t *testing.T) {
		in := NewMockAttributer(t)
		source := NewMockSource[*EventMock](t)
		action := NewMockAction[*EventMock, *EventMock](t)
		destination := NewMockDestination[*EventMock](t)

		rule := &MockRule{
			When: source,
			Then: action,
			To:   destination,
		}

		in.On("Attributes", t.Context()).Return(map[string]any{}, nil).Once()
		action.On("Process", t.Context(), in, mock.Anything).Return(in, NewReport(StatusSuccess, nil)).Once()
		destination.On("Send", t.Context(), in).Return(NewReport(StatusSuccess, nil)).Once()

		// Create a Rule with a different type (string instead of *EventMock)
		// This will cause a panic when type-asserting to Runnable[*EventMock]
		incompatibleRule := &Rule[string, string]{}
		incompatibleSourceRegistry := newMocksourceRegistry(t)
		incompatibleSourceRegistry.On("getRegistry").Return(incompatibleRule).Once()
		rule.nextSameSource = incompatibleSourceRegistry

		reportsChan := make(chan Report, 10)
		require.Panics(t, func() { rule.callback(t.Context(), in, reportsChan) })
		close(reportsChan)
		reports := chanToSlice(reportsChan)
		require.NotNil(t, reports)
		require.Len(t, reports, 1)

	})
}

func chanToSlice[T any](ch <-chan T) []T {
	var result []T
	for v := range ch {
		result = append(result, v)
	}

	return result
}

func TestRuleActionOverride(t *testing.T) {
	source := NewMockSource[*MockAttributer](t)
	oldAction := NewMockAction[*MockAttributer, *MockAttributer](t)
	newAction := NewMockAction[*MockAttributer, *MockAttributer](t)
	destination := NewMockDestination[*MockAttributer](t)
	event := NewMockAttributer(t)

	rule := &Rule[*MockAttributer, *MockAttributer]{
		ID:   "test-rule",
		When: source,
		Then: oldAction,
		To:   destination,
	}

	// Register the rule
	_, err := AddRule(t.Context(), nil, rule, nil, nil, nil, event, event)
	require.NoError(t, err)

	// Override the action after registration
	rule.Then = newAction

	event.On("Attributes", t.Context()).Return(map[string]any{}, nil).Once()
	newAction.On("Process", t.Context(), event, mock.Anything).
		Return(event, NewReport(StatusSuccess, nil)).Once()
	destination.On("Send", t.Context(), event).
		Return(NewReport(StatusSuccess, nil)).Once()

	reportsChan := make(chan Report, 10)
	rule.callback(t.Context(), event, reportsChan)
	close(reportsChan)

	reports := chanToSlice(reportsChan)
	require.NotNil(t, reports)
	require.Len(t, reports, 1)
	require.NoError(t, reports[0].Err)
}

func TestRuleDestinationOverride(t *testing.T) {
	source := NewMockSource[*MockAttributer](t)
	action := NewMockAction[*MockAttributer, *MockAttributer](t)
	oldDestination := NewMockDestination[*MockAttributer](t)
	newDestination := NewMockDestination[*MockAttributer](t)
	event := NewMockAttributer(t)

	rule := &Rule[*MockAttributer, *MockAttributer]{
		ID:   "test-rule",
		When: source,
		Then: action,
		To:   oldDestination,
	}

	// Register the rule
	_, err := AddRule(t.Context(), nil, rule, nil, nil, nil, event, event)
	require.NoError(t, err)

	// Override the destination after registration
	rule.To = newDestination

	event.On("Attributes", t.Context()).Return(map[string]any{}, nil).Once()
	action.On("Process", t.Context(), event, mock.Anything).
		Return(event, NewReport(StatusSuccess, nil)).Once()
	newDestination.On("Send", t.Context(), event).
		Return(NewReport(StatusSuccess, nil)).Once()

	reportsChan := make(chan Report, 10)
	rule.callback(t.Context(), event, reportsChan)
	close(reportsChan)

	reports := chanToSlice(reportsChan)
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
				event := NewMockAttributer(t)

				rule := &MockRule{
					ID:   "test-rule",
					Then: action,
					To:   destination,
				}

				action.On("Process", mock.Anything, event, mock.Anything).
					Return(event, NewReport(StatusSuccess, nil)).Once()
				destination.On("Send", mock.Anything, event).
					Return(NewReport(StatusSuccess, nil)).Once()

				return rule, event
			},
			expectedReports: 1,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				require.NoError(t, report.Err)
				require.Equal(t, StatusSuccess, report.Status)
			},
		},
		{
			name: "action error stops destination call",
			setupMocks: func() (*MockRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewMockAttributer(t)

				rule := &MockRule{
					ID:   "test-rule",
					Then: action,
					To:   destination,
				}

				action.On("Process", mock.Anything, event, mock.Anything).
					Return(event, NewReport(StatusActionError, os.ErrClosed)).Once()

				return rule, event
			},
			expectedReports: 1,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				require.ErrorIs(t, report.Err, os.ErrClosed)
				require.Equal(t, StatusActionError, report.Status)
			},
		},
		{
			name: "abort stops destination call",
			setupMocks: func() (*MockRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewMockAttributer(t)

				rule := &MockRule{
					ID:   "test-rule",
					Then: action,
					To:   destination,
				}

				abortReport := NewAbortReport(StatusError, os.ErrInvalid)
				action.On("Process", mock.Anything, event, mock.Anything).
					Return(event, abortReport).Once()

				return rule, event
			},
			expectedReports: 1,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				require.ErrorIs(t, report.Err, os.ErrInvalid)
				require.True(t, report.Abort)
			},
		},
		{
			name: "destination error is reported",
			setupMocks: func() (*MockRule, *EventMock) {
				action := NewMockAction[*EventMock, *EventMock](t)
				destination := NewMockDestination[*EventMock](t)
				event := NewMockAttributer(t)

				rule := &MockRule{
					ID:   "test-rule",
					Then: action,
					To:   destination,
				}

				action.On("Process", mock.Anything, event, mock.Anything).
					Return(event, NewReport(StatusSuccess, nil)).Once()
				destination.On("Send", mock.Anything, event).
					Return(NewReport(StatusDestinationError, os.ErrPermission)).Once()

				return rule, event
			},
			expectedReports: 1,
			validateReport: func(t *testing.T, report Report) {
				require.Equal(t, "test-rule", report.Rule)
				require.ErrorIs(t, report.Err, os.ErrPermission)
				require.Equal(t, StatusDestinationError, report.Status)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule, event := tc.setupMocks()

			reportsChan := make(chan Report, 10)

			// Use nil symbols for this test
			rule.Run(t.Context(), event, nil, reportsChan)

			close(reportsChan)
			reports := chanToSlice(reportsChan)

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
					When: source,
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
				rule1 := &MockRule{ID: "rule1", When: source}
				rule2 := &MockRule{ID: "rule2", When: source}
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
					When: source,
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
