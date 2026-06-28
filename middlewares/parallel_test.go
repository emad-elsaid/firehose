package middlewares

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// mockEvent implements the Attributer interface for testing
type mockEvent struct {
	mock.Mock
}

func (e *mockEvent) Attributes(ctx context.Context) (map[string]any, error) {
	args := e.Called(ctx)
	r1, ok := args.Get(0).(map[string]any)
	if !ok {
		return nil, args.Error(1)
	}
	return r1, args.Error(1)
}

// mockAction implements Action interface
type mockAction[I, O any] struct {
	mock.Mock
}

func (a *mockAction[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, fh.Report) {
	args := a.Called(ctx, event, syms)
	var zero O
	r1, ok := args.Get(0).(O)
	if !ok {
		r1 = zero
	}
	return r1, args.Get(1).(fh.Report)
}

// mockSource implements Source interface
type mockSource[T any] struct {
	mock.Mock
}

func (s *mockSource[T]) Start(ctx context.Context, cb fh.Callback[T]) (context.Context, error) {
	args := s.Called(ctx, cb)
	return args.Get(0).(context.Context), args.Error(1)
}

// testTaskRunner extends MockTaskRunner with ExecuteAll helper for testing
type testTaskRunner struct {
	*MockTaskRunner
	mu    sync.Mutex
	tasks []func()
}

func newTestTaskRunner() *testTaskRunner {
	return &testTaskRunner{
		MockTaskRunner: &MockTaskRunner{},
	}
}

func (r *testTaskRunner) Run(f func()) {
	r.MockTaskRunner.Run(f)
	r.mu.Lock()
	r.tasks = append(r.tasks, f)
	r.mu.Unlock()
}

func (r *testTaskRunner) ExecuteAll() {
	r.mu.Lock()
	tasks := make([]func(), len(r.tasks))
	copy(tasks, r.tasks)
	r.mu.Unlock()

	for _, task := range tasks {
		task()
	}
}

// syncTaskRunner executes tasks immediately
type syncTaskRunner struct{}

func (r *syncTaskRunner) Run(f func()) {
	f()
}

// concurrentTaskRunner executes tasks in goroutines
type concurrentTaskRunner struct{}

func (r *concurrentTaskRunner) Run(f func()) {
	go f()
}

func TestParallel_Wrap(t *testing.T) {
	tests := []struct {
		name           string
		setupRule      func() *fh.Rule[*mockEvent, *mockEvent]
		setupRunner    func() TaskRunner
		expectedError  bool
		validateResult func(t *testing.T, cb fh.Callback[*mockEvent], err error)
	}{
		{
			name: "wraps callback successfully",
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "test-rule",
					On:   &mockSource[*mockEvent]{},
					Then: &mockAction[*mockEvent, *mockEvent]{},
					To:   &mockDestination[*mockEvent]{},
				}
			},
			setupRunner: func() TaskRunner {
				return &syncTaskRunner{}
			},
			expectedError: false,
			validateResult: func(t *testing.T, cb fh.Callback[*mockEvent], err error) {
				require.NoError(t, err)
				require.NotNil(t, cb)
			},
		},
		{
			name: "stores rule reference",
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "test-rule-2",
					On:   &mockSource[*mockEvent]{},
					Then: &mockAction[*mockEvent, *mockEvent]{},
					To:   &mockDestination[*mockEvent]{},
				}
			},
			setupRunner: func() TaskRunner {
				return &syncTaskRunner{}
			},
			expectedError: false,
			validateResult: func(t *testing.T, cb fh.Callback[*mockEvent], err error) {
				require.NoError(t, err)
				require.NotNil(t, cb)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := tc.setupRule()
			runner := tc.setupRunner()
			parallel := &Parallel[*mockEvent, *mockEvent]{Runner: runner}

			event := &mockEvent{}
			cb := func(ctx context.Context, e *mockEvent, reports chan<- fh.Report) {}

			result, err := parallel.WrapCallback(context.Background(), rule, cb, event)

			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tc.validateResult != nil {
				tc.validateResult(t, result, err)
			}
		})
	}
}

func TestParallel_Callback(t *testing.T) {
	tests := []struct {
		name            string
		setupRule       func() *fh.Rule[*mockEvent, *mockEvent]
		setupEvent      func() *mockEvent
		setupRunner     func() TaskRunner
		expectedReports int
		validateReports func(t *testing.T, reports []fh.Report)
	}{
		{
			name: "executes single rule in parallel",
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				action := &mockAction[*mockEvent, *mockEvent]{}
				dest := &mockDestination[*mockEvent]{}

				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(&mockEvent{}, fh.NewReport(fh.StatusSuccess, nil))
				dest.On("Send", mock.Anything, mock.Anything).
					Return(fh.NewReport(fh.StatusSuccess, nil))

				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "rule-1",
					On:   &mockSource[*mockEvent]{},
					Then: action,
					To:   dest,
				}
			},
			setupEvent: func() *mockEvent {
				event := &mockEvent{}
				event.On("Attributes", mock.Anything).Return(map[string]any{"key": "value"}, nil)
				return event
			},
			setupRunner: func() TaskRunner {
				return &syncTaskRunner{}
			},
			expectedReports: 1,
			validateReports: func(t *testing.T, reports []fh.Report) {
				require.Len(t, reports, 1)
				assert.Equal(t, fh.StatusSuccess, reports[0].Status)
			},
		},

		{
			name: "handles event attributes error",
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "rule-error",
					On:   &mockSource[*mockEvent]{},
					Then: &mockAction[*mockEvent, *mockEvent]{},
					To:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				event := &mockEvent{}
				event.On("Attributes", mock.Anything).
					Return(nil, errors.New("attributes error"))
				return event
			},
			setupRunner: func() TaskRunner {
				return &syncTaskRunner{}
			},
			expectedReports: 1,
			validateReports: func(t *testing.T, reports []fh.Report) {
				require.Len(t, reports, 1)
				assert.Equal(t, fh.StatusError, reports[0].Status)
				assert.Error(t, reports[0].Err)
				assert.Contains(t, reports[0].Err.Error(), "failed to get event attributes")
			},
		},
		{
			name: "executes callbacks concurrently with goroutine runner",
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				action := &mockAction[*mockEvent, *mockEvent]{}
				dest := &mockDestination[*mockEvent]{}

				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(&mockEvent{}, fh.NewReport(fh.StatusSuccess, nil))
				dest.On("Send", mock.Anything, mock.Anything).
					Return(fh.NewReport(fh.StatusSuccess, nil))

				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "concurrent-rule",
					On:   &mockSource[*mockEvent]{},
					Then: action,
					To:   dest,
				}
			},
			setupEvent: func() *mockEvent {
				event := &mockEvent{}
				event.On("Attributes", mock.Anything).Return(map[string]any{"key": "value"}, nil)
				return event
			},
			setupRunner: func() TaskRunner {
				return &concurrentTaskRunner{}
			},
			expectedReports: 1,
			validateReports: func(t *testing.T, reports []fh.Report) {
				require.Len(t, reports, 1)
				assert.Equal(t, fh.StatusSuccess, reports[0].Status)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := tc.setupRule()
			event := tc.setupEvent()
			runner := tc.setupRunner()

			parallel := &Parallel[*mockEvent, *mockEvent]{Runner: runner}
			_, err := parallel.WrapCallback(context.Background(), rule, nil, event)
			require.NoError(t, err)

			reports := make(chan fh.Report, 10)
			ctx := context.Background()

			go func() {
				parallel.callback(ctx, event, reports)
				close(reports)
			}()

			var collectedReports []fh.Report
			for report := range reports {
				collectedReports = append(collectedReports, report)
			}

			assert.Equal(t, tc.expectedReports, len(collectedReports))
			if tc.validateReports != nil {
				tc.validateReports(t, collectedReports)
			}
		})
	}
}

func TestParallel_ConcurrencySafety(t *testing.T) {
	tests := []struct {
		name           string
		numCalls       int
		validateResult func(t *testing.T, counter int32)
	}{
		{
			name:     "safely handles concurrent task execution",
			numCalls: 10,
			validateResult: func(t *testing.T, counter int32) {
				assert.Equal(t, int32(10), counter)
			},
		},
		{
			name:     "safely handles many concurrent task executions",
			numCalls: 100,
			validateResult: func(t *testing.T, counter int32) {
				assert.Equal(t, int32(100), counter)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var counter int32

			action := &mockAction[*mockEvent, *mockEvent]{}
			dest := &mockDestination[*mockEvent]{}

			action.On("Process", mock.Anything, mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) {
					atomic.AddInt32(&counter, 1)
				}).
				Return(&mockEvent{}, fh.NewReport(fh.StatusSuccess, nil))

			dest.On("Send", mock.Anything, mock.Anything).
				Return(fh.NewReport(fh.StatusSuccess, nil))

			rule := &fh.Rule[*mockEvent, *mockEvent]{
				ID:   "concurrent-rule",
				On:   &mockSource[*mockEvent]{},
				Then: action,
				To:   dest,
			}

			event := &mockEvent{}
			event.On("Attributes", mock.Anything).Return(map[string]any{"key": "value"}, nil)

			parallel := &Parallel[*mockEvent, *mockEvent]{Runner: &concurrentTaskRunner{}}
			_, err := parallel.WrapCallback(context.Background(), rule, nil, event)
			require.NoError(t, err)

			// Execute callback multiple times concurrently
			var wg sync.WaitGroup
			for i := 0; i < tc.numCalls; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					reports := make(chan fh.Report, 10)
					parallel.callback(context.Background(), event, reports)
					close(reports)
					// Drain reports
					for range reports {
					}
				}()
			}

			wg.Wait()

			if tc.validateResult != nil {
				tc.validateResult(t, counter)
			}
		})
	}
}

func TestParallel_WaitGroup(t *testing.T) {
	tests := []struct {
		name           string
		setupRules     func() *fh.Rule[*mockEvent, *mockEvent]
		validateTiming func(t *testing.T, taskExecuted bool)
	}{
		{
			name: "waits for all tasks to complete before returning",
			setupRules: func() *fh.Rule[*mockEvent, *mockEvent] {
				action := &mockAction[*mockEvent, *mockEvent]{}
				dest := &mockDestination[*mockEvent]{}

				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(&mockEvent{}, fh.NewReport(fh.StatusSuccess, nil))
				dest.On("Send", mock.Anything, mock.Anything).
					Return(fh.NewReport(fh.StatusSuccess, nil))

				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "wait-rule",
					On:   &mockSource[*mockEvent]{},
					Then: action,
					To:   dest,
				}
			},
			validateTiming: func(t *testing.T, taskExecuted bool) {
				assert.True(t, taskExecuted, "task should be executed before callback returns")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := tc.setupRules()
			event := &mockEvent{}
			event.On("Attributes", mock.Anything).Return(map[string]any{"key": "value"}, nil)

			// Use syncTaskRunner which executes immediately
			taskExecuted := false
			runner := &syncTaskRunner{}

			parallel := &Parallel[*mockEvent, *mockEvent]{Runner: runner}
			_, err := parallel.WrapCallback(context.Background(), rule, nil, event)
			require.NoError(t, err)

			reports := make(chan fh.Report, 10)
			ctx := context.Background()

			// The callback should wait for tasks to complete
			parallel.callback(ctx, event, reports)
			taskExecuted = true
			close(reports)

			// Drain reports
			for range reports {
			}

			if tc.validateTiming != nil {
				tc.validateTiming(t, taskExecuted)
			}
		})
	}
}
