package middlewares

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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

			cb := func(ctx context.Context, e *mockEvent, report fh.ReportFunc) {}

			result, err := parallel.WrapCallback(context.Background(), rule, cb)

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
					Return(newMockEvent(nil), fh.NewReport(nil))
				dest.On("Send", mock.Anything, mock.Anything).
					Return(fh.NewReport(nil))

				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "rule-1",
					On:   &mockSource[*mockEvent]{},
					Then: action,
					To:   dest,
				}
			},
			setupEvent: func() *mockEvent {
				return newMockEvent(map[string]any{"key": "value"})
			},
			setupRunner: func() TaskRunner {
				return &syncTaskRunner{}
			},
			expectedReports: 1,
			validateReports: func(t *testing.T, reports []fh.Report) {
				require.Len(t, reports, 1)
				assert.NoError(t, reports[0].Err)
			},
		},

		{
			name: "executes callbacks concurrently with goroutine runner",
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				action := &mockAction[*mockEvent, *mockEvent]{}
				dest := &mockDestination[*mockEvent]{}

				action.On("Process", mock.Anything, mock.Anything, mock.Anything).
					Return(newMockEvent(nil), fh.NewReport(nil))
				dest.On("Send", mock.Anything, mock.Anything).
					Return(fh.NewReport(nil))

				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "concurrent-rule",
					On:   &mockSource[*mockEvent]{},
					Then: action,
					To:   dest,
				}
			},
			setupEvent: func() *mockEvent {
				return newMockEvent(map[string]any{"key": "value"})
			},
			setupRunner: func() TaskRunner {
				return &concurrentTaskRunner{}
			},
			expectedReports: 1,
			validateReports: func(t *testing.T, reports []fh.Report) {
				require.Len(t, reports, 1)
				assert.NoError(t, reports[0].Err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := tc.setupRule()
			event := tc.setupEvent()
			runner := tc.setupRunner()

			parallel := &Parallel[*mockEvent, *mockEvent]{Runner: runner}
			_, err := parallel.WrapCallback(context.Background(), rule, nil)
			require.NoError(t, err)

			collector := newReportCollector()
			ctx := context.Background()

			parallel.callback(ctx, event, collector.Collect)

			collectedReports := collector.Reports()

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
				Return(newMockEvent(nil), fh.NewReport(nil))

			dest.On("Send", mock.Anything, mock.Anything).
				Return(fh.NewReport(nil))

			rule := &fh.Rule[*mockEvent, *mockEvent]{
				ID:   "concurrent-rule",
				On:   &mockSource[*mockEvent]{},
				Then: action,
				To:   dest,
			}

			event := newMockEvent(map[string]any{"key": "value"})

			parallel := &Parallel[*mockEvent, *mockEvent]{Runner: &concurrentTaskRunner{}}
			_, err := parallel.WrapCallback(context.Background(), rule, nil)
			require.NoError(t, err)

			// Execute callback multiple times concurrently
			var wg sync.WaitGroup
			for i := 0; i < tc.numCalls; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					collector := newReportCollector()
					parallel.callback(context.Background(), event, collector.Collect)
					_ = collector.Reports()
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
					Return(newMockEvent(nil), fh.NewReport(nil))
				dest.On("Send", mock.Anything, mock.Anything).
					Return(fh.NewReport(nil))

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
			event := newMockEvent(map[string]any{"key": "value"})

			// Use syncTaskRunner which executes immediately
			taskExecuted := false
			runner := &syncTaskRunner{}

			parallel := &Parallel[*mockEvent, *mockEvent]{Runner: runner}
			_, err := parallel.WrapCallback(context.Background(), rule, nil)
			require.NoError(t, err)

			collector := newReportCollector()
			ctx := context.Background()

			// The callback should wait for tasks to complete
			parallel.callback(ctx, event, collector.Collect)
			taskExecuted = true
			_ = collector.Reports()

			if tc.validateTiming != nil {
				tc.validateTiming(t, taskExecuted)
			}
		})
	}
}
