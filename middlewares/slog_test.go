package middlewares

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// logEntry represents a parsed JSON log entry
type logEntry struct {
	Level   string                 `json:"level"`
	Msg     string                 `json:"msg"`
	Source  any                    `json:"source"`
	Event   any                    `json:"event"`
	Reports []map[string]any       `json:"reports"`
	Extra   map[string]interface{} `json:"-"`
}

func TestSlog_Wrap(t *testing.T) {
	tests := []struct {
		name           string
		setupRule      func() *fh.Rule[*mockEvent, *mockEvent]
		setupCallback  func() fh.Callback[*mockEvent]
		expectedError  bool
		validateResult func(t *testing.T, cb fh.Callback[*mockEvent], err error, middleware *Slog[*mockEvent, *mockEvent])
	}{
		{
			name: "wraps callback successfully",
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "log-rule",
					On:   &mockSource[*mockEvent]{},
					Then: &mockAction[*mockEvent, *mockEvent]{},
					To:   &mockDestination[*mockEvent]{},
				}
			},
			setupCallback: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, reports chan<- fh.Report) {}
			},
			expectedError: false,
			validateResult: func(t *testing.T, cb fh.Callback[*mockEvent], err error, middleware *Slog[*mockEvent, *mockEvent]) {
				require.NoError(t, err)
				require.NotNil(t, cb)
				require.NotNil(t, middleware.downstream)
				require.NotNil(t, middleware.source)
			},
		},
		{
			name: "stores downstream callback reference",
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				source := &mockSource[*mockEvent]{}
				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "log-rule-2",
					On:   source,
					Then: &mockAction[*mockEvent, *mockEvent]{},
					To:   &mockDestination[*mockEvent]{},
				}
			},
			setupCallback: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, reports chan<- fh.Report) {}
			},
			expectedError: false,
			validateResult: func(t *testing.T, cb fh.Callback[*mockEvent], err error, middleware *Slog[*mockEvent, *mockEvent]) {
				require.NoError(t, err)
				require.NotNil(t, middleware.downstream)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := tc.setupRule()
			callback := tc.setupCallback()
			middleware := &Slog[*mockEvent, *mockEvent]{}

			event := &mockEvent{}
			result, err := middleware.WrapCallback(context.Background(), rule, callback, event)

			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tc.validateResult != nil {
				tc.validateResult(t, result, err, middleware)
			}
		})
	}
}

func TestSlog_Callback(t *testing.T) {
	tests := []struct {
		name            string
		setupLogger     func() (*slog.Logger, *bytes.Buffer)
		setupRule       func() *fh.Rule[*mockEvent, *mockEvent]
		setupEvent      func() *mockEvent
		setupDownstream func() fh.Callback[*mockEvent]
		validateLog     func(t *testing.T, logOutput string)
		validateReports func(t *testing.T, reports []fh.Report)
	}{
		{
			name: "logs event and reports with correct level",
			setupLogger: func() (*slog.Logger, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{
					Level: slog.LevelInfo,
				}))
				return logger, buf
			},
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "log-rule",
					On:   &mockSource[*mockEvent]{},
					Then: &mockAction[*mockEvent, *mockEvent]{},
					To:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				return &mockEvent{}
			},
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, reports chan<- fh.Report) {
					reports <- fh.NewRuleReport("log-rule", fh.StatusSuccess, nil)
				}
			},
			validateLog: func(t *testing.T, logOutput string) {
				var entry logEntry
				err := json.Unmarshal([]byte(logOutput), &entry)
				require.NoError(t, err)
				assert.Equal(t, "INFO", entry.Level)
			},
			validateReports: func(t *testing.T, reports []fh.Report) {
				require.Len(t, reports, 1)
				assert.Equal(t, fh.StatusSuccess, reports[0].Status)
			},
		},
		{
			name: "includes source in log output",
			setupLogger: func() (*slog.Logger, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				logger := slog.New(slog.NewJSONHandler(buf, nil))
				return logger, buf
			},
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				source := &mockSource[*mockEvent]{}
				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "source-log-rule",
					On:   source,
					Then: &mockAction[*mockEvent, *mockEvent]{},
					To:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				return &mockEvent{}
			},
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, reports chan<- fh.Report) {
					reports <- fh.NewRuleReport("source-log-rule", fh.StatusSuccess, nil)
				}
			},
			validateLog: func(t *testing.T, logOutput string) {
				var entry logEntry
				err := json.Unmarshal([]byte(logOutput), &entry)
				require.NoError(t, err)
				assert.NotNil(t, entry.Source)
			},
			validateReports: func(t *testing.T, reports []fh.Report) {
				require.Len(t, reports, 1)
			},
		},
		{
			name: "includes event in log output",
			setupLogger: func() (*slog.Logger, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				logger := slog.New(slog.NewJSONHandler(buf, nil))
				return logger, buf
			},
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "event-log-rule",
					On:   &mockSource[*mockEvent]{},
					Then: &mockAction[*mockEvent, *mockEvent]{},
					To:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				return &mockEvent{}
			},
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, reports chan<- fh.Report) {
					reports <- fh.NewRuleReport("event-log-rule", fh.StatusSuccess, nil)
				}
			},
			validateLog: func(t *testing.T, logOutput string) {
				var entry logEntry
				err := json.Unmarshal([]byte(logOutput), &entry)
				require.NoError(t, err)
				assert.NotNil(t, entry.Event)
			},
			validateReports: func(t *testing.T, reports []fh.Report) {
				require.Len(t, reports, 1)
			},
		},
		{
			name: "includes all reports in log output",
			setupLogger: func() (*slog.Logger, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				logger := slog.New(slog.NewJSONHandler(buf, nil))
				return logger, buf
			},
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "multi-report-rule",
					On:   &mockSource[*mockEvent]{},
					Then: &mockAction[*mockEvent, *mockEvent]{},
					To:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				return &mockEvent{}
			},
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, reports chan<- fh.Report) {
					reports <- fh.NewRuleReport("multi-report-rule", fh.StatusSuccess, nil)
					reports <- fh.NewRuleReport("multi-report-rule", fh.StatusSuccess, nil)
					reports <- fh.NewRuleReport("multi-report-rule", fh.StatusSuccess, nil)
				}
			},
			validateLog: func(t *testing.T, logOutput string) {
				var entry logEntry
				err := json.Unmarshal([]byte(logOutput), &entry)
				require.NoError(t, err)
				assert.Len(t, entry.Reports, 3)
			},
			validateReports: func(t *testing.T, reports []fh.Report) {
				require.Len(t, reports, 3)
			},
		},
		{
			name: "forwards reports to outer channel",
			setupLogger: func() (*slog.Logger, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				logger := slog.New(slog.NewJSONHandler(buf, nil))
				return logger, buf
			},
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "forward-rule",
					On:   &mockSource[*mockEvent]{},
					Then: &mockAction[*mockEvent, *mockEvent]{},
					To:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				return &mockEvent{}
			},
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, reports chan<- fh.Report) {
					reports <- fh.NewRuleReport("forward-rule", fh.StatusSuccess, nil)
				}
			},
			validateLog: func(t *testing.T, logOutput string) {
				assert.NotEmpty(t, logOutput)
			},
			validateReports: func(t *testing.T, reports []fh.Report) {
				require.Len(t, reports, 1)
				assert.Equal(t, "forward-rule", reports[0].Rule)
				assert.Equal(t, fh.StatusSuccess, reports[0].Status)
			},
		},
		{
			name: "logs error reports correctly",
			setupLogger: func() (*slog.Logger, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				logger := slog.New(slog.NewJSONHandler(buf, nil))
				return logger, buf
			},
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "error-rule",
					On:   &mockSource[*mockEvent]{},
					Then: &mockAction[*mockEvent, *mockEvent]{},
					To:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				return &mockEvent{}
			},
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, reports chan<- fh.Report) {
					reports <- fh.NewRuleReport("error-rule", fh.StatusError, errors.New("test error"))
				}
			},
			validateLog: func(t *testing.T, logOutput string) {
				var entry logEntry
				err := json.Unmarshal([]byte(logOutput), &entry)
				require.NoError(t, err)
				require.Len(t, entry.Reports, 1)
			},
			validateReports: func(t *testing.T, reports []fh.Report) {
				require.Len(t, reports, 1)
				assert.Equal(t, fh.StatusError, reports[0].Status)
				assert.Error(t, reports[0].Err)
			},
		},
		{
			name: "handles empty reports",
			setupLogger: func() (*slog.Logger, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				logger := slog.New(slog.NewJSONHandler(buf, nil))
				return logger, buf
			},
			setupRule: func() *fh.Rule[*mockEvent, *mockEvent] {
				return &fh.Rule[*mockEvent, *mockEvent]{
					ID:   "empty-rule",
					On:   &mockSource[*mockEvent]{},
					Then: &mockAction[*mockEvent, *mockEvent]{},
					To:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				return &mockEvent{}
			},
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, reports chan<- fh.Report) {
					// No reports
				}
			},
			validateLog: func(t *testing.T, logOutput string) {
				var entry logEntry
				err := json.Unmarshal([]byte(logOutput), &entry)
				require.NoError(t, err)
				assert.Empty(t, entry.Reports)
			},
			validateReports: func(t *testing.T, reports []fh.Report) {
				assert.Empty(t, reports)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger, buf := tc.setupLogger()
			slog.SetDefault(logger)

			rule := tc.setupRule()
			event := tc.setupEvent()
			downstream := tc.setupDownstream()

			middleware := &Slog[*mockEvent, *mockEvent]{}
			wrappedCallback, err := middleware.WrapCallback(context.Background(), rule, downstream, event)
			require.NoError(t, err)

			reports := make(chan fh.Report, 10)
			ctx := context.Background()

			// Collect reports in background
			var collectedReports []fh.Report
			var reportsMu sync.Mutex
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				for report := range reports {
					reportsMu.Lock()
					collectedReports = append(collectedReports, report)
					reportsMu.Unlock()
				}
			}()

			// Execute the callback - it now synchronizes internally
			wrappedCallback(ctx, event, reports)
			close(reports)

			// Wait for collection to finish
			wg.Wait()

			if tc.validateLog != nil && buf.Len() > 0 {
				tc.validateLog(t, buf.String())
			}

			if tc.validateReports != nil {
				reportsMu.Lock()
				tc.validateReports(t, collectedReports)
				reportsMu.Unlock()
			}
		})
	}
}

func TestSlog_CallsDownstream(t *testing.T) {
	tests := []struct {
		name              string
		setupDownstream   func() (fh.Callback[*mockEvent], *mock.Mock)
		expectedCallCount int
	}{
		{
			name: "calls downstream callback exactly once",
			setupDownstream: func() (fh.Callback[*mockEvent], *mock.Mock) {
				m := &mock.Mock{}
				cb := func(ctx context.Context, e *mockEvent, reports chan<- fh.Report) {
					m.MethodCalled("callback", ctx, e, reports)
					reports <- fh.NewReport(fh.StatusSuccess, nil)
				}
				m.On("callback", mock.Anything, mock.Anything, mock.Anything).Return()
				return cb, m
			},
			expectedCallCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			logger := slog.New(slog.NewJSONHandler(buf, nil))
			slog.SetDefault(logger)

			downstream, mockObj := tc.setupDownstream()
			defer mockObj.AssertExpectations(t)

			rule := &fh.Rule[*mockEvent, *mockEvent]{
				ID:   "downstream-rule",
				On:   &mockSource[*mockEvent]{},
				Then: &mockAction[*mockEvent, *mockEvent]{},
				To:   &mockDestination[*mockEvent]{},
			}

			event := &mockEvent{}

			middleware := &Slog[*mockEvent, *mockEvent]{}
			wrappedCallback, err := middleware.WrapCallback(context.Background(), rule, downstream, event)
			require.NoError(t, err)

			reports := make(chan fh.Report, 10)
			ctx := context.Background()

			done := make(chan struct{})
			go func() {
				wrappedCallback(ctx, event, reports)
				close(done)
			}()

			<-done
			close(reports)

			// Drain reports
			for range reports {
			}

			// Give logging goroutine time to complete
			time.Sleep(50 * time.Millisecond)

			mockObj.AssertNumberOfCalls(t, "callback", tc.expectedCallCount)
		})
	}
}

func TestSlog_ChannelClosure(t *testing.T) {
	tests := []struct {
		name            string
		setupDownstream func() fh.Callback[*mockEvent]
		validateClosure func(t *testing.T, panicked bool)
	}{
		{
			name: "closes internal reports channel properly",
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, reports chan<- fh.Report) {
					reports <- fh.NewReport(fh.StatusSuccess, nil)
				}
			},
			validateClosure: func(t *testing.T, panicked bool) {
				assert.False(t, panicked, "should not panic on channel closure")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			logger := slog.New(slog.NewJSONHandler(buf, nil))
			slog.SetDefault(logger)

			downstream := tc.setupDownstream()
			rule := &fh.Rule[*mockEvent, *mockEvent]{
				ID:   "closure-rule",
				On:   &mockSource[*mockEvent]{},
				Then: &mockAction[*mockEvent, *mockEvent]{},
				To:   &mockDestination[*mockEvent]{},
			}

			event := &mockEvent{}

			middleware := &Slog[*mockEvent, *mockEvent]{}
			wrappedCallback, err := middleware.WrapCallback(context.Background(), rule, downstream, event)
			require.NoError(t, err)

			reports := make(chan fh.Report, 10)
			ctx := context.Background()

			panicked := false
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicked = true
					}
				}()

				done := make(chan struct{})
				go func() {
					wrappedCallback(ctx, event, reports)
					close(done)
				}()

				<-done
				close(reports)

				// Drain reports
				for range reports {
				}

				// Give logging goroutine time to complete
				time.Sleep(50 * time.Millisecond)
			}()

			if tc.validateClosure != nil {
				tc.validateClosure(t, panicked)
			}
		})
	}
}
