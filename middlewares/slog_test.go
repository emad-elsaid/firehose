package middlewares

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

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
		setupRule      func() *fh.SQLRule[*mockEvent, *mockEvent]
		setupCallback  func() fh.Callback[*mockEvent]
		expectedError  bool
		validateResult func(t *testing.T, cb fh.Callback[*mockEvent], err error, middleware *Slog[*mockEvent, *mockEvent])
	}{
		{
			name: "wraps callback successfully",
			setupRule: func() *fh.SQLRule[*mockEvent, *mockEvent] {
				return &fh.SQLRule[*mockEvent, *mockEvent]{
					ID:     "log-rule",
					From:   &mockSource[*mockEvent]{},
					Select: &mockAction[*mockEvent, *mockEvent]{},
					Into:   &mockDestination[*mockEvent]{},
				}
			},
			setupCallback: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, report fh.ErrorHandler) {}
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
			setupRule: func() *fh.SQLRule[*mockEvent, *mockEvent] {
				source := &mockSource[*mockEvent]{}
				return &fh.SQLRule[*mockEvent, *mockEvent]{
					ID:     "log-rule-2",
					From:   source,
					Select: &mockAction[*mockEvent, *mockEvent]{},
					Into:   &mockDestination[*mockEvent]{},
				}
			},
			setupCallback: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, report fh.ErrorHandler) {}
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

			result, err := middleware.WrapCallback(context.Background(), rule, callback)

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
		setupRule       func() *fh.SQLRule[*mockEvent, *mockEvent]
		setupEvent      func() *mockEvent
		setupDownstream func() fh.Callback[*mockEvent]
		validateLog     func(t *testing.T, logOutput string)
		validateReports func(t *testing.T, reports []error)
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
			setupRule: func() *fh.SQLRule[*mockEvent, *mockEvent] {
				return &fh.SQLRule[*mockEvent, *mockEvent]{
					ID:     "log-rule",
					From:   &mockSource[*mockEvent]{},
					Select: &mockAction[*mockEvent, *mockEvent]{},
					Into:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				return newMockEvent(nil)
			},
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, report fh.ErrorHandler) {
					report(fh.NewRuleError("log-rule", nil))
				}
			},
			validateLog: func(t *testing.T, logOutput string) {
				var entry logEntry
				err := json.Unmarshal([]byte(logOutput), &entry)
				require.NoError(t, err)
				assert.Equal(t, "INFO", entry.Level)
			},
			validateReports: func(t *testing.T, reports []error) {
				require.Len(t, reports, 0) // Nil not collected
			},
		},
		{
			name: "includes source in log output",
			setupLogger: func() (*slog.Logger, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				logger := slog.New(slog.NewJSONHandler(buf, nil))
				return logger, buf
			},
			setupRule: func() *fh.SQLRule[*mockEvent, *mockEvent] {
				source := &mockSource[*mockEvent]{}
				return &fh.SQLRule[*mockEvent, *mockEvent]{
					ID:     "source-log-rule",
					From:   source,
					Select: &mockAction[*mockEvent, *mockEvent]{},
					Into:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				return newMockEvent(nil)
			},
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, report fh.ErrorHandler) {
					report(fh.NewRuleError("source-log-rule", nil))
				}
			},
			validateLog: func(t *testing.T, logOutput string) {
				var entry logEntry
				err := json.Unmarshal([]byte(logOutput), &entry)
				require.NoError(t, err)
				assert.NotNil(t, entry.Source)
			},
			validateReports: func(t *testing.T, reports []error) {
				require.Len(t, reports, 0) // Nil not collected
			},
		},
		{
			name: "includes event in log output",
			setupLogger: func() (*slog.Logger, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				logger := slog.New(slog.NewJSONHandler(buf, nil))
				return logger, buf
			},
			setupRule: func() *fh.SQLRule[*mockEvent, *mockEvent] {
				return &fh.SQLRule[*mockEvent, *mockEvent]{
					ID:     "event-log-rule",
					From:   &mockSource[*mockEvent]{},
					Select: &mockAction[*mockEvent, *mockEvent]{},
					Into:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				return newMockEvent(nil)
			},
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, report fh.ErrorHandler) {
					report(fh.NewRuleError("event-log-rule", nil))
				}
			},
			validateLog: func(t *testing.T, logOutput string) {
				var entry logEntry
				err := json.Unmarshal([]byte(logOutput), &entry)
				require.NoError(t, err)
				assert.NotNil(t, entry.Event)
			},
			validateReports: func(t *testing.T, reports []error) {
				require.Len(t, reports, 0) // Nil not collected
			},
		},
		{
			name: "includes all reports in log output",
			setupLogger: func() (*slog.Logger, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				logger := slog.New(slog.NewJSONHandler(buf, nil))
				return logger, buf
			},
			setupRule: func() *fh.SQLRule[*mockEvent, *mockEvent] {
				return &fh.SQLRule[*mockEvent, *mockEvent]{
					ID:     "multi-report-rule",
					From:   &mockSource[*mockEvent]{},
					Select: &mockAction[*mockEvent, *mockEvent]{},
					Into:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				return newMockEvent(nil)
			},
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, report fh.ErrorHandler) {
					report(fh.NewRuleError("multi-report-rule", nil))
					report(fh.NewRuleError("multi-report-rule", nil))
					report(fh.NewRuleError("multi-report-rule", nil))
				}
			},
			validateLog: func(t *testing.T, logOutput string) {
				// Nil errors are not logged
				if logOutput == "" {
					return
				}
				var entry logEntry
				err := json.Unmarshal([]byte(logOutput), &entry)
				require.NoError(t, err)
				assert.Len(t, entry.Reports, 0) // Nil errors not logged
			},
			validateReports: func(t *testing.T, reports []error) {
				require.Len(t, reports, 0) // Nil errors not collected
			},
		},
		{
			name: "forwards reports to outer channel",
			setupLogger: func() (*slog.Logger, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				logger := slog.New(slog.NewJSONHandler(buf, nil))
				return logger, buf
			},
			setupRule: func() *fh.SQLRule[*mockEvent, *mockEvent] {
				return &fh.SQLRule[*mockEvent, *mockEvent]{
					ID:     "forward-rule",
					From:   &mockSource[*mockEvent]{},
					Select: &mockAction[*mockEvent, *mockEvent]{},
					Into:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				return newMockEvent(nil)
			},
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, report fh.ErrorHandler) {
					report(fh.NewRuleError("forward-rule", errors.New("test error")))
				}
			},
			validateLog: func(t *testing.T, logOutput string) {
				assert.NotEmpty(t, logOutput)
			},
			validateReports: func(t *testing.T, reports []error) {
				require.Len(t, reports, 1)
				var ruleErr fh.RuleError
				if assert.ErrorAs(t, reports[0], &ruleErr) {
					assert.Equal(t, "forward-rule", ruleErr.Rule)
					assert.Error(t, ruleErr.Err)
				}
			},
		},
		{
			name: "logs error reports correctly",
			setupLogger: func() (*slog.Logger, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				logger := slog.New(slog.NewJSONHandler(buf, nil))
				return logger, buf
			},
			setupRule: func() *fh.SQLRule[*mockEvent, *mockEvent] {
				return &fh.SQLRule[*mockEvent, *mockEvent]{
					ID:     "error-rule",
					From:   &mockSource[*mockEvent]{},
					Select: &mockAction[*mockEvent, *mockEvent]{},
					Into:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				return newMockEvent(nil)
			},
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, report fh.ErrorHandler) {
					report(fh.NewRuleError("error-rule", errors.New("test error")))
				}
			},
			validateLog: func(t *testing.T, logOutput string) {
				var entry logEntry
				err := json.Unmarshal([]byte(logOutput), &entry)
				require.NoError(t, err)
				require.Len(t, entry.Reports, 1)
			},
			validateReports: func(t *testing.T, reports []error) {
				require.Len(t, reports, 1)
				assert.Error(t, reports[0])
			},
		},
		{
			name: "handles empty reports",
			setupLogger: func() (*slog.Logger, *bytes.Buffer) {
				buf := &bytes.Buffer{}
				logger := slog.New(slog.NewJSONHandler(buf, nil))
				return logger, buf
			},
			setupRule: func() *fh.SQLRule[*mockEvent, *mockEvent] {
				return &fh.SQLRule[*mockEvent, *mockEvent]{
					ID:     "empty-rule",
					From:   &mockSource[*mockEvent]{},
					Select: &mockAction[*mockEvent, *mockEvent]{},
					Into:   &mockDestination[*mockEvent]{},
				}
			},
			setupEvent: func() *mockEvent {
				return newMockEvent(nil)
			},
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, report fh.ErrorHandler) {
					// No reports
				}
			},
			validateLog: func(t *testing.T, logOutput string) {
				var entry logEntry
				err := json.Unmarshal([]byte(logOutput), &entry)
				require.NoError(t, err)
				assert.Empty(t, entry.Reports)
			},
			validateReports: func(t *testing.T, reports []error) {
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
			wrappedCallback, err := middleware.WrapCallback(context.Background(), rule, downstream)
			require.NoError(t, err)

			collector := newReportCollector()
			ctx := context.Background()

			// Execute the callback - it now synchronizes internally
			wrappedCallback(ctx, event, collector.Collect)

			collectedReports := collector.Errors()

			if tc.validateLog != nil && buf.Len() > 0 {
				tc.validateLog(t, buf.String())
			}

			if tc.validateReports != nil {
				tc.validateReports(t, collectedReports)
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
				cb := func(ctx context.Context, e *mockEvent, report fh.ErrorHandler) {
					m.MethodCalled("callback", ctx, e, report)
					report(nil)
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

			rule := &fh.SQLRule[*mockEvent, *mockEvent]{
				ID:     "downstream-rule",
				From:   &mockSource[*mockEvent]{},
				Select: &mockAction[*mockEvent, *mockEvent]{},
				Into:   &mockDestination[*mockEvent]{},
			}

			event := newMockEvent(nil)

			middleware := &Slog[*mockEvent, *mockEvent]{}
			wrappedCallback, err := middleware.WrapCallback(context.Background(), rule, downstream)
			require.NoError(t, err)

			collector := newReportCollector()
			ctx := context.Background()

			wrappedCallback(ctx, event, collector.Collect)
			_ = collector.Errors()

			mockObj.AssertNumberOfCalls(t, "callback", tc.expectedCallCount)
		})
	}
}

func TestSlog_CallbackDoesNotPanic(t *testing.T) {
	tests := []struct {
		name            string
		setupDownstream func() fh.Callback[*mockEvent]
		validateClosure func(t *testing.T, panicked bool)
	}{
		{
			name: "returns without panic while reporting",
			setupDownstream: func() fh.Callback[*mockEvent] {
				return func(ctx context.Context, e *mockEvent, report fh.ErrorHandler) {
					report(nil)
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
			rule := &fh.SQLRule[*mockEvent, *mockEvent]{
				ID:     "closure-rule",
				From:   &mockSource[*mockEvent]{},
				Select: &mockAction[*mockEvent, *mockEvent]{},
				Into:   &mockDestination[*mockEvent]{},
			}

			event := newMockEvent(nil)

			middleware := &Slog[*mockEvent, *mockEvent]{}
			wrappedCallback, err := middleware.WrapCallback(context.Background(), rule, downstream)
			require.NoError(t, err)

			collector := newReportCollector()
			ctx := context.Background()

			panicked := false
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicked = true
					}
				}()

				wrappedCallback(ctx, event, collector.Collect)
				_ = collector.Errors()
			}()

			if tc.validateClosure != nil {
				tc.validateClosure(t, panicked)
			}
		})
	}
}
