package actions

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestRateLimit_Wrap(t *testing.T) {
	tests := []struct {
		name           string
		rateLimit      rate.Limit
		wantWrapped    bool
		wantLimiterNil bool
	}{
		{
			name:           "returns original action when rate limit is zero",
			rateLimit:      0,
			wantWrapped:    false,
			wantLimiterNil: true,
		},
		{
			name:           "returns original action when rate limit is negative",
			rateLimit:      -1,
			wantWrapped:    false,
			wantLimiterNil: true,
		},
		{
			name:           "wraps action when rate limit is positive",
			rateLimit:      10,
			wantWrapped:    true,
			wantLimiterNil: false,
		},
		{
			name:           "wraps action with rate limit of 1",
			rateLimit:      1,
			wantWrapped:    true,
			wantLimiterNil: false,
		},
		{
			name:           "wraps action with high rate limit",
			rateLimit:      1000,
			wantWrapped:    true,
			wantLimiterNil: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mw := &RateLimit[*event, *event]{}
			mockAction := new(action[*event, *event])
			in := new(event)
			rule := &firehose.Rule[*event, *event]{
				RateLimit: tc.rateLimit,
			}

			wrappedAction, err := mw.Wrap(context.Background(), rule, mockAction, in)

			require.NoError(t, err)
			require.NotNil(t, wrappedAction)

			if tc.wantWrapped {
				require.IsType(t, (*RateLimit[*event, *event])(nil), wrappedAction)
				rlMw := wrappedAction.(*RateLimit[*event, *event])
				require.Equal(t, mockAction, rlMw.downstream)
				if tc.wantLimiterNil {
					assert.Nil(t, rlMw.limiter)
				} else {
					assert.NotNil(t, rlMw.limiter)
				}
			} else {
				require.Equal(t, mockAction, wrappedAction)
			}
		})
	}
}

func TestRateLimit_Process(t *testing.T) {
	tests := []struct {
		name            string
		rateLimit       rate.Limit
		eventCount      int
		timeBetween     time.Duration
		cancelContext   bool
		wantSuccess     int
		wantRateLimited int
	}{
		{
			name:            "allows single event under rate limit",
			rateLimit:       10,
			eventCount:      1,
			wantSuccess:     1,
			wantRateLimited: 0,
		},
		{
			name:            "allows burst of 1 event immediately",
			rateLimit:       1,
			eventCount:      1,
			wantSuccess:     1,
			wantRateLimited: 0,
		},
		{
			name:            "handles context cancellation",
			rateLimit:       1,
			eventCount:      1,
			cancelContext:   true,
			wantSuccess:     0,
			wantRateLimited: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockAction := new(action[*event, *event])
			defer mockAction.AssertExpectations(t)

			ev := new(event)

			// Mock only the expected successful calls
			if tc.wantSuccess > 0 {
				mockAction.On("Process", mock.Anything, ev, mock.Anything).
					Return(ev, firehose.Report{Status: firehose.StatusSuccess}).
					Times(tc.wantSuccess)
			}

			mw := &RateLimit[*event, *event]{
				limiter:    rate.NewLimiter(tc.rateLimit, 1),
				downstream: mockAction,
			}
			syms := boolexpr.NewSymbolsCached(map[string]any{})

			ctx := context.Background()
			if tc.cancelContext {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel() // Cancel immediately
			}

			successCount := 0
			rateLimitedCount := 0

			for i := 0; i < tc.eventCount; i++ {
				if i > 0 && tc.timeBetween > 0 {
					time.Sleep(tc.timeBetween)
				}

				_, report := mw.Process(ctx, ev, syms)

				if report.Status == StatusRateLimitError {
					rateLimitedCount++
					assert.True(t, report.Abort)
					assert.Error(t, report.Err)
				} else {
					successCount++
				}
			}

			assert.Equal(t, tc.wantSuccess, successCount)
			assert.Equal(t, tc.wantRateLimited, rateLimitedCount)
		})
	}
}

func TestRateLimit_Process_BurstAndSustained(t *testing.T) {
	tests := []struct {
		name        string
		description string
		setup       func() (*RateLimit[*event, *event], *action[*event, *event])
		validate    func(t *testing.T, mw *RateLimit[*event, *event])
	}{
		{
			name: "burst limit allows 1 event immediately",
			setup: func() (*RateLimit[*event, *event], *action[*event, *event]) {
				mockAction := new(action[*event, *event])
				ev := new(event)
				mockAction.On("Process", mock.Anything, ev, mock.Anything).
					Return(ev, firehose.Report{Status: firehose.StatusSuccess}).Once()

				mw := &RateLimit[*event, *event]{
					limiter:    rate.NewLimiter(rate.Limit(1), 1),
					downstream: mockAction,
				}
				return mw, mockAction
			},
			validate: func(t *testing.T, mw *RateLimit[*event, *event]) {
				ev := new(event)
				syms := boolexpr.NewSymbolsCached(map[string]any{})
				_, report := mw.Process(context.Background(), ev, syms)
				assert.Equal(t, firehose.StatusSuccess, report.Status)
			},
		},
		{
			name: "sustained rate enforcement over time",
			setup: func() (*RateLimit[*event, *event], *action[*event, *event]) {
				mockAction := new(action[*event, *event])
				ev := new(event)
				// Expect 2 successful calls
				mockAction.On("Process", mock.Anything, ev, mock.Anything).
					Return(ev, firehose.Report{Status: firehose.StatusSuccess}).Twice()

				mw := &RateLimit[*event, *event]{
					limiter:    rate.NewLimiter(rate.Limit(100), 1), // 100 events per second
					downstream: mockAction,
				}
				return mw, mockAction
			},
			validate: func(t *testing.T, mw *RateLimit[*event, *event]) {
				ev := new(event)
				syms := boolexpr.NewSymbolsCached(map[string]any{})

				// First event should succeed immediately
				_, report1 := mw.Process(context.Background(), ev, syms)
				assert.Equal(t, firehose.StatusSuccess, report1.Status)

				// Second event should succeed after short delay (within burst)
				time.Sleep(15 * time.Millisecond)
				_, report2 := mw.Process(context.Background(), ev, syms)
				assert.Equal(t, firehose.StatusSuccess, report2.Status)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mw, mockAction := tc.setup()
			defer mockAction.AssertExpectations(t)
			tc.validate(t, mw)
		})
	}
}

func TestRateLimit_Process_ErrorPropagation(t *testing.T) {
	tests := []struct {
		name           string
		setupAction    func(*action[*event, *event], *event)
		wantStatus     firehose.Status
		wantErrContain string
	}{
		{
			name: "propagates downstream action errors",
			setupAction: func(a *action[*event, *event], ev *event) {
				a.On("Process", mock.Anything, ev, mock.Anything).
					Return(nil, firehose.Report{
						Status: firehose.StatusActionError,
						Err:    errors.New("downstream error"),
						Abort:  true,
					}).Once()
			},
			wantStatus:     firehose.StatusActionError,
			wantErrContain: "downstream error",
		},
		{
			name: "propagates success from downstream action",
			setupAction: func(a *action[*event, *event], ev *event) {
				a.On("Process", mock.Anything, ev, mock.Anything).
					Return(ev, firehose.Report{Status: firehose.StatusSuccess}).Once()
			},
			wantStatus: firehose.StatusSuccess,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockAction := new(action[*event, *event])
			defer mockAction.AssertExpectations(t)

			ev := new(event)
			tc.setupAction(mockAction, ev)

			mw := &RateLimit[*event, *event]{
				limiter:    rate.NewLimiter(rate.Limit(100), 1),
				downstream: mockAction,
			}
			syms := boolexpr.NewSymbolsCached(map[string]any{})

			_, report := mw.Process(context.Background(), ev, syms)

			assert.Equal(t, tc.wantStatus, report.Status)
			if tc.wantErrContain != "" {
				require.Error(t, report.Err)
				assert.Contains(t, report.Err.Error(), tc.wantErrContain)
			}
		})
	}
}
