package actions

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestOnce_Wrap(t *testing.T) {
	tests := []struct {
		name        string
		onceEvery   time.Duration
		wantWrapped bool
	}{
		{
			name:        "returns original action when OnceEvery is zero",
			onceEvery:   0,
			wantWrapped: false,
		},
		{
			name:        "wraps action when OnceEvery is positive",
			onceEvery:   1 * time.Minute,
			wantWrapped: true,
		},
		{
			name:        "wraps action with short OnceEvery duration",
			onceEvery:   1 * time.Second,
			wantWrapped: true,
		},
		{
			name:        "wraps action with long OnceEvery duration",
			onceEvery:   24 * time.Hour,
			wantWrapped: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCache := new(MockCacheStorage[string])
			mw := &Once[*event, *event]{Cache: mockCache}
			mockAction := new(action[*event, *event])
			in := new(event)
			rule := firehose.Rule[*event, *event]{
				OnceEvery: tc.onceEvery,
			}

			wrappedAction, err := mw.Wrap(context.Background(), rule, mockAction, in)

			require.NoError(t, err)
			require.NotNil(t, wrappedAction)

			if tc.wantWrapped {
				require.IsType(t, (*Once[*event, *event])(nil), wrappedAction)
				onceMw := wrappedAction.(*Once[*event, *event])
				require.Equal(t, mockAction, onceMw.downstream)
				require.Equal(t, tc.onceEvery, onceMw.ttl)
			} else {
				require.Equal(t, mockAction, wrappedAction)
			}
		})
	}
}

func TestOnce_Process(t *testing.T) {
	tests := []struct {
		name          string
		setupCache    func(*MockCacheStorage[string], string)
		setupAction   func(*action[*event, *event], *event)
		wantStatus    firehose.Status
		wantAbort     bool
		wantNilOutput bool
		wantCacheSet  bool
	}{
		{
			name: "allows first event and caches it",
			setupCache: func(cache *MockCacheStorage[string], key string) {
				cache.On("Get", mock.Anything, key).
					Return("", firehose.Report{}, false).Once()
				cache.On("Set", mock.Anything, key, "1", mock.Anything, 1*time.Minute).
					Return(firehose.Report{}).Once()
			},
			setupAction: func(a *action[*event, *event], ev *event) {
				a.On("Process", mock.Anything, ev, mock.Anything).
					Return(ev, firehose.Report{Status: firehose.StatusSuccess}).Once()
			},
			wantStatus:    firehose.StatusSuccess,
			wantAbort:     false,
			wantNilOutput: false,
			wantCacheSet:  true,
		},
		{
			name: "blocks duplicate event when cache hit",
			setupCache: func(cache *MockCacheStorage[string], key string) {
				cache.On("Get", mock.Anything, key).
					Return("1", firehose.Report{}, true).Once()
			},
			setupAction: func(a *action[*event, *event], ev *event) {
				// Action should not be called
			},
			wantStatus:    StatusOnceHit,
			wantAbort:     true,
			wantNilOutput: true,
			wantCacheSet:  false,
		},
		{
			name: "processes different event IDs independently",
			setupCache: func(cache *MockCacheStorage[string], key string) {
				cache.On("Get", mock.Anything, mock.Anything).
					Return("", firehose.Report{}, false).Once()
				cache.On("Set", mock.Anything, mock.Anything, "1", mock.Anything, 1*time.Minute).
					Return(firehose.Report{}).Once()
			},
			setupAction: func(a *action[*event, *event], ev *event) {
				a.On("Process", mock.Anything, ev, mock.Anything).
					Return(ev, firehose.Report{Status: firehose.StatusSuccess}).Once()
			},
			wantStatus:    firehose.StatusSuccess,
			wantAbort:     false,
			wantNilOutput: false,
			wantCacheSet:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCache := new(MockCacheStorage[string])
			defer mockCache.AssertExpectations(t)

			mockAction := new(action[*event, *event])
			defer mockAction.AssertExpectations(t)

			ev := new(event)

			// Get event ID for cache key
			id, _ := firehose.EventID(ev)
			key := strconv.FormatUint(id, 10)

			tc.setupCache(mockCache, key)
			tc.setupAction(mockAction, ev)

			mw := &Once[*event, *event]{
				Cache:      mockCache,
				downstream: mockAction,
				ttl:        1 * time.Minute,
			}
			syms := boolexpr.NewSymbolsCached(map[string]any{})

			output, report := mw.Process(context.Background(), ev, syms)

			assert.Equal(t, tc.wantStatus, report.Status)
			assert.Equal(t, tc.wantAbort, report.Abort)

			if tc.wantNilOutput {
				assert.Nil(t, output)
			} else {
				assert.NotNil(t, output)
			}
		})
	}
}

func TestOnce_Process_MultipleCalls(t *testing.T) {
	tests := []struct {
		name             string
		eventCount       int
		wantSuccessCount int
		wantBlockedCount int
	}{
		{
			name:             "first call succeeds, second blocked",
			eventCount:       2,
			wantSuccessCount: 1,
			wantBlockedCount: 1,
		},
		{
			name:             "only first of many calls succeeds",
			eventCount:       5,
			wantSuccessCount: 1,
			wantBlockedCount: 4,
		},
		{
			name:             "single call always succeeds",
			eventCount:       1,
			wantSuccessCount: 1,
			wantBlockedCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCache := new(MockCacheStorage[string])
			defer mockCache.AssertExpectations(t)

			mockAction := new(action[*event, *event])
			defer mockAction.AssertExpectations(t)

			ev := new(event)
			id, _ := firehose.EventID(ev)
			key := strconv.FormatUint(id, 10)

			// First call: cache miss
			mockCache.On("Get", mock.Anything, key).
				Return("", firehose.Report{}, false).Once()

			// First call: set cache
			mockCache.On("Set", mock.Anything, key, "1", mock.Anything, 1*time.Minute).
				Return(firehose.Report{}).Once()

			// Subsequent calls: cache hit
			if tc.eventCount > 1 {
				mockCache.On("Get", mock.Anything, key).
					Return("1", firehose.Report{}, true).Times(tc.eventCount - 1)
			}

			// Action called only once
			mockAction.On("Process", mock.Anything, ev, mock.Anything).
				Return(ev, firehose.Report{Status: firehose.StatusSuccess}).Once()

			mw := &Once[*event, *event]{
				Cache:      mockCache,
				downstream: mockAction,
				ttl:        1 * time.Minute,
			}
			syms := boolexpr.NewSymbolsCached(map[string]any{})

			successCount := 0
			blockedCount := 0

			for i := 0; i < tc.eventCount; i++ {
				_, report := mw.Process(context.Background(), ev, syms)
				if report.Status == StatusOnceHit {
					blockedCount++
				} else {
					successCount++
				}
			}

			assert.Equal(t, tc.wantSuccessCount, successCount)
			assert.Equal(t, tc.wantBlockedCount, blockedCount)
		})
	}
}

func TestOnce_Process_DownstreamError(t *testing.T) {
	tests := []struct {
		name           string
		setupAction    func(*action[*event, *event], *event)
		wantStatus     firehose.Status
		wantErrContain string
		wantCacheSet   bool
	}{
		{
			name: "caches result even when downstream returns error",
			setupAction: func(a *action[*event, *event], ev *event) {
				a.On("Process", mock.Anything, ev, mock.Anything).
					Return(nil, firehose.Report{
						Status: firehose.StatusActionError,
						Err:    errors.New("action failed"),
						Abort:  true,
					}).Once()
			},
			wantStatus:     firehose.StatusActionError,
			wantErrContain: "action failed",
			wantCacheSet:   true,
		},
		{
			name: "caches successful result",
			setupAction: func(a *action[*event, *event], ev *event) {
				a.On("Process", mock.Anything, ev, mock.Anything).
					Return(ev, firehose.Report{Status: firehose.StatusSuccess}).Once()
			},
			wantStatus:   firehose.StatusSuccess,
			wantCacheSet: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCache := new(MockCacheStorage[string])
			defer mockCache.AssertExpectations(t)

			mockAction := new(action[*event, *event])
			defer mockAction.AssertExpectations(t)

			ev := new(event)
			id, _ := firehose.EventID(ev)
			key := strconv.FormatUint(id, 10)

			mockCache.On("Get", mock.Anything, key).
				Return("", firehose.Report{}, false).Once()

			if tc.wantCacheSet {
				mockCache.On("Set", mock.Anything, key, "1", mock.Anything, 1*time.Minute).
					Return(firehose.Report{}).Once()
			}

			tc.setupAction(mockAction, ev)

			mw := &Once[*event, *event]{
				Cache:      mockCache,
				downstream: mockAction,
				ttl:        1 * time.Minute,
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
