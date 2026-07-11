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

func TestCache_Process(t *testing.T) {
	tests := []struct {
		name             string
		setupCache       func(*MockCacheStorage[*event], string, *event, *action[*event, *event])
		wantErr          error
		wantCacheHit     bool
		wantActionCalled bool
	}{
		{
			name: "cache miss calls downstream action and caches result",
			setupCache: func(cache *MockCacheStorage[*event], key string, ev *event, a *action[*event, *event]) {
				a.On("Process", mock.Anything, ev, mock.Anything).
					Return(ev, firehose.NewReport(nil)).Once()

				cache.On("GetOrSet", mock.Anything, key, 5*time.Minute, mock.Anything).
					Run(func(args mock.Arguments) {
						cb := args.Get(3).(func() (*event, error))
						_, _ = cb()
					}).
					Return(ev, nil, false).Once()
			},
			wantErr:          nil,
			wantCacheHit:     false,
			wantActionCalled: true,
		},
		{
			name: "cache hit returns cached result without calling action",
			setupCache: func(cache *MockCacheStorage[*event], key string, ev *event, a *action[*event, *event]) {
				cache.On("GetOrSet", mock.Anything, key, 5*time.Minute, mock.Anything).
					Return(ev, nil, true).Once()
			},
			wantErr:          nil,
			wantCacheHit:     true,
			wantActionCalled: false,
		},
		{
			name: "caches error results from downstream action",
			setupCache: func(cache *MockCacheStorage[*event], key string, ev *event, a *action[*event, *event]) {
				actionErr := errors.New("action failed")
				a.On("Process", mock.Anything, ev, mock.Anything).
					Return(nil, firehose.NewReport(actionErr)).Once()

				cache.On("GetOrSet", mock.Anything, key, 5*time.Minute, mock.Anything).
					Run(func(args mock.Arguments) {
						cb := args.Get(3).(func() (*event, error))
						_, _ = cb()
					}).
					Return((*event)(nil), actionErr, false).Once()
			},
			wantErr:          errors.New("action failed"),
			wantCacheHit:     false,
			wantActionCalled: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCache := new(MockCacheStorage[*event])
			defer mockCache.AssertExpectations(t)

			mockAction := new(action[*event, *event])
			defer mockAction.AssertExpectations(t)

			ev := new(event)
			id, _ := firehose.EventID(ev)
			key := strconv.FormatUint(id, 10)

			tc.setupCache(mockCache, key, ev, mockAction)

			mw := &Cache[*event, *event]{
				Cache:  mockCache,
				Action: mockAction,
				TTL:    5 * time.Minute,
			}
			syms := boolexpr.NewCachedMap(map[string]any{})

			_, report := mw.Process(context.Background(), ev, syms)

			if tc.wantErr == nil {
				assert.NoError(t, report.Err)
			} else {
				assert.EqualError(t, report.Err, tc.wantErr.Error())
			}
		})
	}
}

// simpleEvent is a simple event for testing that doesn't have circular references
// (kept for parity with the original test set).
type simpleEvent struct {
	id int
}

func TestCache_Process_DifferentEventIDs(t *testing.T) {
	tests := []struct {
		name            string
		createEvents    func() []*simpleEvent
		wantActionCalls int
	}{
		{
			name: "different events have different cache keys",
			createEvents: func() []*simpleEvent {
				return []*simpleEvent{{id: 1}, {id: 2}}
			},
			wantActionCalls: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create a custom cache storage for simple events
			type simpleCacheStorage struct {
				mock.Mock
			}

			mockCache := new(simpleCacheStorage)
			defer mockCache.AssertExpectations(t)

			// Create a custom action for simple events
			type simpleAction struct {
				mock.Mock
			}

			mockAction := new(simpleAction)
			defer mockAction.AssertExpectations(t)

			events := tc.createEvents()

			for _, ev := range events {
				id, _ := firehose.EventID(ev)
				key := strconv.FormatUint(id, 10)

				mockAction.On("Process", mock.Anything, ev, mock.Anything).
					Return(ev, firehose.NewReport(nil)).Once()

				mockCache.On("GetOrSet", mock.Anything, key, 5*time.Minute, mock.Anything).
					Run(func(args mock.Arguments) {
						cb := args.Get(3).(func() (*simpleEvent, error))
						_, _ = cb()
					}).
					Return(ev, nil, false).Once()
			}

			// kept as in the original suite
			t.Skip("Skipping due to complex generic type matching requirements")
		})
	}
}

func TestCache_Process_SameEventMultipleTimes(t *testing.T) {
	tests := []struct {
		name            string
		processCount    int
		wantActionCalls int
		wantCacheHits   int
	}{
		{name: "same event processed twice hits cache on second call", processCount: 2, wantActionCalls: 1, wantCacheHits: 1},
		{name: "same event processed many times hits cache", processCount: 5, wantActionCalls: 1, wantCacheHits: 4},
		{name: "single processing always calls action", processCount: 1, wantActionCalls: 1, wantCacheHits: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCache := new(MockCacheStorage[*event])
			defer mockCache.AssertExpectations(t)

			mockAction := new(action[*event, *event])
			defer mockAction.AssertExpectations(t)

			ev := new(event)
			id, _ := firehose.EventID(ev)
			key := strconv.FormatUint(id, 10)

			mockAction.On("Process", mock.Anything, ev, mock.Anything).
				Return(ev, firehose.NewReport(nil)).Once()

			mockCache.On("GetOrSet", mock.Anything, key, 5*time.Minute, mock.Anything).
				Run(func(args mock.Arguments) {
					cb := args.Get(3).(func() (*event, error))
					_, _ = cb()
				}).
				Return(ev, nil, false).Once()

			if tc.processCount > 1 {
				mockCache.On("GetOrSet", mock.Anything, key, 5*time.Minute, mock.Anything).
					Return(ev, nil, true).
					Times(tc.processCount - 1)
			}

			mw := &Cache[*event, *event]{
				Cache:  mockCache,
				Action: mockAction,
				TTL:    5 * time.Minute,
			}
			syms := boolexpr.NewCachedMap(map[string]any{})

			for i := 0; i < tc.processCount; i++ {
				_, report := mw.Process(context.Background(), ev, syms)
				assert.NoError(t, report.Err)
			}
		})
	}
}

func TestCache_Process_TTLRespected(t *testing.T) {
	tests := []struct {
		name    string
		ttl     time.Duration
		wantTTL time.Duration
	}{
		{name: "uses configured TTL for cache entries", ttl: 10 * time.Minute, wantTTL: 10 * time.Minute},
		{name: "uses short TTL", ttl: 1 * time.Second, wantTTL: 1 * time.Second},
		{name: "uses long TTL", ttl: 24 * time.Hour, wantTTL: 24 * time.Hour},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mockCache := new(MockCacheStorage[*event])
			defer mockCache.AssertExpectations(t)

			mockAction := new(action[*event, *event])
			defer mockAction.AssertExpectations(t)

			ev := new(event)
			id, _ := firehose.EventID(ev)
			key := strconv.FormatUint(id, 10)

			mockAction.On("Process", mock.Anything, ev, mock.Anything).
				Return(ev, firehose.NewReport(nil)).Once()

			mockCache.On("GetOrSet", mock.Anything, key, tc.wantTTL, mock.Anything).
				Run(func(args mock.Arguments) {
					cb := args.Get(3).(func() (*event, error))
					_, _ = cb()
				}).
				Return(ev, nil, false).Once()

			mw := &Cache[*event, *event]{
				Cache:  mockCache,
				Action: mockAction,
				TTL:    tc.ttl,
			}
			syms := boolexpr.NewCachedMap(map[string]any{})

			_, report := mw.Process(context.Background(), ev, syms)
			assert.NoError(t, report.Err)
		})
	}
}

type nonHashableEvent struct {
	Fn func()
}

func TestCache_Process_EventIDFailureReturnsActionError(t *testing.T) {
	mw := &Cache[*nonHashableEvent, *event]{
		TTL: 5 * time.Minute,
	}

	out, report := mw.Process(context.Background(), &nonHashableEvent{Fn: func() {}}, boolexpr.NewCachedMap(nil))

	require.Nil(t, out)
	var actionErr firehose.ActionError
	require.ErrorAs(t, report.Err, &actionErr)
	require.Error(t, actionErr.Err)
}
