package ifs_test

import (
	"context"
	"testing"
	"time"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/ifs"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

type EventMock struct {
	attrs map[string]any
}

func (e *EventMock) Attributes(ctx context.Context) (map[string]any, error) {
	if e.attrs == nil {
		return make(map[string]any), nil
	}
	return e.attrs, nil
}

type MockCacheStorage struct {
	mock.Mock
}

func (m *MockCacheStorage) Get(ctx context.Context, key string) (string, firehose.Report, bool) {
	args := m.Called(ctx, key)
	return args.String(0), args.Get(1).(firehose.Report), args.Bool(2)
}

func (m *MockCacheStorage) Set(ctx context.Context, key string, value string, report firehose.Report, ttl time.Duration) firehose.Report {
	return m.Called(ctx, key, value, report, ttl).Get(0).(firehose.Report)
}

func TestCond_Evaluate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cond     ifs.Cond[*EventMock]
		symbols  map[string]any
		expected bool
		wantErr  bool
	}{
		{
			name:     "empty condition returns true",
			cond:     "",
			symbols:  map[string]any{},
			expected: true,
			wantErr:  false,
		},
		{
			name:     "simple true condition",
			cond:     `status == "active"`,
			symbols:  map[string]any{"status": "active"},
			expected: true,
			wantErr:  false,
		},
		{
			name:     "simple false condition",
			cond:     `status == "active"`,
			symbols:  map[string]any{"status": "inactive"},
			expected: false,
			wantErr:  false,
		},
		{
			name:     "complex condition with and",
			cond:     `status == "active" and count > 5`,
			symbols:  map[string]any{"status": "active", "count": 10},
			expected: true,
			wantErr:  false,
		},
		{
			name:     "invalid expression",
			cond:     `status ==`,
			symbols:  map[string]any{},
			expected: false,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			syms := boolexpr.NewSymbolsCached(tc.symbols)
			var event *EventMock
			result, err := tc.cond.Evaluate(context.Background(), event, syms)

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestRateLimit_Evaluate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func() *ifs.RateLimit[*EventMock]
		expected bool
		wantErr  bool
	}{
		{
			name: "zero limit always passes",
			setup: func() *ifs.RateLimit[*EventMock] {
				return &ifs.RateLimit[*EventMock]{Limit: 0}
			},
			expected: true,
			wantErr:  false,
		},
		{
			name: "negative limit always passes",
			setup: func() *ifs.RateLimit[*EventMock] {
				return &ifs.RateLimit[*EventMock]{Limit: -1}
			},
			expected: true,
			wantErr:  false,
		},
		{
			name: "first call within rate limit passes",
			setup: func() *ifs.RateLimit[*EventMock] {
				return &ifs.RateLimit[*EventMock]{Limit: rate.Limit(10), Burst: 1}
			},
			expected: true,
			wantErr:  false,
		},
		{
			name: "context cancellation returns error",
			setup: func() *ifs.RateLimit[*EventMock] {
				// Use a very low rate to ensure it blocks
				return &ifs.RateLimit[*EventMock]{Limit: rate.Limit(0.00001), Burst: 1}
			},
			expected: false,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cond := tc.setup()
			var event *EventMock

			if tc.wantErr {
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // Cancel immediately

				_, err := cond.Evaluate(ctx, event, nil)
				require.Error(t, err)
			} else {
				result, err := cond.Evaluate(context.Background(), event, nil)
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestOnce_Evaluate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(t *testing.T) (*ifs.Once[*EventMock], *EventMock)
		validate func(t *testing.T, cond *ifs.Once[*EventMock], event *EventMock)
	}{
		{
			name: "zero duration always passes",
			setup: func(t *testing.T) (*ifs.Once[*EventMock], *EventMock) {
				cond := &ifs.Once[*EventMock]{Duration: 0}
				event := &EventMock{attrs: map[string]any{"id": "1"}}
				return cond, event
			},
			validate: func(t *testing.T, cond *ifs.Once[*EventMock], event *EventMock) {
				result, err := cond.Evaluate(context.Background(), event, nil)
				require.NoError(t, err)
				require.True(t, result)
			},
		},
		{
			name: "missing cache returns error",
			setup: func(t *testing.T) (*ifs.Once[*EventMock], *EventMock) {
				cond := &ifs.Once[*EventMock]{Duration: time.Second}
				event := &EventMock{attrs: map[string]any{"id": "1"}}
				return cond, event
			},
			validate: func(t *testing.T, cond *ifs.Once[*EventMock], event *EventMock) {
				_, err := cond.Evaluate(context.Background(), event, nil)
				require.Error(t, err)
				require.Contains(t, err.Error(), "Cache is required")
			},
		},
		{
			name: "first call passes and second fails",
			setup: func(t *testing.T) (*ifs.Once[*EventMock], *EventMock) {
				cache := &MockCacheStorage{}
				cond := &ifs.Once[*EventMock]{
					Duration: time.Second,
					Cache:    cache,
				}
				event := &EventMock{attrs: map[string]any{"id": "123"}}

				// First call: cache miss
				cache.On("Get", context.Background(), mock.Anything).
					Return("", firehose.NewReport("", nil), false).Once()
				cache.On("Set", context.Background(), mock.Anything, "1", mock.Anything, time.Second).Once().Return(firehose.NewSuccessReport())

				// Second call: cache hit
				cache.On("Get", context.Background(), mock.Anything).
					Return("1", firehose.NewReport("", nil), true).Once()

				return cond, event
			},
			validate: func(t *testing.T, cond *ifs.Once[*EventMock], event *EventMock) {
				// First call should pass
				result, err := cond.Evaluate(context.Background(), event, nil)
				require.NoError(t, err)
				require.True(t, result)

				// Second call should fail (deduplicated)
				result, err = cond.Evaluate(context.Background(), event, nil)
				require.NoError(t, err)
				require.False(t, result)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cond, event := tc.setup(t)
			tc.validate(t, cond, event)
		})
	}
}
