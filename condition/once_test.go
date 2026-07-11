package condition_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/condition"
	"github.com/stretchr/testify/require"
)

func TestOnce_Evaluate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func(t *testing.T) (*condition.Once[string], string)
		validate func(t *testing.T, cond *condition.Once[string], event string)
	}{
		{
			name: "first call passes and second fails",
			setup: func(t *testing.T) (*condition.Once[string], string) {
				cache := condition.NewMockCacheStorage[string](t)
				cond := &condition.Once[string]{
					Duration: time.Second,
					Cache:    cache,
				}
				event := "event-123"

				id, err := firehose.EventID(event)
				require.NoError(t, err)
				key := strconv.FormatUint(id, 10)

				cache.On("Get", context.Background(), key).
					Return("", nil, false).Once()
				cache.On("Set", context.Background(), key, time.Second, "1").
					Return(error(nil)).Once()
				cache.On("Get", context.Background(), key).
					Return("1", nil, true).Once()

				return cond, event
			},
			validate: func(t *testing.T, cond *condition.Once[string], event string) {
				result, err := cond.Evaluate(context.Background(), event, nil)
				require.NoError(t, err)
				require.True(t, result)

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
