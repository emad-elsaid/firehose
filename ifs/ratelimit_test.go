package ifs_test

import (
	"context"
	"testing"

	"github.com/emad-elsaid/firehose/ifs"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestRateLimit_Evaluate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    func() *ifs.RateLimit[any]
		expected bool
		wantErr  bool
	}{
		{
			name: "zero limit always passes",
			setup: func() *ifs.RateLimit[any] {
				return &ifs.RateLimit[any]{Limit: 0}
			},
			expected: true,
			wantErr:  false,
		},
		{
			name: "negative limit always passes",
			setup: func() *ifs.RateLimit[any] {
				return &ifs.RateLimit[any]{Limit: -1}
			},
			expected: true,
			wantErr:  false,
		},
		{
			name: "first call within rate limit passes",
			setup: func() *ifs.RateLimit[any] {
				return &ifs.RateLimit[any]{Limit: rate.Limit(10), Burst: 1}
			},
			expected: true,
			wantErr:  false,
		},
		{
			name: "context cancellation returns error",
			setup: func() *ifs.RateLimit[any] {
				return &ifs.RateLimit[any]{Limit: rate.Limit(0.00001), Burst: 1}
			},
			expected: false,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cond := tc.setup()

			if tc.wantErr {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				_, err := cond.Evaluate(ctx, nil, nil)
				require.Error(t, err)
				return
			}

			result, err := cond.Evaluate(context.Background(), nil, nil)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}
