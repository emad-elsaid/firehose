package condition_test

import (
	"context"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose/condition"
	"github.com/stretchr/testify/require"
)

func TestCond_Evaluate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cond     condition.Cond[any]
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

			syms := boolexpr.NewCachedMap(tc.symbols)
			result, err := tc.cond.Evaluate(context.Background(), nil, syms)

			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}
