package ifs

import (
	"context"
	"errors"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	"github.com/stretchr/testify/require"
)

func TestFuncEvaluate(t *testing.T) {
	ifFailedErr := errors.New("if failed")

	tests := []struct {
		name      string
		condition Func[int]
		wantPass  bool
		wantErr   error
	}{
		{
			name: "pass",
			condition: func(_ context.Context, _ int, _ boolexpr.Symbols) (bool, error) {
				return true, nil
			},
			wantPass: true,
		},
		{
			name: "fail with error",
			condition: func(_ context.Context, _ int, _ boolexpr.Symbols) (bool, error) {
				return false, ifFailedErr
			},
			wantErr: ifFailedErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pass, err := tc.condition.Evaluate(t.Context(), 1, boolexpr.SymbolsMap{})
			require.Equal(t, tc.wantPass, pass)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
