package actions

import (
	"context"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/require"
)

func TestRoundRobinProcess(t *testing.T) {
	tests := []struct {
		name        string
		actions     []fh.Action[int, int]
		calls       int
		wantOutputs []int
		wantErrIs   error
		wantErrAs   any
	}{
		{
			name:      "returns error when no actions configured",
			actions:   nil,
			calls:     1,
			wantErrIs: ErrNoActionsConfigured,
			wantErrAs: &fh.ActionError{},
		},
		{
			name: "cycles across actions",
			actions: []fh.Action[int, int]{
				Func[int, int](func(_ context.Context, _ int, _ boolexpr.Symbols) (int, error) {
					return 1, nil
				}),
				Func[int, int](func(_ context.Context, _ int, _ boolexpr.Symbols) (int, error) {
					return 2, nil
				}),
				Func[int, int](func(_ context.Context, _ int, _ boolexpr.Symbols) (int, error) {
					return 3, nil
				}),
			},
			calls:       5,
			wantOutputs: []int{1, 2, 3, 1, 2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			roundRobin := &RoundRobin[int, int]{Actions: tc.actions}

			outputs := make([]int, 0, tc.calls)
			for range tc.calls {
				out, report := roundRobin.Process(t.Context(), 1, boolexpr.SymbolsMap{})
				outputs = append(outputs, out)

				if tc.wantErrAs == nil {
					require.NoError(t, report)
					continue
				}

				require.ErrorIs(t, report, tc.wantErrIs)
				require.ErrorAs(t, report, tc.wantErrAs)
			}

			if len(tc.wantOutputs) > 0 {
				require.Equal(t, tc.wantOutputs, outputs)
			}
		})
	}
}
