package actions

import (
	"context"
	"errors"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	"github.com/stretchr/testify/require"
)

func TestChain5Process(t *testing.T) {
	fifthErr := errors.New("fifth failed")

	tests := []struct {
		name       string
		chain      Chain5[int, int, int, int, int, int]
		wantOutput int
		wantErr    error
	}{
		{
			name: "success",
			chain: Chain5[int, int, int, int, int, int]{
				First: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event + 1, nil
				}),
				Second: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event * 2, nil
				}),
				Third: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event - 3, nil
				}),
				Fourth: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event + 10, nil
				}),
				Fifth: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event / 3, nil
				}),
			},
			wantOutput: 5,
		},
		{
			name: "returns final step error",
			chain: Chain5[int, int, int, int, int, int]{
				First: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event + 1, nil
				}),
				Second: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event * 2, nil
				}),
				Third: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event - 3, nil
				}),
				Fourth: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event + 10, nil
				}),
				Fifth: Func[int, int](func(_ context.Context, _ int, _ boolexpr.Symbols) (int, error) { return 0, (fifthErr) }),
			},
			wantErr: fifthErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, report := tc.chain.Process(t.Context(), 3, boolexpr.SymbolsMap{})

			require.Equal(t, tc.wantOutput, out)
			require.ErrorIs(t, report, tc.wantErr)
		})
	}
}
