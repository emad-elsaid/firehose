package actions

import (
	"context"
	"errors"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	"github.com/stretchr/testify/require"
)

func TestChainProcess(t *testing.T) {
	firstErr := errors.New("first failed")
	secondErr := errors.New("second failed")

	tests := []struct {
		name       string
		chain      Chain[int, int, int]
		wantOutput int
		wantErr    error
	}{
		{
			name: "success",
			chain: Chain[int, int, int]{
				First: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event + 1, nil
				}),
				Second: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event * 2, nil
				}),
			},
			wantOutput: 8,
		},
		{
			name: "stops on first error",
			chain: Chain[int, int, int]{
				First: Func[int, int](func(_ context.Context, _ int, _ boolexpr.Symbols) (int, error) {
					return 0, (firstErr)
				}),
				Second: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event, nil
				}),
			},
			wantErr: firstErr,
		},
		{
			name: "returns second error",
			chain: Chain[int, int, int]{
				First: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event + 1, nil
				}),
				Second: Func[int, int](func(_ context.Context, _ int, _ boolexpr.Symbols) (int, error) {
					return 0, (secondErr)
				}),
			},
			wantErr: secondErr,
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
