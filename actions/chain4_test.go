package actions

import (
	"context"
	"errors"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	"github.com/stretchr/testify/require"
)

func TestChain4Process(t *testing.T) {
	thirdErr := errors.New("third failed")

	tests := []struct {
		name       string
		chain      Chain4[int, int, int, int, int]
		wantOutput int
		wantErr    error
	}{
		{
			name: "success",
			chain: Chain4[int, int, int, int, int]{
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
			},
			wantOutput: 15,
		},
		{
			name: "stops on third error",
			chain: Chain4[int, int, int, int, int]{
				First: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event + 1, nil
				}),
				Second: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event * 2, nil
				}),
				Third: Func[int, int](func(_ context.Context, _ int, _ boolexpr.Symbols) (int, error) { return 0, (thirdErr) }),
				Fourth: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, error) {
					return event + 10, nil
				}),
			},
			wantErr: thirdErr,
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
