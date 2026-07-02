package actions

import (
	"context"
	"errors"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/require"
)

func TestChain3Process(t *testing.T) {
	secondErr := errors.New("second failed")

	tests := []struct {
		name       string
		chain      Chain3[int, int, int, int]
		wantOutput int
		wantErr    error
	}{
		{
			name: "success",
			chain: Chain3[int, int, int, int]{
				First: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, fh.Report) {
					return event + 1, fh.NewSuccessReport()
				}),
				Second: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, fh.Report) {
					return event * 2, fh.NewSuccessReport()
				}),
				Third: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, fh.Report) {
					return event - 1, fh.NewSuccessReport()
				}),
			},
			wantOutput: 7,
		},
		{
			name: "stops on middle error",
			chain: Chain3[int, int, int, int]{
				First: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, fh.Report) {
					return event + 1, fh.NewSuccessReport()
				}),
				Second: Func[int, int](func(_ context.Context, _ int, _ boolexpr.Symbols) (int, fh.Report) { return 0, fh.NewReport(secondErr) }),
				Third: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, fh.Report) {
					return event, fh.NewSuccessReport()
				}),
			},
			wantErr: secondErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, report := tc.chain.Process(t.Context(), 3, boolexpr.SymbolsMap{})

			require.Equal(t, tc.wantOutput, out)
			require.ErrorIs(t, report.Err, tc.wantErr)
		})
	}
}
