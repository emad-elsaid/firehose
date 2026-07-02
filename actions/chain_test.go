package actions

import (
	"context"
	"errors"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
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
				First: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, fh.Report) {
					return event + 1, fh.NewSuccessReport()
				}),
				Second: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, fh.Report) {
					return event * 2, fh.NewSuccessReport()
				}),
			},
			wantOutput: 8,
		},
		{
			name: "stops on first error",
			chain: Chain[int, int, int]{
				First: Func[int, int](func(_ context.Context, _ int, _ boolexpr.Symbols) (int, fh.Report) {
					return 0, fh.NewReport(firstErr)
				}),
				Second: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, fh.Report) {
					return event, fh.NewSuccessReport()
				}),
			},
			wantErr: firstErr,
		},
		{
			name: "returns second error",
			chain: Chain[int, int, int]{
				First: Func[int, int](func(_ context.Context, event int, _ boolexpr.Symbols) (int, fh.Report) {
					return event + 1, fh.NewSuccessReport()
				}),
				Second: Func[int, int](func(_ context.Context, _ int, _ boolexpr.Symbols) (int, fh.Report) {
					return 0, fh.NewReport(secondErr)
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
