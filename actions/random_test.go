package actions

import (
	"context"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/require"
)

func TestRandomProcess(t *testing.T) {
	tests := []struct {
		name      string
		actions   []fh.Action[int, int]
		calls     int
		wantErrIs error
		wantErrAs any
		wantInSet []int
	}{
		{
			name:      "returns error when no actions configured",
			actions:   nil,
			calls:     1,
			wantErrIs: ErrNoActionsConfigured,
			wantErrAs: &fh.ActionError{},
		},
		{
			name: "selects one of configured actions",
			actions: []fh.Action[int, int]{
				Func[int, int](func(_ context.Context, _ int, _ boolexpr.Symbols) (int, fh.Report) {
					return 10, fh.NewSuccessReport()
				}),
				Func[int, int](func(_ context.Context, _ int, _ boolexpr.Symbols) (int, fh.Report) {
					return 20, fh.NewSuccessReport()
				}),
				Func[int, int](func(_ context.Context, _ int, _ boolexpr.Symbols) (int, fh.Report) {
					return 30, fh.NewSuccessReport()
				}),
			},
			calls:     20,
			wantInSet: []int{10, 20, 30},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			random := &Random[int, int]{Actions: tc.actions}

			for range tc.calls {
				out, report := random.Process(t.Context(), 1, boolexpr.SymbolsMap{})

				if tc.wantErrAs == nil {
					require.NoError(t, report.Err)
					if len(tc.wantInSet) > 0 {
						require.Contains(t, tc.wantInSet, out)
					}

					continue
				}

				require.ErrorIs(t, report.Err, tc.wantErrIs)
				require.ErrorAs(t, report.Err, tc.wantErrAs)
			}
		})
	}
}
