package actions

import (
	"context"
	"errors"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/require"
)

func TestFuncProcess(t *testing.T) {
	boomErr := errors.New("boom")

	tests := []struct {
		name       string
		action     Func[int, int]
		wantOutput int
		wantErr    error
	}{
		{
			name: "success",
			action: func(_ context.Context, event int, _ boolexpr.Symbols) (int, fh.Report) {
				return event * 2, fh.NewSuccessReport()
			},
			wantOutput: 8,
		},
		{
			name: "error report",
			action: func(_ context.Context, _ int, _ boolexpr.Symbols) (int, fh.Report) {
				return 0, fh.NewReport(boomErr)
			},
			wantErr: boomErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, report := tc.action.Process(t.Context(), 4, boolexpr.SymbolsMap{})
			require.Equal(t, tc.wantOutput, out)
			require.ErrorIs(t, report.Err, tc.wantErr)
		})
	}
}
