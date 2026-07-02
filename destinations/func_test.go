package destinations

import (
	"context"
	"errors"
	"testing"

	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/require"
)

func TestFuncSend(t *testing.T) {
	sendFailedErr := errors.New("send failed")

	tests := []struct {
		name        string
		destination Func[int]
		wantErr     error
	}{
		{
			name: "success",
			destination: func(_ context.Context, _ int) fh.Report {
				return fh.NewSuccessReport()
			},
		},
		{
			name: "error report",
			destination: func(_ context.Context, _ int) fh.Report {
				return fh.NewReport(sendFailedErr)
			},
			wantErr: sendFailedErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report := tc.destination.Send(t.Context(), 1)
			require.ErrorIs(t, report.Err, tc.wantErr)
		})
	}
}
