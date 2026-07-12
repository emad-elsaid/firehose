package destinations

import (
	"context"
	"errors"
	"testing"

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
			destination: func(_ context.Context, _ int) error {
				return nil
			},
		},
		{
			name: "error report",
			destination: func(_ context.Context, _ int) error {
				return (sendFailedErr)
			},
			wantErr: sendFailedErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report := tc.destination.Send(t.Context(), 1)
			require.ErrorIs(t, report, tc.wantErr)
		})
	}
}
