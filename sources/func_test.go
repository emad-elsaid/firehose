package sources

import (
	"context"
	"errors"
	"testing"

	"github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/require"
)

func TestFuncStart(t *testing.T) {
	sourceFailedErr := errors.New("source failed")

	tests := []struct {
		name    string
		source  Func[int]
		wantErr error
	}{
		{
			name: "calls wrapped source function",
			source: func(ctx context.Context, cb firehose.Callback[int]) (context.Context, error) {
				cb(ctx, 7, nil)

				return context.WithValue(ctx, "done", true), nil
			},
		},
		{
			name: "returns source function error",
			source: func(ctx context.Context, _ firehose.Callback[int]) (context.Context, error) {
				return ctx, sourceFailedErr
			},
			wantErr: sourceFailedErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
		called := false
		cb := func(_ context.Context, _ int, _ firehose.ErrorHandler) {
			called = true
		}

			doneCtx, err := tc.source.Start(t.Context(), cb)

			require.ErrorIs(t, err, tc.wantErr)
			require.NotNil(t, doneCtx)
			if tc.wantErr == nil {
				require.True(t, called)
			}
		})
	}
}
