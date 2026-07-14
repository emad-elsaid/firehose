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
			source: func(ctx context.Context, cb firehose.Callback[int]) (<-chan struct{}, error) {
				cb(ctx, 7, nil)

				done := make(chan struct{})
				close(done)
				return done, nil
			},
		},
		{
			name: "returns source function error",
			source: func(ctx context.Context, _ firehose.Callback[int]) (<-chan struct{}, error) {
				return nil, sourceFailedErr
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

			done, err := tc.source.Start(t.Context(), cb)

			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr == nil {
				require.NotNil(t, done)
				require.True(t, called)
			}
		})
	}
}
