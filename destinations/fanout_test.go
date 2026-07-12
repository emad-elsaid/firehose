package destinations

import (
	"context"
	"errors"
	"testing"

	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/require"
)

func TestFanoutSend(t *testing.T) {
	firstErr := errors.New("first")
	secondErr := errors.New("second")

	tests := []struct {
		name      string
		targets   []fh.Destination[int]
		wantErrIs []error
		wantErrAs any
	}{
		{
			name:      "returns error when no destinations configured",
			targets:   nil,
			wantErrIs: []error{ErrNoDestinationsConfigured},
			wantErrAs: &fh.DestinationError{},
		},
		{
			name: "returns success when all destinations succeed",
			targets: []fh.Destination[int]{
				Func[int](func(_ context.Context, _ int) error {
					return nil
				}),
				Func[int](func(_ context.Context, _ int) error {
					return nil
				}),
			},
		},
		{
			name: "joins destination errors",
			targets: []fh.Destination[int]{
				Func[int](func(_ context.Context, _ int) error {
					return (firstErr)
				}),
				Func[int](func(_ context.Context, _ int) error {
					return (secondErr)
				}),
			},
			wantErrIs: []error{firstErr, secondErr},
			wantErrAs: &fh.DestinationError{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fanout := Fanout[int]{Destinations: tc.targets}
			report := fanout.Send(t.Context(), 1)

			for _, expectedErr := range tc.wantErrIs {
				require.ErrorIs(t, report, expectedErr)
			}

			if tc.wantErrAs == nil {
				require.NoError(t, report)
			} else {
				require.ErrorAs(t, report, tc.wantErrAs)
			}
		})
	}
}
