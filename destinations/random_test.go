package destinations

import (
	"context"
	"testing"

	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/require"
)

func TestRandomSend(t *testing.T) {
	tests := []struct {
		name      string
		targets   []fh.Destination[int]
		calls     int
		wantErrIs error
		wantErrAs any
		wantTotal int
	}{
		{
			name:      "returns error when no destinations configured",
			targets:   nil,
			calls:     1,
			wantErrIs: ErrNoDestinationsConfigured,
			wantErrAs: &fh.DestinationError{},
		},
		{
			name:      "selects one of configured destinations",
			calls:     20,
			wantTotal: 20,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			counter := map[int]int{}
			targets := tc.targets
			if targets == nil && tc.wantErrAs == nil {
				targets = []fh.Destination[int]{
					Func[int](func(_ context.Context, _ int) error {
						counter[0]++
						return nil
					}),
					Func[int](func(_ context.Context, _ int) error {
						counter[1]++
						return nil
					}),
					Func[int](func(_ context.Context, _ int) error {
						counter[2]++
						return nil
					}),
				}
			}

			random := &Random[int]{Destinations: targets}

			for range tc.calls {
				report := random.Send(t.Context(), 10)
				if tc.wantErrAs == nil {
					require.NoError(t, report)
					continue
				}

				require.ErrorIs(t, report, tc.wantErrIs)
				require.ErrorAs(t, report, tc.wantErrAs)
			}

			if tc.wantTotal > 0 {
				require.Equal(t, tc.wantTotal, counter[0]+counter[1]+counter[2])
			}
		})
	}
}
