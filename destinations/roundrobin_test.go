package destinations

import (
	"context"
	"testing"

	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/require"
)

func TestRoundRobinSend(t *testing.T) {
	tests := []struct {
		name      string
		calls     int
		wantErrIs error
		wantErrAs any
		wantCount map[int]int
	}{
		{
			name:      "returns error when no destinations configured",
			calls:     1,
			wantErrIs: ErrNoDestinationsConfigured,
			wantErrAs: &fh.DestinationError{},
		},
		{
			name:  "cycles across destinations",
			calls: 5,
			wantCount: map[int]int{
				0: 2,
				1: 2,
				2: 1,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			counter := map[int]int{}
			targets := []fh.Destination[int]{
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
			if tc.wantErrAs != nil {
				targets = nil
			}

			roundRobin := &RoundRobin[int]{Destinations: targets}

			for range tc.calls {
				report := roundRobin.Send(t.Context(), 10)
				if tc.wantErrAs == nil {
					require.NoError(t, report)
					continue
				}

				require.ErrorIs(t, report, tc.wantErrIs)
				require.ErrorAs(t, report, tc.wantErrAs)
			}

			if tc.wantCount != nil {
				require.Equal(t, tc.wantCount, counter)
			}
		})
	}
}
