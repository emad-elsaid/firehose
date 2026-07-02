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
					Func[int](func(_ context.Context, _ int) fh.Report {
						counter[0]++
						return fh.NewSuccessReport()
					}),
					Func[int](func(_ context.Context, _ int) fh.Report {
						counter[1]++
						return fh.NewSuccessReport()
					}),
					Func[int](func(_ context.Context, _ int) fh.Report {
						counter[2]++
						return fh.NewSuccessReport()
					}),
				}
			}

			random := &Random[int]{Destinations: targets}

			for range tc.calls {
				report := random.Send(t.Context(), 10)
				if tc.wantErrAs == nil {
					require.NoError(t, report.Err)
					continue
				}

				require.ErrorIs(t, report.Err, tc.wantErrIs)
				require.ErrorAs(t, report.Err, tc.wantErrAs)
			}

			if tc.wantTotal > 0 {
				require.Equal(t, tc.wantTotal, counter[0]+counter[1]+counter[2])
			}
		})
	}
}
