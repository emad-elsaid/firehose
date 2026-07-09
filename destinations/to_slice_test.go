package destinations

import (
	"context"
	"testing"

	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/require"
)

func TestToSliceSend(t *testing.T) {
	tests := []struct {
		name      string
		wrapper   ToSlice[int]
		item      int
		wantErrIs error
		wantErrAs any
		wantItems []int
	}{
		{
			name:      "returns error when wrapped destination is missing",
			wrapper:   ToSlice[int]{},
			item:      1,
			wantErrIs: ErrWrappedDestinationRequired,
			wantErrAs: &fh.DestinationError{},
		},
		{
			name:      "wraps single item as one-item slice",
			item:      7,
			wantItems: []int{7},
			wrapper: ToSlice[int]{
				Into: Func[[]int](func(_ context.Context, _ []int) fh.Report {
					return fh.NewSuccessReport()
				}),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			received := []int{}
			if tc.wrapper.Into != nil {
				target := tc.wrapper.Into
				tc.wrapper.Into = Func[[]int](func(ctx context.Context, event []int) fh.Report {
					received = append(received, event...)
					return target.Send(ctx, event)
				})
			}

			report := tc.wrapper.Send(t.Context(), tc.item)

			if len(tc.wantItems) > 0 {
				require.Equal(t, tc.wantItems, received)
			}

			if tc.wantErrAs == nil {
				require.NoError(t, report.Err)
			} else {
				require.ErrorIs(t, report.Err, tc.wantErrIs)
				require.ErrorAs(t, report.Err, tc.wantErrAs)
			}
		})
	}
}
