package destinations

import (
	"context"
	"errors"
	"testing"

	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/require"
)

func TestFromSliceSend(t *testing.T) {
	firstErr := errors.New("first failed")
	thirdErr := errors.New("third failed")

	tests := []struct {
		name       string
		wrapper    FromSlice[int]
		items      []int
		wantErrIs  []error
		wantErrAs  any
		wantEvents []int
	}{
		{
			name:      "returns error when wrapped destination is missing",
			wrapper:   FromSlice[int]{},
			items:     []int{1},
			wantErrIs: []error{ErrWrappedDestinationRequired},
			wantErrAs: &fh.DestinationError{},
		},
		{
			name:  "forwards all items",
			items: []int{1, 2, 3},
			wrapper: FromSlice[int]{
				Into: Func[int](func(_ context.Context, _ int) fh.Report {
					return fh.NewSuccessReport()
				}),
			},
			wantEvents: []int{1, 2, 3},
		},
		{
			name:  "joins destination errors while continuing",
			items: []int{1, 2, 3},
			wrapper: FromSlice[int]{
				Into: Func[int](func(_ context.Context, event int) fh.Report {
					switch event {
					case 1:
						return fh.NewReport(firstErr)
					case 3:
						return fh.NewReport(thirdErr)
					default:
						return fh.NewSuccessReport()
					}
				}),
			},
			wantErrIs: []error{firstErr, thirdErr},
			wantErrAs: &fh.DestinationError{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			received := []int{}
			if tc.wrapper.Into != nil {
				target := tc.wrapper.Into
				tc.wrapper.Into = Func[int](func(ctx context.Context, event int) fh.Report {
					received = append(received, event)
					return target.Send(ctx, event)
				})
			}

			report := tc.wrapper.Send(t.Context(), tc.items)

			if len(tc.wantEvents) > 0 {
				require.Equal(t, tc.wantEvents, received)
			}

			for _, expectedErr := range tc.wantErrIs {
				require.ErrorIs(t, report.Err, expectedErr)
			}

			if tc.wantErrAs == nil {
				require.NoError(t, report.Err)
			} else {
				require.ErrorAs(t, report.Err, tc.wantErrAs)
			}
		})
	}
}
