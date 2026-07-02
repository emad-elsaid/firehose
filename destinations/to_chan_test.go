package destinations

import (
	"context"
	"testing"

	fh "github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/require"
)

func TestToChanSend(t *testing.T) {
	tests := []struct {
		name      string
		wrapper   ToChan[int]
		item      int
		wantErrIs error
		wantErrAs any
		wantItems []int
	}{
		{
			name:      "returns error when wrapped destination is missing",
			wrapper:   ToChan[int]{},
			item:      1,
			wantErrIs: ErrWrappedDestinationRequired,
			wantErrAs: &fh.DestinationError{},
		},
		{
			name:      "wraps single item as one-item channel",
			item:      7,
			wantItems: []int{7},
			wrapper: ToChan[int]{
				To: Func[chan int](func(_ context.Context, _ chan int) fh.Report {
					return fh.NewSuccessReport()
				}),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			received := []int{}
			if tc.wrapper.To != nil {
				target := tc.wrapper.To
				tc.wrapper.To = Func[chan int](func(ctx context.Context, event chan int) fh.Report {
					for item := range event {
						received = append(received, item)
					}

					forward := make(chan int, len(received))
					for _, item := range received {
						forward <- item
					}
					close(forward)

					return target.Send(ctx, forward)
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
