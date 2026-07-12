package destinations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccumulatorSend(t *testing.T) {
	tests := []struct {
		name       string
		events     []int
		wantEvents []int
	}{
		{
			name:       "accumulates events in order",
			events:     []int{1, 2, 3},
			wantEvents: []int{1, 2, 3},
		},
		{
			name:       "handles no events",
			events:     []int{},
			wantEvents: []int{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			destination := &Accumulator[int]{}

			for _, event := range tc.events {
				report := destination.Send(t.Context(), event)
				require.NoError(t, report)
			}

			require.Equal(t, tc.wantEvents, destination.Items())
		})
	}
}

func TestAccumulatorItemsReturnsCopy(t *testing.T) {
	destination := &Accumulator[int]{}
	_ = destination.Send(t.Context(), 1)

	items := destination.Items()
	items[0] = 999

	require.Equal(t, []int{1}, destination.Items())
}
