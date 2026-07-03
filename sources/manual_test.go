package sources

import (
	"context"
	"testing"

	"github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/require"
)

func TestManualEmit(t *testing.T) {
	tests := []struct {
		name        string
		startSource bool
		event       int
		wantErr     error
		wantEvents  []int
	}{
		{
			name:       "returns error if source not started",
			event:      7,
			wantErr:    ErrNotStarted,
			wantEvents: []int{},
		},
		{
			name:        "emits event after start",
			startSource: true,
			event:       7,
			wantErr:     nil,
			wantEvents:  []int{7},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := &Manual[int]{}
			received := []int{}

			if tc.startSource {
				_, err := source.Start(t.Context(), func(_ context.Context, event int, _ firehose.ReportFunc) {
					received = append(received, event)
				})
				require.NoError(t, err)
			}

			err := source.Emit(t.Context(), tc.event)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantEvents, received)
		})
	}
}

func TestManualEmitWithReport(t *testing.T) {
	source := &Manual[int]{}

	_, err := source.Start(t.Context(), func(_ context.Context, _ int, report firehose.ReportFunc) {
		report(firehose.NewReport(context.Canceled))
	})
	require.NoError(t, err)

	gotReported := false
	err = source.EmitWithReport(t.Context(), 1, func(report firehose.Report) {
		gotReported = report.Err == context.Canceled
	})
	require.NoError(t, err)
	require.True(t, gotReported)
}
