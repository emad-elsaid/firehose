package destinations

import (
	"context"
	"errors"
	"testing"

	"github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type event struct {
	Value string
}

type customPanicType struct {
	message string
	code    int
}

// panicDestination is a custom destination that panics with the specified value
type panicDestination[T firehose.Event] struct {
	panicValue any
}

func (d *panicDestination[T]) Send(ctx context.Context, event T) firehose.Report {
	panic(d.panicValue)
}

// successDestination always returns success
type successDestination[T firehose.Event] struct{}

func (d *successDestination[T]) Send(ctx context.Context, event T) firehose.Report {
	return firehose.NewReport(firehose.StatusSuccess, nil)
}

// errorDestination always returns an error
type errorDestination[T firehose.Event] struct {
	err error
}

func (d *errorDestination[T]) Send(ctx context.Context, event T) firehose.Report {
	return firehose.NewReport(firehose.StatusDestinationError, d.err)
}

func TestPanic_Wrap(t *testing.T) {
	tests := []struct {
		name        string
		destination firehose.Destination[*event]
	}{
		{
			name:        "wraps destination successfully",
			destination: &successDestination[*event]{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mw := new(Panic[*event, *event])
			in := &event{Value: "test"}
			rule := &firehose.Rule[*event, *event]{}

			outDest, err := mw.Wrap(context.Background(), rule, tc.destination, in)

			require.NoError(t, err)
			require.NotNil(t, outDest)
			require.IsType(t, (*Panic[*event, *event])(nil), outDest)
		})
	}
}

func TestPanic_Send(t *testing.T) {
	tests := []struct {
		name           string
		destination    firehose.Destination[*event]
		expectedStatus firehose.Status
		expectedAbort  bool
		expectError    bool
		errorContains  string
	}{
		{
			name:           "destination executes normally without panic",
			destination:    &successDestination[*event]{},
			expectedStatus: firehose.StatusSuccess,
			expectedAbort:  false,
			expectError:    false,
		},
		{
			name:           "destination returns error normally",
			destination:    &errorDestination[*event]{err: errors.New("send failed")},
			expectedStatus: firehose.StatusDestinationError,
			expectedAbort:  false,
			expectError:    true,
			errorContains:  "send failed",
		},
		{
			name:           "recovers from panic",
			destination:    &panicDestination[*event]{panicValue: "something went wrong"},
			expectedStatus: StatusPanicRecovered,
			expectedAbort:  true,
			expectError:    true,
			errorContains:  "something went wrong",
		},
		{
			name:           "recovers from panic with nil",
			destination:    &panicDestination[*event]{panicValue: nil},
			expectedStatus: StatusPanicRecovered,
			expectedAbort:  true,
			expectError:    true,
			errorContains:  "panic called with nil argument",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mw := new(Panic[*event, *event])
			in := &event{Value: "test"}
			rule := &firehose.Rule[*event, *event]{}

			wrappedDest, err := mw.Wrap(context.Background(), rule, tc.destination, in)
			require.NoError(t, err)

			report := wrappedDest.Send(context.Background(), in)

			assert.Equal(t, tc.expectedStatus, report.Status)
			assert.Equal(t, tc.expectedAbort, report.Abort)

			if tc.expectError {
				require.Error(t, report.Err)
				assert.Contains(t, report.Err.Error(), tc.errorContains)

				if report.Status == StatusPanicRecovered {
					assert.ErrorIs(t, report.Err, ErrPanicRecovered)
				}
			} else {
				assert.NoError(t, report.Err)
			}
		})
	}
}

// toggleDestination alternates between success and panic based on call count
type toggleDestination[T firehose.Event] struct {
	callCount int
}

func (d *toggleDestination[T]) Send(ctx context.Context, event T) firehose.Report {
	d.callCount++
	if d.callCount%2 == 0 {
		panic("oops")
	}
	return firehose.NewReport(firehose.StatusSuccess, nil)
}

// consecutivePanicDestination panics for the first N calls, then succeeds
type consecutivePanicDestination[T firehose.Event] struct {
	panicCount int
	callCount  int
}

func (d *consecutivePanicDestination[T]) Send(ctx context.Context, event T) firehose.Report {
	d.callCount++
	if d.callCount <= d.panicCount {
		panic("panic " + string(rune('0'+d.callCount)))
	}
	return firehose.NewReport(firehose.StatusSuccess, nil)
}

func TestPanic_Send_MultipleInvocations(t *testing.T) {
	tests := []struct {
		name           string
		destination    firehose.Destination[*event]
		numInvocations int
		expectedStates []struct {
			status firehose.Status
			abort  bool
			hasErr bool
		}
	}{
		{
			name:           "handles alternating success and panic",
			destination:    &toggleDestination[*event]{},
			numInvocations: 3,
			expectedStates: []struct {
				status firehose.Status
				abort  bool
				hasErr bool
			}{
				{status: firehose.StatusSuccess, abort: false, hasErr: false},
				{status: StatusPanicRecovered, abort: true, hasErr: true},
				{status: firehose.StatusSuccess, abort: false, hasErr: false},
			},
		},
		{
			name:           "handles multiple consecutive panics",
			destination:    &consecutivePanicDestination[*event]{panicCount: 2},
			numInvocations: 2,
			expectedStates: []struct {
				status firehose.Status
				abort  bool
				hasErr bool
			}{
				{status: StatusPanicRecovered, abort: true, hasErr: true},
				{status: StatusPanicRecovered, abort: true, hasErr: true},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mw := new(Panic[*event, *event])
			in := &event{Value: "test"}
			rule := &firehose.Rule[*event, *event]{}

			wrappedDest, err := mw.Wrap(context.Background(), rule, tc.destination, in)
			require.NoError(t, err)

			for i, expected := range tc.expectedStates {
				report := wrappedDest.Send(context.Background(), in)

				assert.Equal(t, expected.status, report.Status, "invocation %d: status mismatch", i)
				assert.Equal(t, expected.abort, report.Abort, "invocation %d: abort mismatch", i)
				if expected.hasErr {
					assert.Error(t, report.Err, "invocation %d: expected error", i)
				} else {
					assert.NoError(t, report.Err, "invocation %d: unexpected error", i)
				}
			}
		})
	}
}
