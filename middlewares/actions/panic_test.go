package actions

import (
	"context"
	"errors"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPanic_Wrap(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "stores downstream action and returns panic middleware",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mw := &Panic[*event, *event]{}
			mockAction := new(action[*event, *event])
			in := new(event)
			rule := &firehose.Rule[*event, *event]{}

			wrappedAction, err := mw.Wrap(context.Background(), rule, mockAction, in)

			require.NoError(t, err)
			require.NotNil(t, wrappedAction)
			require.IsType(t, (*Panic[*event, *event])(nil), wrappedAction)
			panicMw := wrappedAction.(*Panic[*event, *event])
			require.Equal(t, mockAction, panicMw.downstream)
		})
	}
}

// panicAction is a custom action implementation for testing panics
type panicAction struct {
	panicValue   any
	shouldPanic  bool
	returnEvent  *event
	returnReport firehose.Report
}

func (p *panicAction) Process(ctx context.Context, event *event, syms boolexpr.Symbols) (*event, firehose.Report) {
	if p.shouldPanic {
		panic(p.panicValue)
	}
	return p.returnEvent, p.returnReport
}

func TestPanic_Process(t *testing.T) {
	tests := []struct {
		name           string
		action         firehose.Action[*event, *event]
		wantStatus     firehose.Status
		wantAbort      bool
		wantErrContain string
		wantNilOutput  bool
	}{
		{
			name: "calls downstream action successfully without panic",
			action: &panicAction{
				shouldPanic:  false,
				returnEvent:  &event{},
				returnReport: firehose.Report{Status: firehose.StatusSuccess},
			},
			wantStatus:    firehose.StatusSuccess,
			wantAbort:     false,
			wantNilOutput: false,
		},
		{
			name: "recovers from panic in downstream action",
			action: &panicAction{
				shouldPanic: true,
				panicValue:  "something went wrong",
			},
			wantStatus:     StatusPanicRecovered,
			wantAbort:      true,
			wantErrContain: "action panicked: something went wrong",
			wantNilOutput:  true,
		},
		{
			name: "recovers from nil panic",
			action: &panicAction{
				shouldPanic: true,
				panicValue:  nil,
			},
			wantStatus:     StatusPanicRecovered,
			wantAbort:      true,
			wantErrContain: "panic called with nil argument",
			wantNilOutput:  true,
		},

		{
			name: "preserves error report from downstream action",
			action: &panicAction{
				shouldPanic: false,
				returnEvent: nil,
				returnReport: firehose.Report{
					Status: firehose.StatusActionError,
					Err:    errors.New("downstream error"),
					Abort:  true,
				},
			},
			wantStatus:     firehose.StatusActionError,
			wantAbort:      true,
			wantErrContain: "downstream error",
			wantNilOutput:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev := new(event)
			mw := &Panic[*event, *event]{downstream: tc.action}
			syms := boolexpr.NewSymbolsCached(map[string]any{})

			output, report := mw.Process(context.Background(), ev, syms)

			assert.Equal(t, tc.wantStatus, report.Status)
			assert.Equal(t, tc.wantAbort, report.Abort)

			if tc.wantErrContain != "" {
				require.Error(t, report.Err)
				assert.Contains(t, report.Err.Error(), tc.wantErrContain)
			}

			if tc.wantNilOutput {
				assert.Nil(t, output)
			} else {
				assert.NotNil(t, output)
			}
		})
	}
}

func TestPanic_ErrPanicRecovered(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "ErrPanicRecovered is unwrappable from recovered panic error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ev := new(event)
			panicAct := &panicAction{
				shouldPanic: true,
				panicValue:  "test panic",
			}
			mw := &Panic[*event, *event]{downstream: panicAct}
			syms := boolexpr.NewSymbolsCached(map[string]any{})

			_, report := mw.Process(context.Background(), ev, syms)

			require.Error(t, report.Err)
			assert.True(t, errors.Is(report.Err, ErrPanicRecovered))
		})
	}
}
