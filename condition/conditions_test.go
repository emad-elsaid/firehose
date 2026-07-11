package condition_test

import (
	"context"
	"errors"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/condition"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestConditions_Evaluate(t *testing.T) {
	t.Parallel()

	mockCond := func(pass bool, err error) firehose.Condition[string] {
		m := &MockCondition{}
		m.On("Evaluate", mock.Anything, mock.Anything, mock.Anything).
			Return(pass, err)
		return m
	}

	tests := []struct {
		name     string
		conds    condition.Conditions[string]
		wantPass bool
		wantErr  bool
	}{
		{
			name:     "empty conditions returns true",
			conds:    condition.Conditions[string]{},
			wantPass: true,
			wantErr:  false,
		},
		{
			name: "all conditions pass",
			conds: condition.Conditions[string]{
				mockCond(true, nil),
				mockCond(true, nil),
				mockCond(true, nil),
			},
			wantPass: true,
			wantErr:  false,
		},
		{
			name: "first condition fails",
			conds: condition.Conditions[string]{
				mockCond(false, nil),
				mockCond(true, nil),
			},
			wantPass: false,
			wantErr:  false,
		},
		{
			name: "middle condition fails",
			conds: condition.Conditions[string]{
				mockCond(true, nil),
				mockCond(false, nil),
				mockCond(true, nil),
			},
			wantPass: false,
			wantErr:  false,
		},
		{
			name: "first condition errors",
			conds: condition.Conditions[string]{
				mockCond(false, errors.New("eval error")),
				mockCond(true, nil),
			},
			wantPass: false,
			wantErr:  true,
		},
		{
			name: "condition errors stops evaluation",
			conds: condition.Conditions[string]{
				mockCond(true, nil),
				mockCond(false, errors.New("eval error")),
				mockCond(true, nil),
			},
			wantPass: false,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pass, err := tc.conds.Evaluate(context.Background(), "test", nil)

			require.Equal(t, tc.wantPass, pass)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// MockCondition is a mock implementation of firehose.Condition
type MockCondition struct {
	mock.Mock
}

func (m *MockCondition) Evaluate(ctx context.Context, event string, syms boolexpr.Symbols) (bool, error) {
	args := m.Called(ctx, event, syms)
	return args.Bool(0), args.Error(1)
}
