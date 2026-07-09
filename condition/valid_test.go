package condition_test

import (
	"context"
	"testing"

	"github.com/emad-elsaid/firehose/condition"
	"github.com/stretchr/testify/require"
)

type ValidEvent struct {
	Name  string `validate:"required,min=3,max=50"`
	Email string `validate:"required,email"`
	Age   int    `validate:"required,gte=18,lte=120"`
}

type InvalidEvent struct {
	Name  string `validate:"required"`
	Email string `validate:"required,email"`
}

type NoValidationEvent struct {
	Name  string
	Email string
	Age   int
}

func TestValid_Evaluate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name      string
		event     any
		expectErr bool
		expectVal bool
	}{
		{
			name: "valid event passes validation",
			event: ValidEvent{
				Name:  "John Doe",
				Email: "john@example.com",
				Age:   25,
			},
			expectErr: false,
			expectVal: true,
		},
		{
			name: "missing required field fails validation",
			event: ValidEvent{
				Email: "john@example.com",
				Age:   25,
			},
			expectErr: true,
			expectVal: false,
		},
		{
			name: "invalid email format fails validation",
			event: ValidEvent{
				Name:  "John Doe",
				Email: "invalid-email",
				Age:   25,
			},
			expectErr: true,
			expectVal: false,
		},
		{
			name: "age below minimum fails validation",
			event: ValidEvent{
				Name:  "John Doe",
				Email: "john@example.com",
				Age:   17,
			},
			expectErr: true,
			expectVal: false,
		},
		{
			name: "age above maximum fails validation",
			event: ValidEvent{
				Name:  "John Doe",
				Email: "john@example.com",
				Age:   121,
			},
			expectErr: true,
			expectVal: false,
		},
		{
			name: "name too short fails validation",
			event: ValidEvent{
				Name:  "Jo",
				Email: "john@example.com",
				Age:   25,
			},
			expectErr: true,
			expectVal: false,
		},
		{
			name: "name too long fails validation",
			event: ValidEvent{
				Name:  "This is a very long name that exceeds the maximum allowed length of fifty characters",
				Email: "john@example.com",
				Age:   25,
			},
			expectErr: true,
			expectVal: false,
		},
		{
			name: "event with no validation tags passes",
			event: NoValidationEvent{
				Name:  "Any Name",
				Email: "not-an-email",
				Age:   -5,
			},
			expectErr: false,
			expectVal: true,
		},
		{
			name: "multiple validation failures",
			event: ValidEvent{
				Name:  "",
				Email: "invalid",
				Age:   10,
			},
			expectErr: true,
			expectVal: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			validate := &condition.Valid[any]{}
			result, err := validate.Evaluate(ctx, tc.event, nil)

			require.Equal(t, tc.expectVal, result)

			if tc.expectErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "validation failed")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValid_NonStructType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		event     any
		expectErr bool
	}{
		{
			name:      "string type",
			event:     "not a struct",
			expectErr: true,
		},
		{
			name:      "int type",
			event:     42,
			expectErr: true,
		},
		{
			name:      "slice type",
			event:     []string{"a", "b"},
			expectErr: true,
		},
		{
			name:      "map type",
			event:     map[string]string{"key": "value"},
			expectErr: true,
		},
		{
			name:      "pointer to struct",
			event:     &ValidEvent{Name: "John", Email: "john@example.com", Age: 25},
			expectErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			validate := &condition.Valid[any]{}

			result, err := validate.Evaluate(context.Background(), tc.event, nil)

			if tc.expectErr {
				require.Error(t, err)
				require.False(t, result)
			} else {
				require.NoError(t, err)
				require.True(t, result)
			}
		})
	}
}
