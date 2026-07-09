package condition

import (
	"context"
	"fmt"

	"github.com/emad-elsaid/boolexpr"
	"github.com/go-playground/validator/v10"
)

// Valid validates event fields using go-playground/validator tags.
// Use this as a Where condition for input validation or Having for output validation.
//
// Example usage:
//
//	type UserEvent struct {
//		Username string `validate:"required,min=3,max=20"`
//		Email    string `validate:"required,email"`
//		Age      int    `validate:"required,gte=18"`
//	}
//
//	rule := &firehose.Rule[UserEvent, ProcessedUser]{
//		ID: "validate-user",
//		Where: &condition.Valid[UserEvent]{},  // Validate input
//		Having: &condition.Valid[ProcessedUser]{},  // Validate output
//	}
//
// Returns true if validation passes, false with error if validation fails.
type Valid[I any] struct{}

//nolint:gochecknoglobals // Shared validator instance for efficiency
var validate = validator.New(validator.WithRequiredStructEnabled())

// Evaluate validates the event using the configured validator.
// Returns true if validation passes, false with validation error if it fails.
func (v *Valid[I]) Evaluate(_ context.Context, event I, _ boolexpr.Symbols) (bool, error) {
	err := validate.Struct(event)
	if err != nil {
		return false, fmt.Errorf("validation failed: %w", err)
	}

	return true, nil
}
