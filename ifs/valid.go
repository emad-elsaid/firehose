package ifs

import (
	"context"
	"fmt"

	"github.com/emad-elsaid/boolexpr"
	"github.com/go-playground/validator/v10"
)

// Valid validates event fields using go-playground/validator tags.
// Use this as an If condition for input validation or IfOutput for output validation.
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
//		If: &ifs.Valid[UserEvent]{},  // Validate input
//		IfOutput: &ifs.Valid[ProcessedUser]{},  // Validate output
//	}
//
// Returns true if validation passes, false with error if validation fails.
type Valid[I any] struct{}

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
