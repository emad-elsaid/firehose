package condition_test

import (
	"context"
	"fmt"

	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/condition"
)

// UserEvent represents a user registration event with validation constraints.
type UserEvent struct {
	Username string `validate:"required,min=3,max=20,alphanum"`
	Email    string `validate:"required,email"`
	Age      int    `validate:"required,gte=18,lte=120"`
	Country  string `validate:"required,iso3166_1_alpha2"`
}

// ProcessedUser represents the output after processing.
type ProcessedUser struct {
	ID       string `validate:"required,uuid4"`
	Username string `validate:"required"`
	Status   string `validate:"required,oneof=active pending suspended"`
}

// Example_validate demonstrates using Valid as an Condition for input validation.
func Example_validate() {
	rule := &firehose.SQLRule[UserEvent, ProcessedUser]{
		ID: "validate-user-registration",
		// Validate input before processing
		Where: &condition.Valid[UserEvent]{},
		// ... rest of rule configuration
	}

	// Valid event - will pass validation
	validEvent := UserEvent{
		Username: "johndoe",
		Email:    "john@example.com",
		Age:      25,
		Country:  "US",
	}

	passed, err := rule.Where.Evaluate(context.Background(), validEvent, nil)
	fmt.Printf("Valid event passed: %v, error: %v\n", passed, err)

	// Invalid event - will fail validation
	invalidEvent := UserEvent{
		Username: "jo", // Too short
		Email:    "invalid-email",
		Age:      15,    // Too young
		Country:  "USA", // Should be 2-letter code
	}

	passed, err = rule.Where.Evaluate(context.Background(), invalidEvent, nil)
	fmt.Printf("Invalid event passed: %v, has error: %v\n", passed, err != nil)

	// Output:
	// Valid event passed: true, error: <nil>
	// Invalid event passed: false, has error: true
}

// Example_validateOutput demonstrates using Valid as a Having condition for output validation.
func Example_validateOutput() {
	rule := &firehose.SQLRule[UserEvent, ProcessedUser]{
		ID: "validate-processed-user",
		// Validate output before sending to destination
		Having: &condition.Valid[ProcessedUser]{},
		// ... rest of rule configuration
	}

	// Valid output - will pass validation
	validOutput := ProcessedUser{
		ID:       "550e8400-e29b-41d4-a716-446655440000",
		Username: "johndoe",
		Status:   "active",
	}

	passed, err := rule.Having.Evaluate(context.Background(), validOutput, nil)
	fmt.Printf("Valid output passed: %v, error: %v\n", passed, err)

	// Invalid output - will fail validation
	invalidOutput := ProcessedUser{
		ID:       "not-a-uuid",
		Username: "johndoe",
		Status:   "invalid-status", // Not in allowed values
	}

	passed, err = rule.Having.Evaluate(context.Background(), invalidOutput, nil)
	fmt.Printf("Invalid output passed: %v, has error: %v\n", passed, err != nil)

	// Output:
	// Valid output passed: true, error: <nil>
	// Invalid output passed: false, has error: true
}

// Example_validateBoth demonstrates using Valid for both input and output validation.
func Example_validateBoth() {
	rule := &firehose.SQLRule[UserEvent, ProcessedUser]{
		ID: "validate-both",
		// Validate input
		Where: &condition.Valid[UserEvent]{},
		// Validate output
		Having: &condition.Valid[ProcessedUser]{},
		// ... rest of rule configuration
	}

	fmt.Printf("Rule has input validation: %v\n", rule.Where != nil)
	fmt.Printf("Rule has output validation: %v\n", rule.Having != nil)

	// Output:
	// Rule has input validation: true
	// Rule has output validation: true
}
