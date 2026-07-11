package firehose

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConditionErrorImplementsError(t *testing.T) {
	innerErr := errors.New("condition failed")
	condErr := ConditionError{Err: innerErr}

	// Test that it implements the error interface
	var err error = condErr
	require.NotNil(t, err)

	// Test Error() method
	assert.Equal(t, "condition: condition failed", condErr.Error())

	// Test Unwrap() method
	assert.Equal(t, innerErr, condErr.Unwrap())
}

func TestActionErrorImplementsError(t *testing.T) {
	innerErr := errors.New("action failed")
	actErr := ActionError{Err: innerErr}

	// Test that it implements the error interface
	var err error = actErr
	require.NotNil(t, err)

	// Test Error() method
	assert.Equal(t, "action: action failed", actErr.Error())

	// Test Unwrap() method
	assert.Equal(t, innerErr, actErr.Unwrap())
}

func TestDestinationErrorImplementsError(t *testing.T) {
	innerErr := errors.New("destination failed")
	destErr := DestinationError{Err: innerErr}

	// Test that it implements the error interface
	var err error = destErr
	require.NotNil(t, err)

	// Test Error() method
	assert.Equal(t, "destination: destination failed", destErr.Error())

	// Test Unwrap() method
	assert.Equal(t, innerErr, destErr.Unwrap())
}
