package firehose

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRuleError(t *testing.T) {
	t.Run("NewRuleError with nil error returns nil", func(t *testing.T) {
		err := NewRuleError("rule-1", nil)
		assert.NoError(t, err)
	})

	t.Run("NewRuleError with error", func(t *testing.T) {
		baseErr := errors.New("boom")
		err := NewRuleError("rule-1", baseErr)
		assert.Error(t, err)
		assert.ErrorIs(t, err, baseErr)
		assert.Equal(t, "rule-1: boom", err.Error())
	})

	t.Run("NewRuleError without rule name", func(t *testing.T) {
		baseErr := errors.New("boom")
		err := NewRuleError("", baseErr)
		assert.Error(t, err)
		assert.Equal(t, "boom", err.Error())
	})

	t.Run("Unwrap RuleError", func(t *testing.T) {
		baseErr := errors.New("boom")
		err := NewRuleError("rule-1", baseErr)
		assert.ErrorIs(t, err, baseErr)
	})
}
