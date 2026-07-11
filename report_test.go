package firehose

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReportConstructors(t *testing.T) {
	t.Run("NewSuccessReport", func(t *testing.T) {
		report := NewSuccessReport()
		assert.Empty(t, report.Rule)
		assert.NoError(t, report.Err)
	})

	t.Run("NewReport", func(t *testing.T) {
		err := errors.New("boom")
		report := NewReport(err)
		assert.Empty(t, report.Rule)
		assert.ErrorIs(t, report.Err, err)
	})

	t.Run("NewRuleReport", func(t *testing.T) {
		err := errors.New("boom")
		report := NewRuleReport("rule-1", err)
		assert.Equal(t, "rule-1", report.Rule)
		assert.ErrorIs(t, report.Err, err)
	})
}

func TestReportString(t *testing.T) {
	assert.Equal(t, "Success", NewReport(nil).String())
	assert.Equal(t, "Success my-rule", NewRuleReport("my-rule", nil).String())
	assert.Equal(t, "my-rule: failed", NewRuleReport("my-rule", errors.New("failed")).String())
}
