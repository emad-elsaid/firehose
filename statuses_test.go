package firehose

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReportConstructors(t *testing.T) {
	tests := []struct {
		name           string
		constructor    func() Report
		expectedRule   string
		expectedStatus Status
		expectedErr    error
	}{
		{
			name:           "NewRuleReport with success",
			constructor:    func() Report { return NewRuleReport("test-rule", StatusSuccess, nil) },
			expectedRule:   "test-rule",
			expectedStatus: StatusSuccess,
			expectedErr:    nil,
		},
		{
			name:           "NewRuleReport with error",
			constructor:    func() Report { return NewRuleReport("error-rule", StatusError, errors.New("test error")) },
			expectedRule:   "error-rule",
			expectedStatus: StatusError,
			expectedErr:    errors.New("test error"),
		},
		{
			name:           "NewReport with error",
			constructor:    func() Report { return NewReport(StatusError, errors.New("critical")) },
			expectedRule:   "",
			expectedStatus: StatusError,
			expectedErr:    errors.New("critical"),
		},
		{
			name:           "NewReport with success",
			constructor:    func() Report { return NewReport(StatusSuccess, nil) },
			expectedRule:   "",
			expectedStatus: StatusSuccess,
			expectedErr:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			report := tc.constructor()

			assert.Equal(t, tc.expectedRule, report.Rule)
			assert.Equal(t, tc.expectedStatus, report.Status)

			if tc.expectedErr != nil {
				require.Error(t, report.Err)
				assert.Equal(t, tc.expectedErr.Error(), report.Err.Error())
			} else {
				assert.NoError(t, report.Err)
			}
		})
	}
}

func TestReport_String(t *testing.T) {
	tests := []struct {
		name     string
		report   Report
		expected string
	}{
		{
			name:     "success without error",
			report:   Report{Rule: "my-rule", Status: StatusSuccess, Err: nil},
			expected: "Success my-rule",
		},
		{
			name:     "error with message",
			report:   Report{Rule: "error-rule", Status: StatusError, Err: errors.New("failed")},
			expected: "Error  error-rule: failed",
		},
		{
			name:     "action error with message",
			report:   Report{Rule: "action-rule", Status: StatusActionError, Err: errors.New("action failed")},
			expected: "Action error  action-rule: action failed",
		},
		{
			name:     "no match without error",
			report:   Report{Rule: "no-match", Status: StatusNoMatch, Err: nil},
			expected: "No match no-match",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.report.String()
			assert.Equal(t, tc.expected, result)
		})
	}
}
