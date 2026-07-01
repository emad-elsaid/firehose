package firehose

import "errors"

// Sentinel errors used for control flow and classification.
var (
	ErrNoMatch = errors.New("no match")
)

// ConditionError wraps an error that occurred during If evaluation.
type ConditionError struct{ Err error }

func (e ConditionError) Error() string {
	if e.Err == nil {
		return "condition"
	}

	return "condition: " + e.Err.Error()
}

func (e ConditionError) Unwrap() error { return e.Err }

// ActionError wraps an error returned by Action.Process.
type ActionError struct{ Err error }

func (e ActionError) Error() string {
	if e.Err == nil {
		return "action"
	}

	return "action: " + e.Err.Error()
}

func (e ActionError) Unwrap() error { return e.Err }

// DestinationError wraps an error returned by Destination.Send.
type DestinationError struct{ Err error }

func (e DestinationError) Error() string {
	if e.Err == nil {
		return "destination"
	}

	return "destination: " + e.Err.Error()
}

func (e DestinationError) Unwrap() error { return e.Err }

// Report represents the result of processing an event through a rule.
type Report struct {
	Rule string
	Err  error
}

// NewSuccessReport creates a new Report for a successful operation.
func NewSuccessReport() Report {
	return Report{Rule: "", Err: nil}
}

// NewReport creates a new Report with the given error.
func NewReport(err error) Report {
	return Report{Rule: "", Err: err}
}

// NewRuleReport creates a new Report with the given rule and error.
func NewRuleReport(rule string, err error) Report {
	return Report{Rule: rule, Err: err}
}

func (r Report) String() string {
	if r.Err == nil {
		if r.Rule == "" {
			return "Success"
		}

		return "Success " + r.Rule
	}

	if r.Rule == "" {
		return r.Err.Error()
	}

	return r.Rule + ": " + r.Err.Error()
}
