package firehose

// Status represents the outcome of processing an event through a rule,
// indicating whether it was successful, if there was an error, or if there was
// no match.
type Status string

// Predefined status values for processing results.
const (
	StatusSuccess          Status = "Success"
	StatusError            Status = "Error"
	StatusActionError      Status = "Action error"
	StatusDestinationError Status = "Destination error"
	StatusConditionError   Status = "Condition error"
	StatusNoMatch          Status = "No match"
)

// Report represents the result of processing an event through a rule, including the status and any error that occurred.
type Report struct {
	Rule   string
	Status Status
	Abort  bool
	Err    error
}

// NewSuccessReport creates a new Report for a successful operation.
func NewSuccessReport() Report {
	return Report{
		Rule:   "",
		Status: StatusSuccess,
		Err:    nil,
		Abort:  false,
	}
}

// NewReport creates a new Report with the given status and error.
func NewReport(status Status, err error) Report {
	return Report{
		Rule:   "",
		Status: status,
		Err:    err,
		Abort:  false,
	}
}

// NewRuleReport creates a new Report with the given rule, status, and error.
func NewRuleReport(rule string, status Status, err error) Report {
	return Report{
		Rule:   rule,
		Status: status,
		Err:    err,
		Abort:  false,
	}
}

// NewAbortReport creates a new Report with the given status and error, setting Abort to true
// to signal that processing should halt.
func NewAbortReport(status Status, err error) Report {
	return Report{
		Rule:   "",
		Status: status,
		Err:    err,
		Abort:  true,
	}
}

func (r Report) String() string {
	if r.Err == nil {
		return string(r.Status) + " " + r.Rule
	}

	return string(r.Status) + "  " + r.Rule + ": " + r.Err.Error()
}
