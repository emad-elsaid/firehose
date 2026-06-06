package firehose

import "errors"

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
	StatusPanicRecovered   Status = "Panic recovered"
)

// ErrPanicRecovered is a static error for panic recovery in actions and destinations.
var ErrPanicRecovered = errors.New("panic recovered")

// Report represents the result of processing an event through a rule, including the status and any error that occurred.
type Report struct {
	Status Status
	Abort  bool
	Err    error
}

// NewReport creates a new Report with the given status and error.
func NewReport(status Status, err error) Report {
	return Report{
		Status: status,
		Err:    err,
		Abort:  false,
	}
}

// NewAbortReport creates a new Report with the given status and error, setting Abort to true
// to signal that processing should halt.
func NewAbortReport(status Status, err error) Report {
	return Report{
		Status: status,
		Err:    err,
		Abort:  true,
	}
}
