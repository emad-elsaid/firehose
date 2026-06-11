package firehose

import (
	"fmt"
	"log/slog"
)

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

func (r Report) String() string {
	if r.Err == nil {
		return fmt.Sprintf("%c  %s", r.StatusSymbol(), r.Rule)
	}

	return fmt.Sprintf("%c  %s: %s", r.StatusSymbol(), r.Rule, r.Err)
}

func (r Report) StatusSymbol() rune {
	switch r.Status {
	case StatusSuccess:
		return '✔'
	case StatusError, StatusActionError, StatusDestinationError, StatusConditionError:
		return '✘'
	case StatusNoMatch:
		return '⎯'
	}

	if r.Err != nil {
		return '✘'
	}

	return '?'
}

func (r Report) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("rule", r.Rule),
		slog.String("status", string(r.Status)),
		slog.Bool("abort", r.Abort),
		slog.String("error", r.Err.Error()),
	)
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
