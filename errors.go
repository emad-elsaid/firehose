package firehose

import "errors"

// Sentinel errors used for control flow and classification.
var (
	ErrInputNoMatch  = errors.New("no match")
	ErrOutputNoMatch = errors.New("output no match")
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

// ReduceError wraps an error returned by Reducer.Reduce.
type ReduceError struct{ Err error }

func (e ReduceError) Error() string {
	if e.Err == nil {
		return "reduce"
	}

	return "reduce: " + e.Err.Error()
}

func (e ReduceError) Unwrap() error { return e.Err }

// DestinationError wraps an error returned by Destination.Send.
type DestinationError struct{ Err error }

func (e DestinationError) Error() string {
	if e.Err == nil {
		return "destination"
	}

	return "destination: " + e.Err.Error()
}

func (e DestinationError) Unwrap() error { return e.Err }
