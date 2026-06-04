package firehose

import (
	"sync"
	"sync/atomic"
)

type Status int64

var statusCounter atomic.Int64
var statusNames sync.Map

func NewStatus(name string) Status {
	status := Status(statusCounter.Add(1))
	statusNames.Store(status, name)

	return status
}

var (
	StatusNoMatch          Status = NewStatus("No match")
	StatusSuccess                 = NewStatus("Success")
	StatusError                   = NewStatus("Error")
	StatusConditionError          = NewStatus("Condition error")
	StatusActionError             = NewStatus("Action error")
	StatusDestinationError        = NewStatus("Destination error")
)

type Report struct {
	Status Status
	Err    error
}

func (r Report) String() string {
	name, _ := statusNames.Load(r.Status)
	if r.Err != nil {
		return name.(string) + ": " + r.Err.Error()
	}

	return name.(string)
}

func NewReport(status Status, err error) Report {
	return Report{
		Status: status,
		Err:    err,
	}
}
