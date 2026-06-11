package destinations

import (
	"context"

	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/events"
)

type Score struct {
	To *int
}

func (s Score) Send(ctx context.Context, event events.AddScore) firehose.Report {
	*s.To += event.Amount

	return firehose.NewReport(firehose.StatusSuccess, nil)
}
