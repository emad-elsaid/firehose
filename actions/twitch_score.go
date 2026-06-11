package actions

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/events"
)

type TwitchScore struct{}

func (t TwitchScore) Process(ctx context.Context, event events.TwitchMessage, syms boolexpr.Symbols) (events.AddScore, fh.Report) {
	return events.AddScore{
		Amount: len(event.Message),
	}, fh.NewReport(fh.StatusSuccess, nil)
}
