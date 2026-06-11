package actions

import (
	"context"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/events"
)

type KeyPressToAddScore struct{}

func (k KeyPressToAddScore) Process(ctx context.Context, event events.KeyPress, syms boolexpr.Symbols) (events.AddScore, fh.Report) {
	return events.AddScore{
		Amount: 1,
	}, fh.NewReport(fh.StatusSuccess, nil)
}
