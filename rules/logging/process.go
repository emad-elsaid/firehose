package logging

import (
	"log/slog"

	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/actions"
	"github.com/emad-elsaid/firehose/destinations"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/sources"
)

var Process = firehose.Rule[events.Process, events.Process]{
	When: sources.Process{},
	If:   ``,
	Then: actions.Yield[events.Process]{},
	To: destinations.Slog[events.Process]{
		Message: "New",
		Level:   slog.LevelInfo,
	},
}
