package games

import (
	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/actions"
	"github.com/emad-elsaid/firehose/destinations"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/sources"
)

var DeadCells = firehose.Rule[events.Process, events.TwitchStreamInfo]{
	ID:   "dead_cells",
	When: sources.Process{},
	If:   `cmd = "./deadcells"`,
	Then: actions.Event[events.Process, events.TwitchStreamInfo]{
		Output: events.TwitchStreamInfo{
			Title: "Playing Dead Cells",
			Game:  "Dead Cells",
			Tags:  []string{},
		},
	},
	To: destinations.TwitchStreamInfo{},
}
