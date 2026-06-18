package rules

import (
	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/actions"
	"github.com/emad-elsaid/firehose/destinations"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/sources"
)

type ProcessRule = firehose.Rule[events.Process, events.TwitchStreamInfo]
type TwitchUpdate = actions.Event[events.Process, events.TwitchStreamInfo]

var Games = ProcessRule{
	ID:   "games",
	When: sources.Process{},
	To:   destinations.TwitchStreamInfo{},

	SubRules: []ProcessRule{
		{
			ID: "dead_cells",
			If: `cmd contains "deadcells"`,
			Then: TwitchUpdate{
				Output: events.TwitchStreamInfo{
					Title: "Playing Dead Cells",
					Game:  "Dead Cells",
					Tags:  []string{},
				},
			},
		},
		{
			ID: "have_a_nice_death",
			If: `cmd contains "HaveaNiceDeath"`,
			Then: TwitchUpdate{
				Output: events.TwitchStreamInfo{
					Title: "Send Help I'm Being Chased",
					Game:  "Have a Nice Death",
					Tags:  []string{},
				},
			},
		},
		{
			ID: "mortal_kombat_1",
			If: `cmd contains "Mortal Kombat 1"`,
			Then: TwitchUpdate{
				Output: events.TwitchStreamInfo{
					Title: "Call an ambulance",
					Game:  "Mortal Kombat 1",
					Tags:  []string{},
				},
			},
		},
	},
}
