package games

import (
	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/actions"
	"github.com/emad-elsaid/firehose/destinations"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/sources"
)

var MortalKombat1 = firehose.Rule[events.Process, events.TwitchStreamInfo]{
	When: sources.Process{},
	If:   `cmd = "S:\\common\\Mortal Kombat 1\\MK12\\Binaries\\Win64\\MK12.exe\x00MK12"`,
	Then: actions.Event[events.Process, events.TwitchStreamInfo]{
		Output: events.TwitchStreamInfo{
			Title: "Call an ambulance",
			Game:  "Mortal Kombat 1",
			Tags:  []string{},
		},
	},
	To: destinations.TwitchStreamInfo{},
}
