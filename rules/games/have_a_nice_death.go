package games

import (
	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/actions"
	"github.com/emad-elsaid/firehose/destinations"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/sources"
)

var HaveANiceDeath = firehose.Rule[events.Process, events.TwitchStreamInfo]{
	ID:   "have_a_nice_death",
	When: sources.Process{},
	If:   `cmd = "S:\\common\\Have A Nice Death\\HaveaNiceDeath.exe"`,
	Then: actions.Event[events.Process, events.TwitchStreamInfo]{
		Output: events.TwitchStreamInfo{
			Title: "Send Help I'm Being Chased",
			Game:  "Have a Nice Death",
			Tags:  []string{},
		},
	},
	To: destinations.TwitchStreamInfo{},
}
