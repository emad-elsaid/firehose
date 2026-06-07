package apps

import (
	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/actions"
	"github.com/emad-elsaid/firehose/destinations"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/sources"
)

var Emacs = firehose.Rule[events.Process, events.TwitchStreamInfo]{
	ID:   "emacs",
	When: sources.Process{},
	If:   `cmd = "/usr/bin/emacs"`,
	Then: actions.Event[events.Process, events.TwitchStreamInfo]{
		Output: events.TwitchStreamInfo{
			Title: "Linux Go Coding No AI",
			Game:  "Software and Game Development",
			Tags:  []string{},
		},
	},
	To: destinations.TwitchStreamInfo{},
}
