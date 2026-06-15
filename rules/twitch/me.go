package twitch

import (
	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/actions"
	"github.com/emad-elsaid/firehose/destinations"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/sources"
)

var MeScore int = 0

var Me = firehose.Rule[events.KeyPress, events.AddScore]{
	ID: "me",
	When: sources.Keyboard{
		EventDevice: "/dev/input/event14",
	},
	If:   ``,
	Then: actions.KeyPressToAddScore{},
	To: destinations.Score{
		To: &MeScore,
	},
}
