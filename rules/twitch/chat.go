package twitch

import (
	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/actions"
	"github.com/emad-elsaid/firehose/destinations"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/sources"
)

var chatScore int = 0

var Chat = firehose.Rule[events.TwitchMessage, events.AddScore]{
	ID: "chat",
	When: sources.TwitchChat{
		Channel: "internalerr",
	},
	If:   ``,
	Then: actions.TwitchScore{},
	To: destinations.Score{
		To: &chatScore,
	},
}
