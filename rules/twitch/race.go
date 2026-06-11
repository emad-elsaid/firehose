package twitch

import (
	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/actions"
	"github.com/emad-elsaid/firehose/destinations"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/sources"
)

var Race = firehose.Rule[events.HTTPReq, events.HTTPRes]{
	ID:   "race",
	When: sources.HTTP{Endpoint: `/race`},
	If:   ``,
	Then: actions.TwitchRace{
		Me:   &MeScore,
		Chat: &chatScore,
	},
	To: destinations.HTTP{},
}
