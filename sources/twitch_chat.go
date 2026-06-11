package sources

import (
	"context"
	"log/slog"

	fh "github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/events"
	"github.com/gempir/go-twitch-irc/v4"
)

type TwitchChat struct {
	BotName string
	OAuth   string
	Channel string `validate:"required"`
}

func (t TwitchChat) Start(ctx context.Context, cb fh.SourceCallback[events.TwitchMessage]) (done context.Context, err error) {
	go t.connect(ctx, cb)

	return ctx, nil
}

func (t TwitchChat) connect(ctx context.Context, cb fh.SourceCallback[events.TwitchMessage]) {
	var client *twitch.Client

	if t.OAuth != "" && t.BotName != "" {
		client = twitch.NewClient(t.BotName, t.OAuth)
	} else {
		client = twitch.NewAnonymousClient()
	}

	client.OnPrivateMessage(func(message twitch.PrivateMessage) {
		t.ProcessMessage(ctx, message, cb)
	})

	client.Join(t.Channel)

	err := client.Connect()
	if err != nil {
		slog.Error("Error connecting to twitch", "error", err)
	}
}

func (t TwitchChat) ProcessMessage(ctx context.Context, message twitch.PrivateMessage, cb fh.SourceCallback[events.TwitchMessage]) {
	msg := events.TwitchMessage{
		User:    message.User.DisplayName,
		Message: message.Message,
	}

	reports := cb(ctx, msg)

	for range reports {
	}
}
