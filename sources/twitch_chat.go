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

func (t TwitchChat) Start(ctx context.Context, cb fh.Callback[events.TwitchMessage]) (done context.Context, err error) {
	go t.connect(ctx, cb)

	return ctx, nil
}

func (t TwitchChat) connect(ctx context.Context, cb fh.Callback[events.TwitchMessage]) {
	var client *twitch.Client

	if t.OAuth != "" && t.BotName != "" {
		client = twitch.NewClient(t.BotName, t.OAuth)
	} else {
		client = twitch.NewAnonymousClient()
	}

	reports := make(chan fh.Report)
	go func() {
		for range reports {
		}
	}()

	client.OnPrivateMessage(func(message twitch.PrivateMessage) {
		t.ProcessMessage(ctx, message, cb, reports)
	})

	client.Join(t.Channel)

	err := client.Connect()
	if err != nil {
		slog.Error("Error connecting to twitch", "error", err)
	}
}

func (t TwitchChat) ProcessMessage(ctx context.Context, message twitch.PrivateMessage, cb fh.Callback[events.TwitchMessage], reports chan<- fh.Report) {
	msg := events.TwitchMessage{
		User:    message.User.DisplayName,
		Message: message.Message,
	}

	cb(ctx, msg, reports)
}
