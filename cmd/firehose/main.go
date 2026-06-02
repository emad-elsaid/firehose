// Package main provides the firehose application entry point.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"

	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/actions"
	"github.com/emad-elsaid/firehose/destinations"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/sources"
	"github.com/golang-cz/devslog"
)

func main() {
	logger := slog.New(devslog.NewHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	haveANiceDeath := &firehose.Rule[events.Process, events.TwitchStreamInfo]{
		When: sources.Process{},
		If:   `cmd = "S:\\common\\Have A Nice Death\\HaveaNiceDeath.exe"`,
		Then: actions.Event[events.Process, events.TwitchStreamInfo]{
			Output: events.TwitchStreamInfo{
				Title: "Playing Have a Nice Death",
				Game:  "Have a Nice Death",
				Tags:  []string{"English", "gaming", "linux"},
			},
		},
		To: destinations.TwitchStreamInfo{},
	}

	deadCells := &firehose.Rule[events.Process, events.TwitchStreamInfo]{
		When: sources.Process{},
		If:   `cmd = "./deadcells"`,
		Then: actions.Event[events.Process, events.TwitchStreamInfo]{
			Output: events.TwitchStreamInfo{
				Title: "Playing Dead Cells",
				Game:  "Dead Cells",
				Tags:  []string{"English", "gaming", "linux"},
			},
		},
		To: destinations.TwitchStreamInfo{},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	r := must(firehose.AddRule(ctx, nil, haveANiceDeath, events.Process{}))
	r = must(firehose.AddRule(ctx, r, deadCells, events.Process{}))

	errs := make(chan error)
	go firehose.Start(ctx, r, errs)

	for i := range errs {
		if errors.Is(i, context.Canceled) {
			continue
		}

		slog.Error(i.Error())
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}
