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
				Title: "Send Help I'm Being Chased",
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

	mk1 := &firehose.Rule[events.Process, events.TwitchStreamInfo]{
		When: sources.Process{},
		If:   `cmd = "S:\\common\\Mortal Kombat 1\\MK12\\Binaries\\Win64\\MK12.exe\x00MK12"`,
		Then: actions.Event[events.Process, events.TwitchStreamInfo]{
			Output: events.TwitchStreamInfo{
				Title: "Call an ambulance",
				Game:  "Mortal Kombat 1",
				Tags:  []string{"English", "gaming", "linux"},
			},
		},
		To: destinations.TwitchStreamInfo{},
	}

	emacs := &firehose.Rule[events.Process, events.TwitchStreamInfo]{
		When: sources.Process{},
		If:   `cmd = "/usr/bin/emacs"`,
		Then: actions.Event[events.Process, events.TwitchStreamInfo]{
			Output: events.TwitchStreamInfo{
				Title: "Linux Go Coding No AI",
				Game:  "Software and Game Development",
				Tags:  []string{"English", "coding", "linux", "programming"},
			},
		},
		To: destinations.TwitchStreamInfo{},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	r := must(firehose.AddRule(ctx, nil, haveANiceDeath, events.Process{}))
	r = must(firehose.AddRule(ctx, r, deadCells, events.Process{}))
	r = must(firehose.AddRule(ctx, r, mk1, events.Process{}))
	r = must(firehose.AddRule(ctx, r, emacs, events.Process{}))

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
