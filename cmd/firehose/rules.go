package main

import (
	"context"
	"log/slog"

	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/actions"
	"github.com/emad-elsaid/firehose/destinations"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/sources"
)

func activateRules(ctx context.Context) firehose.Registry {
	exampleProc := events.Process{PID: 0}
	exampleTSI := events.TwitchStreamInfo{
		Title: "",
		Game:  "",
		Tags:  []string{},
	}

	registry := must(firehose.AddRule(ctx, nil,
		&firehose.Rule[events.Process, events.TwitchStreamInfo]{
			When: sources.Process{},
			If:   `cmd = "S:\\common\\Have A Nice Death\\HaveaNiceDeath.exe"`,
			Then: actions.Event[events.Process, events.TwitchStreamInfo]{
				Output: events.TwitchStreamInfo{
					Title: "Send Help I'm Being Chased",
					Game:  "Have a Nice Death",
					Tags:  []string{english, gaming, linux},
				},
			},
			To: destinations.TwitchStreamInfo{},
		},
		actionMiddlewares[events.Process, events.TwitchStreamInfo],
		destinationMiddlewares[events.Process, events.TwitchStreamInfo],
		exampleProc,
		exampleTSI,
	))

	registry = must(firehose.AddRule(ctx, registry,
		&firehose.Rule[events.Process, events.TwitchStreamInfo]{
			When: sources.Process{},
			If:   `cmd = "./deadcells"`,
			Then: actions.Event[events.Process, events.TwitchStreamInfo]{
				Output: events.TwitchStreamInfo{
					Title: "Playing Dead Cells",
					Game:  "Dead Cells",
					Tags:  []string{english, gaming, linux},
				},
			},
			To: destinations.TwitchStreamInfo{},
		},
		actionMiddlewares[events.Process, events.TwitchStreamInfo],
		destinationMiddlewares[events.Process, events.TwitchStreamInfo],
		exampleProc,
		exampleTSI,
	))

	registry = must(firehose.AddRule(ctx, registry,
		&firehose.Rule[events.Process, events.TwitchStreamInfo]{
			When: sources.Process{},
			If:   `cmd = "S:\\common\\Mortal Kombat 1\\MK12\\Binaries\\Win64\\MK12.exe\x00MK12"`,
			Then: actions.Event[events.Process, events.TwitchStreamInfo]{
				Output: events.TwitchStreamInfo{
					Title: "Call an ambulance",
					Game:  "Mortal Kombat 1",
					Tags:  []string{english, gaming, linux},
				},
			},
			To: destinations.TwitchStreamInfo{},
		},
		actionMiddlewares[events.Process, events.TwitchStreamInfo],
		destinationMiddlewares[events.Process, events.TwitchStreamInfo],
		exampleProc,
		exampleTSI,
	))

	registry = must(firehose.AddRule(ctx, registry,
		&firehose.Rule[events.Process, events.TwitchStreamInfo]{
			When: sources.Process{},
			If:   `cmd = "/usr/bin/emacs"`,
			Then: actions.Event[events.Process, events.TwitchStreamInfo]{
				Output: events.TwitchStreamInfo{
					Title: "Linux Go Coding No AI",
					Game:  "Software and Game Development",
					Tags:  []string{english, "coding", linux, "programming"},
				},
			},
			To: destinations.TwitchStreamInfo{},
		},
		actionMiddlewares[events.Process, events.TwitchStreamInfo],
		destinationMiddlewares[events.Process, events.TwitchStreamInfo],
		exampleProc,
		exampleTSI,
	))

	registry = must(firehose.AddRule(ctx, registry,
		&firehose.Rule[events.Process, events.Process]{
			When: sources.Process{},
			If:   ``,
			Then: actions.Yield[events.Process]{},
			To: destinations.Slog[events.Process]{
				Message: "New",
				Level:   slog.LevelInfo,
			},
		},
		actionMiddlewares[events.Process, events.Process],
		destinationMiddlewares[events.Process, events.Process],
		exampleProc,
		exampleProc,
	))

	return registry
}
