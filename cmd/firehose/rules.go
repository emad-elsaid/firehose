package main

import (
	"context"

	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/rules/apps"
	"github.com/emad-elsaid/firehose/rules/games"
)

func activateRules(ctx context.Context) firehose.Registry {
	exampleProc := events.Process{PID: 0}
	exampleTSI := events.TwitchStreamInfo{
		Title: "",
		Game:  "",
		Tags:  []string{},
	}

	registry := must(firehose.AddRule(
		ctx,
		nil,
		&games.HaveANiceDeath,
		callbackMiddlewares[events.Process, events.TwitchStreamInfo],
		actionMiddlewares[events.Process, events.TwitchStreamInfo],
		destinationMiddlewares[events.Process, events.TwitchStreamInfo],
		exampleProc,
		exampleTSI,
	))

	registry = must(firehose.AddRule(ctx, registry,
		&games.DeadCells,
		callbackMiddlewares[events.Process, events.TwitchStreamInfo],
		actionMiddlewares[events.Process, events.TwitchStreamInfo],
		destinationMiddlewares[events.Process, events.TwitchStreamInfo],
		exampleProc,
		exampleTSI,
	))

	registry = must(firehose.AddRule(ctx, registry,
		&games.MortalKombat1,
		callbackMiddlewares[events.Process, events.TwitchStreamInfo],
		actionMiddlewares[events.Process, events.TwitchStreamInfo],
		destinationMiddlewares[events.Process, events.TwitchStreamInfo],
		exampleProc,
		exampleTSI,
	))

	registry = must(firehose.AddRule(ctx, registry,
		&apps.Emacs,
		callbackMiddlewares[events.Process, events.TwitchStreamInfo],
		actionMiddlewares[events.Process, events.TwitchStreamInfo],
		destinationMiddlewares[events.Process, events.TwitchStreamInfo],
		exampleProc,
		exampleTSI,
	))

	return registry
}
