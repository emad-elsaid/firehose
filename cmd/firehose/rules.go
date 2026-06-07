package main

import (
	"context"

	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/rules/apps"
	"github.com/emad-elsaid/firehose/rules/games"
	"github.com/emad-elsaid/firehose/rules/logging"
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
		actionMiddlewares[events.Process, events.TwitchStreamInfo],
		destinationMiddlewares[events.Process, events.TwitchStreamInfo],
		exampleProc,
		exampleTSI,
	))

	registry = must(firehose.AddRule(ctx, registry,
		&games.DeadCells,
		actionMiddlewares[events.Process, events.TwitchStreamInfo],
		destinationMiddlewares[events.Process, events.TwitchStreamInfo],
		exampleProc,
		exampleTSI,
	))

	registry = must(firehose.AddRule(ctx, registry,
		&games.MortalKombat1,
		actionMiddlewares[events.Process, events.TwitchStreamInfo],
		destinationMiddlewares[events.Process, events.TwitchStreamInfo],
		exampleProc,
		exampleTSI,
	))

	registry = must(firehose.AddRule(ctx, registry,
		&apps.Emacs,
		actionMiddlewares[events.Process, events.TwitchStreamInfo],
		destinationMiddlewares[events.Process, events.TwitchStreamInfo],
		exampleProc,
		exampleTSI,
	))

	registry = must(firehose.AddRule(ctx, registry,
		&logging.Process,
		actionMiddlewares[events.Process, events.Process],
		destinationMiddlewares[events.Process, events.Process],
		exampleProc,
		exampleProc,
	))

	return registry
}
