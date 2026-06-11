package main

import (
	"context"

	fh "github.com/emad-elsaid/firehose"
	evt "github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/rules/apps"
	"github.com/emad-elsaid/firehose/rules/games"
	"github.com/emad-elsaid/firehose/rules/twitch"
)

func activateRules(ctx context.Context) fh.Registry {
	proc := evt.Process{PID: 0}
	tsi := evt.TwitchStreamInfo{
		Title: "",
		Game:  "",
		Tags:  []string{},
	}
	kp := evt.KeyPress{Key: 0}
	httpReq := evt.HTTPReq{}
	httpRes := evt.HTTPRes{}
	tm := evt.TwitchMessage{}
	as := evt.AddScore{}

	registry := addRule(ctx, nil, &games.HaveANiceDeath, proc, tsi)
	registry = addRule(ctx, registry, &games.DeadCells, proc, tsi)
	registry = addRule(ctx, registry, &games.MortalKombat1, proc, tsi)
	registry = addRule(ctx, registry, &apps.Emacs, proc, tsi)
	registry = addRule(ctx, nil, &twitch.Race, httpReq, httpRes)
	registry = addRule(ctx, registry, &twitch.Me, kp, as)
	registry = addRule(ctx, registry, &twitch.Chat, tm, as)

	return registry
}
