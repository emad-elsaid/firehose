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

	processRule := &firehose.Rule[events.Process, events.Process]{
		When: sources.Process{},
		If:   `cmd = "S:\\common\\Have A Nice Death\\HaveaNiceDeath.exe"`,
		Then: actions.Yield[events.Process]{},
		To: destinations.Slog[events.Process]{
			Level:   slog.LevelInfo,
			Message: "New process",
		},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	r := must(firehose.AddRule(ctx, nil, processRule, events.Process{}))

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
