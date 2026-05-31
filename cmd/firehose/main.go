// Package main provides the firehose application entry point.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/actions"
	"github.com/emad-elsaid/firehose/destinations"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/firehose/sources"
)

const timeoutDuration = 5 * time.Second

func main() {
	printTime := &firehose.Rule[events.Time, events.Time]{
		When: sources.Time{Period: 1 * time.Second},
		If:   "seconds < 3",
		Then: actions.Yield[events.Time]{},
		To:   destinations.Stdout[events.Time]{},
	}

	printTime2 := &firehose.Rule[events.Time, events.Time]{
		When: sources.Time{Period: 1 * time.Second},
		If:   "seconds > 3",
		Then: actions.Yield[events.Time]{},
		To:   destinations.Stdout[events.Time]{},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, timeoutDuration)
	defer cancel()

	r := must(firehose.AddRule(ctx, nil, printTime, events.Time{}))
	r = must(firehose.AddRule(ctx, r, printTime2, events.Time{}))

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
