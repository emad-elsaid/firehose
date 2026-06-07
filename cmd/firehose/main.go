// Package main provides the firehose application entry point.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"

	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/middlewares/actions"
	"github.com/emad-elsaid/firehose/middlewares/destinations"
	"github.com/golang-cz/devslog"
)

// Popular tags.
const (
	english = "English"
	gaming  = "gaming"
	linux   = "linux"
)

func main() {
	logger := slog.New(devslog.NewHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	registry := activateRules(ctx)

	errs := make(chan error)
	firehose.Start(ctx, registry, errs)

	slog.Info("All sources started, waiting for them to finish...")

	go firehose.Wait(registry, errs)

	for i := range errs {
		if errors.Is(i, context.Canceled) {
			continue
		}

		slog.Error(i.Error())
	}
}

func actionMiddlewares[In, Out firehose.Event]() []firehose.ActionMiddleware[In, Out] {
	return []firehose.ActionMiddleware[In, Out]{
		&actions.Panic[In, Out]{},
		&actions.If[In, Out]{},
	}
}

func destinationMiddlewares[In, Out firehose.Event]() []firehose.DestinationMiddleware[In, Out] {
	return []firehose.DestinationMiddleware[In, Out]{
		&destinations.Panic[In, Out]{},
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}
