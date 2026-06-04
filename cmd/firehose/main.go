// Package main provides the firehose application entry point.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"

	"github.com/emad-elsaid/firehose"
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

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}
