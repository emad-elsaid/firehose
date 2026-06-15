// Package main provides the firehose application entry point.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	fh "github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/cache"
	"github.com/emad-elsaid/firehose/middlewares/actions"
	"github.com/emad-elsaid/firehose/middlewares/callbacks"
	"github.com/emad-elsaid/firehose/middlewares/destinations"
	"github.com/emad-elsaid/firehose/runner"
	"github.com/golang-cz/devslog"
)

func main() {
	port := flag.String("port", ":3000", "HTTP server port")
	flag.Parse()
	slogOpts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}

	// new logger with options
	opts := &devslog.Options{
		HandlerOptions:    slogOpts,
		SortKeys:          true,
		NewLineAfterLog:   true,
		StringerFormatter: true,
	}

	logger := slog.New(devslog.NewHandler(os.Stdout, opts))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	registry := activateRules(ctx)

	errs := make(chan error)
	fh.Start(ctx, registry, errs)

	slog.Info("All sources started, waiting for them to finish...")

	go http.ListenAndServe(*port, nil)
	go fh.Wait(registry, errs)

	for i := range errs {
		if errors.Is(i, context.Canceled) {
			continue
		}

		slog.Error(i.Error())
	}
}

func callbackMw[I, O fh.Event]() []fh.CallbackMiddleware[I, O] {
	return []fh.CallbackMiddleware[I, O]{
		&callbacks.Slog[I, O]{},
		&callbacks.Parallel[I, O]{Runner: runner.Basic{}},
	}
}

func actionMw[I, O fh.Event]() []fh.ActionMiddleware[I, O] {
	return []fh.ActionMiddleware[I, O]{
		&actions.Panic[I, O]{},
		&actions.If[I, O]{},
		&actions.RateLimit[I, O]{},
		&actions.Once[I, O]{Cache: cache.NewMemory[string](time.Minute, time.Minute)},
		&actions.Cache[I, O]{Cache: cache.NewMemory[O](time.Minute, time.Minute)},
	}
}

func destinationMw[I, O fh.Event]() []fh.DestinationMiddleware[I, O] {
	return []fh.DestinationMiddleware[I, O]{
		&destinations.Panic[I, O]{},
	}
}

func addRule[I, O fh.Event](
	ctx context.Context,
	reg fh.Registry,
	rule *fh.Rule[I, O],
	in I,
	out O,
) fh.Registry {
	return must(
		fh.AddRule(
			ctx, reg, rule,
			callbackMw[I, O],
			actionMw[I, O],
			destinationMw[I, O],
			in, out,
		),
	)
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}
