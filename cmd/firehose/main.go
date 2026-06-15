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

func callbackMw[In, Out fh.Event]() []fh.CallbackMiddleware[In, Out] {
	return []fh.CallbackMiddleware[In, Out]{
		&callbacks.Slog[In, Out]{},
	}
}

func actionMw[In, Out fh.Event]() []fh.ActionMiddleware[In, Out] {
	return []fh.ActionMiddleware[In, Out]{
		&actions.Panic[In, Out]{},
		&actions.If[In, Out]{},
		&actions.RateLimit[In, Out]{},
		&actions.Once[In, Out]{Cache: cache.NewMemory[string](time.Minute, time.Minute)},
		&actions.Cache[In, Out]{Cache: cache.NewMemory[Out](time.Minute, time.Minute)},
	}
}

func destinationMw[In, Out fh.Event]() []fh.DestinationMiddleware[In, Out] {
	return []fh.DestinationMiddleware[In, Out]{
		&destinations.Panic[In, Out]{},
	}
}

func addRule[In, Out fh.Event](
	ctx context.Context,
	reg fh.Registry,
	rule *fh.Rule[In, Out],
	in In,
	out Out,
) fh.Registry {
	return must(
		fh.AddRule(
			ctx, reg, rule,
			callbackMw[In, Out],
			actionMw[In, Out],
			destinationMw[In, Out],
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
