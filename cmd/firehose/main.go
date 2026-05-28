// Package main provides the firehose application entry point.
package main

import (
	"context"
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
		If:   "",
		Then: actions.Yield[events.Time]{},
		To:   destinations.Stdout[events.Time]{},
	}

	printTime2 := &firehose.Rule[events.Time, events.Time]{
		When: sources.Time{Period: 1 * time.Second},
		If:   "",
		Then: actions.Yield[events.Time]{},
		To:   destinations.Stdout[events.Time]{},
	}

	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, timeoutDuration)
	defer cancel()

	r := must(firehose.AddRule(nil, printTime))
	r = must(firehose.AddRule(r, printTime2))

	err := firehose.Start(ctx, r)
	if err != nil {
		panic(err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}
