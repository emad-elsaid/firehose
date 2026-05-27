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
	printTime := firehose.Rule[events.Time, events.Time]{
		When: sources.Time{Period: 1 * time.Second},
		If:   "",
		Then: actions.Yield[events.Time]{},
		To:   destinations.Stdout[events.Time]{},
	}

	ctx := context.Background()

	ctx, cancel := context.WithTimeout(ctx, timeoutDuration)
	defer cancel()

	ctx, err := firehose.AddRule(ctx, printTime)
	if err != nil {
		panic(err)
	}

	err = firehose.Start(ctx)
	if err != nil {
		panic(err)
	}
}
