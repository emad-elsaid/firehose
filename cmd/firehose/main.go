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

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ctx, err := firehose.AddRule(ctx, printTime)
	if err != nil {
		panic(err)
	}

	firehose.Start(ctx)
}

var printTime = firehose.Rule[events.Time, events.Time]{
	When: sources.Time{Period: 1 * time.Second},
	Then: actions.Yield[events.Time]{},
	To:   destinations.Stdout[events.Time]{},
}
