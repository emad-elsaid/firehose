# Quick Start

This guide walks you through creating your first Firehose event pipeline.

## Installation

```bash
go get github.com/emad-elsaid/firehose
```

## Basic Example

Let's build a timer that prints messages during business hours:

### 1. Define Your Event Type

```go
type Tick struct {
    Time time.Time
}
```

### 2. Make It Conditionally Evaluable (Optional)

Implement the `boolexpr.Symbols` interface to expose attributes for conditions:

```go
func (t Tick) Get(key string) (any, error) {
    if key == "hour" {
        return t.Time.Hour(), nil
    }
    return nil, fmt.Errorf("unknown symbol: %s", key)
}
```

### 3. Implement an Event Source

Sources produce events and send them to a callback:

```go
type Timer struct {
    Interval time.Duration
}

func (t Timer) Start(ctx context.Context, cb fh.Callback[Tick]) (<-chan struct{}, error) {
    go func() {
        ticker := time.NewTicker(t.Interval)
        defer ticker.Stop()

        for {
            select {
            case <-ctx.Done():
                return
            case now := <-ticker.C:
                cb(ctx, Tick{Time: now}, func(err error) {
                    if err != nil {
                        log.Printf("rule failed: %v", err)
                    }
                })
            }
        }
    }()
    return ctx.Done(), nil
}
```

### 4. Implement a Transformation

Actions transform input events to output events:

```go
type FormatTime struct{}

func (FormatTime) Process(
    ctx context.Context,
    t Tick,
    _ boolexpr.Symbols,
) (string, error) {
    return t.Time.Format("15:04:05"), nil
}
```

### 5. Implement a Destination

Destinations consume events and produce side effects:

```go
type Printer struct{}

func (Printer) Send(ctx context.Context, msg string) error {
    println(msg)
    return nil
}
```

### 6. Define and Register a Rule

```go
func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()

    rule := &fh.SQLRule[Tick, string]{
        ID:   "print_business_hours",
        Select: FormatTime{},
        Into:   Printer{},
        From:   Timer{Interval: 1 * time.Second},
        Where:   condition.Cond[Tick]("hour >= 9 and hour < 17"),
    }

    head, err := fh.Add(ctx, nil, rule)
    if err != nil {
        log.Fatal(err)
    }

    errHandler := func(err error) {
        if err != nil && !errors.Is(err, context.Canceled) {
            log.Printf("engine error: %v", err)
        }
    }

    doneChannels := fh.Start(ctx, head, errHandler)
    for _, ch := range doneChannels {
        <-ch
    }
}
```

## Complete Example

<details>
<summary>Click to see the complete code</summary>

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "log"
    "os"
    "os/signal"
    "time"

    "github.com/emad-elsaid/boolexpr"
    fh "github.com/emad-elsaid/firehose"
    "github.com/emad-elsaid/firehose/condition"
)

type Tick struct {
    Time time.Time
}

func (t Tick) Get(key string) (any, error) {
    if key == "hour" {
        return t.Time.Hour(), nil
    }
    return nil, fmt.Errorf("unknown symbol: %s", key)
}

type Timer struct {
    Interval time.Duration
}

func (t Timer) Start(ctx context.Context, cb fh.Callback[Tick]) (<-chan struct{}, error) {
    go func() {
        ticker := time.NewTicker(t.Interval)
        defer ticker.Stop()
        for {
            select {
            case <-ctx.Done():
                return
            case now := <-ticker.C:
                cb(ctx, Tick{Time: now}, func(err error) {
                    if err != nil {
                        log.Printf("rule failed: %v", err)
                    }
                })
            }
        }
    }()
    return ctx.Done(), nil
}

type FormatTime struct{}

func (FormatTime) Process(ctx context.Context, t Tick, _ boolexpr.Symbols) (string, error) {
    return t.Time.Format("15:04:05"), nil
}

type Printer struct{}

func (Printer) Send(ctx context.Context, msg string) error {
    println(msg)
    return nil
}

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()

    rule := &fh.SQLRule[Tick, string]{
        ID:   "print_business_hours",
        Select: FormatTime{},
        Into:   Printer{},
        From:   Timer{Interval: 1 * time.Second},
        Where:   condition.Cond[Tick]("hour >= 9 and hour < 17"),
    }

    head, _ := fh.Add(ctx, nil, rule)

    errHandler := func(err error) {
        if err != nil && !errors.Is(err, context.Canceled) {
            log.Printf("engine error: %v", err)
        }
    }

    doneChannels := fh.Start(ctx, head, errHandler)
    for _, ch := range doneChannels {
        <-ch
    }
}
```
</details>

### Alternative Naming Conventions

The same pipeline can be expressed with BDD, Kafka Streams, or MapReduce naming:

```go
// BDD convention: Given → When → Then → GivenOutput → To
rule := &fh.ScenarioRule[Tick, string]{
    ID:          "print_business_hours",
    When:        Timer{Interval: 1 * time.Second},
    Given:       condition.Cond[Tick]("hour >= 9 and hour < 17"),
    Then:        FormatTime{},
    GivenOutput: condition.Cond[string](`msg != "12:00:00"`), // skip lunch
    To:          Printer{},
}

// Kafka Streams convention: Source → Filter → Map → FilterOutput → Sink
rule := &fh.StreamRule[Tick, string]{
    ID:           "print_business_hours",
    Source:       Timer{Interval: 1 * time.Second},
    Filter:       condition.Cond[Tick]("hour >= 9 and hour < 17"),
    Map:          FormatTime{},
    FilterOutput: condition.Cond[string](`msg != "12:00:00"`),
    Sink:         Printer{},
}

// MapReduce convention: Source → Filter → Map → Reduce → FilterOutput → Sink
rule := &fh.MapReduceRule[Tick, string, Metric]{
    ID:           "print_business_hours",
    Source:       Timer{Interval: 1 * time.Second},
    Filter:       condition.Cond[Tick]("hour >= 9 and hour < 17"),
    Map:          FormatTime{},
    Reduce:       &Accumulator{},
    FilterOutput: condition.Cond[Metric](`count > 10`),
    Sink:         Printer{},
}
```

`SQLRule`, `ScenarioRule`, and `StreamRule` share the same engine — they can be
mixed freely in the same pipeline and use identical components for sources,
conditions, actions, and destinations. `MapReduceRule` adds a Reduce stage for
stateful accumulation across events.

## What's Next?

- Learn about [Core Concepts](/guide/concepts)
- Explore [Built-in Components](/guide/components)
- See [Real-World Examples](/examples/)
- Read the [API Reference](/api/)
