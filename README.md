Firehose
========

A type-safe event-driven business rules engine framework in Go. Process events through composable pipelines with
middleware support for panic recovery, conditional execution, rate limiting, and structured logging.


## Problem

Many systems react to events (HTTP requests, process spawns, chat messages, timers) with preconditions and side
effects. Without structure, this pattern produces tight coupling, manual instrumentation, and brittle code that's
difficult to maintain or parallelize.

## Solution

Firehose enforces a clear pattern: **Source → Condition → Action → Destination**

- **Source**: Event producer (HTTP server, process monitor, Twitch chat, timer, keyboard)
- **Condition**: Boolean expression evaluated against event attributes (`hour >= 9 and blocked = false`)
- **Action**: Event transformer (e.g., extract data, calculate scores, convert formats)
- **Destination**: Side-effect applicator (database write, HTTP response, Twitch API call, stdout)

A **Rule** combines these four components into a type-safe pipeline. Middleware wraps rules to add cross-cutting
concerns like logging, panic recovery, rate limiting, and caching.


## Core Concepts

### Type-Safe Pipeline

Rules are strongly typed with input and output event types:

```go
Rule[events.TwitchMessage, events.AddScore]
```

This ensures compile-time type safety from source through action to destination.

### Event Fanout

Multiple rules sharing the same source instance form a linked chain. The source starts once, fanning out events to
all registered rules efficiently.

### Middleware Composition

Middlewares wrap components in reverse registration order (last registered wraps first), creating layered execution:

```
Panic → If (condition) → RateLimit → Action → Destination
```

### Report-Based Error Handling

Operations return `Report{Status, Error, Abort}` instead of panicking. The `Abort` flag signals whether to stop
processing remaining rules.


## Features

- ✅ Type-safe generic pipelines (`Rule[In, Out]`)
- ✅ Boolean expression conditions using event attributes
- ✅ Panic recovery middleware (actions and destinations)
- ✅ Rate limiting with token bucket algorithm
- ✅ Structured logging via `log/slog`
- ✅ Same-source event fanout optimization
- ✅ Circular registry for efficient rule management
- ✅ Validation framework with `go-playground/validator`
- ✅ Context-based state passing and cancellation


## Available Components

### Sources
- `sources.Time` - Periodic timer events
- `sources.Process` - Linux process creation monitor (polls `/proc`)
- `sources.TwitchChat` - Twitch IRC chat messages
- `sources.HTTP` - HTTP endpoint handler
- `sources.Keyboard` - Linux input device reader (`/dev/input/eventX`)

### Actions
- `actions.Yield[T]` - Pass-through (no transformation)
- `actions.Event[In, Out]` - Emit static event
- `actions.TwitchScore` - Calculate score from Twitch message
- `actions.KeypressToAddScore` - Convert keypresses to scores

### Destinations
- `destinations.Stdout[T]` - Print to standard output
- `destinations.Slog[T]` - Structured logging
- `destinations.HTTP` - Write HTTP response
- `destinations.TwitchStreamInfo` - Update Twitch stream metadata
- `destinations.Score` - Update in-memory score counter

### Middlewares

**Action Middlewares:**
- `actions.If` - Conditional execution (boolean expressions)
- `actions.Panic` - Panic recovery
- `actions.RateLimit` - Token bucket rate limiting

**Callback Middlewares:**
- `callbacks.Slog` - Event and report logging

**Destination Middlewares:**
- `destinations.Panic` - Panic recovery


## Quick Start

```go
package main

import (
    "context"
    "os"
    "os/signal"
    "time"

    fh "github.com/emad-elsaid/firehose"
    "github.com/emad-elsaid/firehose/actions"
    "github.com/emad-elsaid/firehose/destinations"
    "github.com/emad-elsaid/firehose/events"
    "github.com/emad-elsaid/firehose/middlewares/actions"
    "github.com/emad-elsaid/firehose/sources"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
    defer stop()

    // Define middleware factories
    actionMw := func() []fh.ActionMiddleware[events.Time, events.Time] {
        return []fh.ActionMiddleware[events.Time, events.Time]{
            &actions.Panic[events.Time, events.Time]{},
            &actions.If[events.Time, events.Time]{},
        }
    }

    // Define a rule
    printTime := &fh.Rule[events.Time, events.Time]{
        ID:   "print_time",
        When: sources.Time{Period: 1 * time.Second},
        If:   "hour >= 9 and hour < 17", // Only during work hours
        Then: actions.Yield[events.Time]{},
        To:   destinations.Stdout[events.Time]{},
    }

    // Register the rule
    registry, err := fh.AddRule(
        ctx,
        nil, // First rule, no existing registry
        printTime,
        func() []fh.CallbackMiddleware[events.Time, events.Time] { return nil },
        actionMw,
        func() []fh.DestinationMiddleware[events.Time, events.Time] { return nil },
        events.Time{}, // Input instance
        events.Time{}, // Output instance
    )
    if err != nil {
        panic(err)
    }

    // Start all sources
    errs := make(chan error)
    fh.Start(ctx, registry, errs)

    // Wait for completion
    go fh.Wait(registry, errs)
    for err := range errs {
        if err != nil && err != context.Canceled {
            panic(err)
        }
    }
}
```


## Boolean Expression Syntax

The `If` field supports boolean expressions evaluated against event attributes:

```go
// events.Time provides: second, minute, hour
If: "hour >= 9 and hour < 17"

// events.Process provides: pid, cwd, cmd
If: `cmd = "./game" and cwd != "/tmp"`

// Custom attributes from your event's Attributes() method
If: "username != 'bot' and score > 100"

// String operations (new in latest boolexpr)
If: `name starts_with "user_" and email ends_with "@example.com"`

// List operations
If: `tags contains "production" and roles excludes "guest"`
```

**Comparison Operators:** `=`, `==`, `!=`, `<`, `<=`, `>`, `>=`, `contains`, `excludes`, `starts_with`, `ends_with`

**Logical Operators:** `and`, `or`

**Value Types:** `int`, `float`, `string`, `bool`, `[]string`, `[]int`, `[]float64`, `[]bool`

**Grouping:** Use parentheses `(...)` to group logical expressions


## Architecture

### Rule Structure

```go
type Rule[In, Out Event] struct {
    ID   string       // Unique identifier
    When Source[In]   // Event producer
    If   string       // Boolean expression (optional)
    Then Action[In, Out] // Event transformer
    To   Destination[Out] // Side-effect applicator
}
```

### Core Interfaces

```go
type Event interface {
    ID() string
    Attributes(context.Context) (map[string]any, error)
}

type Source[T Event] interface {
    Start(context.Context, Callback[T]) (context.Context, error)
}

type Action[In, Out Event] interface {
    Process(context.Context, In, map[string]any) (Out, Report)
}

type Destination[T Event] interface {
    Send(context.Context, T) Report
}
```


## Example: Multi-Rule System

```go
func buildRegistry(ctx context.Context) fh.Registry {
    proc := events.Process{}
    streamInfo := events.TwitchStreamInfo{}
    
    // Rule 1: Update stream when Dead Cells starts
    registry := addRule(ctx, nil, &fh.Rule[events.Process, events.TwitchStreamInfo]{
        ID:   "dead_cells",
        When: sources.Process{},
        If:   `cmd = "./deadcells"`,
        Then: actions.Event[events.Process, events.TwitchStreamInfo]{
            Output: events.TwitchStreamInfo{
                Title: "Playing Dead Cells",
                Game:  "Dead Cells",
                Tags:  []string{"roguelike", "linux"},
            },
        },
        To: destinations.TwitchStreamInfo{},
    }, proc, streamInfo)
    
    // Rule 2: Update stream when Emacs starts (shares same Process source)
    registry = addRule(ctx, registry, &fh.Rule[events.Process, events.TwitchStreamInfo]{
        ID:   "emacs",
        When: sources.Process{}, // Same source instance = fanout
        If:   `cmd = "emacs"`,
        Then: actions.Event[events.Process, events.TwitchStreamInfo]{
            Output: events.TwitchStreamInfo{
                Title: "Coding in Emacs",
                Game:  "Software and Game Development",
            },
        },
        To: destinations.TwitchStreamInfo{},
    }, proc, streamInfo)
    
    return registry
}
```


## Design Goals

- **Minimal primitives**: Four core concepts (Source, Action, Destination, Rule)
- **Isolated logic**: Define components independently, compose via rules
- **Component reusability**: Share sources, actions, destinations across rules
- **Type safety**: Compile-time guarantees across pipelines
- **Maximum linting**: 50+ golangci-lint rules enabled
- **Validation**: Declarative struct validation with tags


## Use Cases

- Event-driven microservices (HTTP, gRPC)
- Stream processing (Kafka, Twitch chat, websockets)
- System monitoring (process tracking, file watching)
- Game engines (input handling, state machines)
- ETL pipelines (database, queue, API integrations)


## License

See [LICENSE](LICENSE) file.
