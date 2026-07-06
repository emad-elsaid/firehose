# Built-in Components

Firehose ships with reusable building blocks for common event processing patterns.

## Conditions (`ifs`)

### Expression-Based Filtering

```go
import "github.com/emad-elsaid/firehose/ifs"

// Simple comparison
If: ifs.Cond[OrderEvent]("amount > 1000")

// Boolean logic
If: ifs.Cond[OrderEvent]("premium = true and amount > 500")

// String operations
If: ifs.Cond[OrderEvent](`country = "US" or country = "CA"`)
```

### Function Adapter

```go
If: ifs.Func[OrderEvent](func(ctx context.Context, evt OrderEvent, syms boolexpr.Symbols) (bool, error) {
    return evt.Amount > 1000, nil
})
```

### Rate Limiting

```go
If: &ifs.RateLimit[OrderEvent]{
    Limit: rate.Every(time.Second),
    Burst: 10,
}
```

### Deduplication

```go
If: &ifs.Once[OrderEvent]{
    Duration: 5 * time.Minute,
    Cache:    cache.NewMemory[bool](10*time.Minute, time.Minute),
}
```

### Multiple Conditions

```go
If: ifs.Ifs[OrderEvent]{
    ifs.Cond[OrderEvent]("amount > 100"),
    &ifs.RateLimit[OrderEvent]{Limit: rate.Every(time.Second), Burst: 5},
    ifs.Func[OrderEvent](customCheck),
}
```

## Actions (`actions`)

### Function Adapter

```go
import "github.com/emad-elsaid/firehose/actions"

Then: actions.Func[HTTPRequest, User](func(
    ctx context.Context,
    req HTTPRequest,
    syms boolexpr.Symbols,
) (User, fh.Report) {
    user := extractUserFromRequest(req)
    return user, fh.NewReport(nil)
})
```

### Caching

```go
Then: &actions.Cache[OrderEvent, ProcessedOrder]{
    Action: ProcessOrder{},
    Cache:  cache.NewMemory[ProcessedOrder](10*time.Minute, time.Minute),
    TTL:    5 * time.Minute,
}
```

### Action Composition

Chain multiple transformations:

```go
// Two actions: I → M → O
Then: actions.Chain[HTTPRequest, ParsedRequest, User]{
    First:  ParseRequest{},
    Second: ExtractUser{},
}

// Three actions: I → A → B → O
Then: actions.Chain3[HTTPRequest, ParsedRequest, ValidatedRequest, User]{
    First:  ParseRequest{},
    Second: ValidateRequest{},
    Third:  ExtractUser{},
}

// Chain4 and Chain5 also available
```

### Load Balancing

Round-robin distribution:

```go
Then: &actions.RoundRobin[OrderEvent, ProcessedOrder]{
    Actions: []fh.Action[OrderEvent, ProcessedOrder]{
        ProcessWithServiceA{},
        ProcessWithServiceB{},
        ProcessWithServiceC{},
    },
}
```

Random distribution:

```go
Then: &actions.Random[OrderEvent, ProcessedOrder]{
    Actions: []fh.Action[OrderEvent, ProcessedOrder]{
        ProcessWithServiceA{},
        ProcessWithServiceB{},
    },
}
```

## Destinations (`destinations`)

### Function Adapter

```go
import "github.com/emad-elsaid/firehose/destinations"

To: destinations.Func[User](func(ctx context.Context, user User) fh.Report {
    err := saveToDatabase(user)
    return fh.NewReport(err)
})
```

### Accumulator

Collect events in memory (useful for testing):

```go
accumulator := &destinations.Accumulator[User]{}

To: accumulator

// Later, retrieve collected items
users := accumulator.Items()
```

### Fanout

Send to all destinations:

```go
To: destinations.Fanout[User]{
    Destinations: []fh.Destination[User]{
        UserDatabase{},
        EmailService{},
        AnalyticsService{},
    },
}
```

### Load Balancing

Round-robin:

```go
To: &destinations.RoundRobin[User]{
    Destinations: []fh.Destination[User]{
        DatabaseShard1{},
        DatabaseShard2{},
        DatabaseShard3{},
    },
}
```

Random:

```go
To: &destinations.Random[User]{
    Destinations: []fh.Destination[User]{
        Server1{},
        Server2{},
    },
}
```

### Channel Adapters

Convert between single events and channels:

```go
// Consume from channel, forward each item
To: destinations.FromChan[User]{
    To: UserDatabase{},
}

// Wrap event in single-item channel
To: destinations.ToChan[User]{
    To: ChannelConsumer{},
}
```

### Slice Adapters

Convert between single events and slices:

```go
// Consume from slice, forward each item
To: destinations.FromSlice[User]{
    To: UserDatabase{},
}

// Wrap event in single-item slice
To: destinations.ToSlice[User]{
    To: BatchProcessor{},
}
```

## Sources (`sources`)

### Function Adapter

```go
import "github.com/emad-elsaid/firehose/sources"

On: sources.Func[HTTPRequest](func(ctx context.Context, cb fh.Callback[HTTPRequest]) (context.Context, error) {
    server := &http.Server{Addr: ":8080"}
    
    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        event := HTTPRequest{Method: r.Method, Path: r.URL.Path}
        cb(r.Context(), event, func(report fh.Report) {
            // Handle report
        })
    })
    
    go server.ListenAndServe()
    return ctx, nil
})
```

### Manual Source

Emit events manually (useful for testing):

```go
manual := &sources.Manual[OrderEvent]{}

On: manual

// Later, emit events
manual.Emit(ctx, OrderEvent{OrderID: "123", Amount: 99.99})
```

## Cache Storage (`cache`)

In-memory cache backend:

```go
import "github.com/emad-elsaid/firehose/cache"

cache := cache.NewMemory[ProcessedOrder](
    10*time.Minute,  // default TTL
    time.Minute,     // cleanup interval
)

// Use with actions.Cache or ifs.Once
Then: &actions.Cache[OrderEvent, ProcessedOrder]{
    Action: ProcessOrder{},
    Cache:  cache,
    TTL:    5 * time.Minute,
}
```

## Middlewares (`middlewares`)

### Panic Recovery

```go
import "github.com/emad-elsaid/firehose/middlewares"

Middlewares: []fh.Middleware[I, O]{
    &middlewares.Panic[I, O]{},
}
```

### Structured Logging

```go
Middlewares: []fh.Middleware[I, O]{
    &middlewares.Slog[I, O]{},
}
```

### Parallel Execution

Run same-source rules in parallel:

```go
import "github.com/emad-elsaid/firehose/runner"

Middlewares: []fh.Middleware[I, O]{
    &middlewares.Parallel[I, O]{
        Runner: runner.Basic{},
    },
}
```

## Next Steps

- Learn about [Middleware](/guide/middleware)
- See [Real-World Examples](/examples/)
- Read the [API Reference](/api/)
