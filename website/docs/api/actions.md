# Actions API

API reference for action interfaces and built-in implementations.

## Action Interface

```go
type Action[I, O any] interface {
    Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, Report)
}
```

## Built-in Actions

### actions.Func

Function adapter for transformations.

```go
import "github.com/emad-elsaid/firehose/actions"

Then: actions.Func[HTTPRequest, User](func(
    ctx context.Context,
    req HTTPRequest,
    syms boolexpr.Symbols,
) (User, fh.Report) {
    user := extractUser(req)
    return user, fh.NewReport(nil)
})
```

### actions.Cache

Memoize action output.

```go
Then: &actions.Cache[Event, Result]{
    Action: ExpensiveOperation{},
    Cache:  cache.NewMemory[Result](10*time.Minute, time.Minute),
    TTL:    5 * time.Minute,
}
```

### actions.Chain

Compose two actions (I → M → O).

```go
Then: actions.Chain[HTTPRequest, ParsedRequest, User]{
    First:  ParseRequest{},
    Second: ExtractUser{},
}
```

### actions.Chain3, Chain4, Chain5

Compose 3, 4, or 5 actions sequentially.

```go
Then: actions.Chain3[HTTPRequest, Parsed, Validated, User]{
    First:  Parse{},
    Second: Validate{},
    Third:  Extract{},
}
```

### actions.RoundRobin

Distribute events across actions in round-robin order.

```go
Then: &actions.RoundRobin[Event, Result]{
    Actions: []fh.Action[Event, Result]{
        ProcessA{},
        ProcessB{},
        ProcessC{},
    },
}
```

### actions.Random

Dispatch to a random action.

```go
Then: &actions.Random[Event, Result]{
    Actions: []fh.Action[Event, Result]{
        ProcessA{},
        ProcessB{},
    },
}
```
