# Sources API

API reference for source interfaces and built-in implementations.

## Source Interface

```go
type Source[T any] interface {
    Start(ctx context.Context, cb Callback[T]) (done <-chan struct{}, err error)
}
```

## Callback and ErrorHandler

```go
type Callback[I any] func(context.Context, I, ErrorHandler)
type ErrorHandler func(err error)
```

## Built-in Sources

### sources.Func

Function adapter for custom sources.

```go
import "github.com/emad-elsaid/firehose/sources"

From: sources.Func[Event](func(ctx context.Context, cb fh.Callback[Event]) (<-chan struct{}, error) {
    go func() {
        for {
            select {
            case <-ctx.Done():
                return
            case event := <-someChannel:
                cb(ctx, event, func(err error) {
                    // Handle error
                })
            }
        }
    }()
    return ctx.Done(), nil
})
```

### sources.Manual

Manually emit events (useful for testing).

```go
manual := &sources.Manual[Event]{}

From: manual

// Emit events
manual.Emit(ctx, Event{ID: "123"})
```

## Implementing Custom Sources

Example HTTP source:

```go
type HTTPSource struct {
    Addr string
}

func (s HTTPSource) Start(ctx context.Context, cb fh.Callback[HTTPRequest]) (<-chan struct{}, error) {
    server := &http.Server{Addr: s.Addr}

    http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        event := HTTPRequest{Method: r.Method, Path: r.URL.Path}

        cb(r.Context(), event, func(err error) {
            if err != nil {
                log.Printf("Error: %v", err)
            }
        })
    })

    go server.ListenAndServe()
    return ctx.Done(), nil
}
```
