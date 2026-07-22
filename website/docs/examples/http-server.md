# HTTP Server Example

Build an HTTP event processing pipeline with request routing and response handling.

## Overview

This example demonstrates:
- HTTP request as event source
- Path-based routing with conditions
- Request transformation
- Response generation

## Example

:::tabs

== Input

```go
type HTTPRequest struct {
    Method string
    Path   string
    Body   []byte
}

func (h HTTPRequest) Get(key string) (any, error) {
    switch key {
    case "method":
        return h.Method, nil
    case "path":
        return h.Path, nil
    default:
        return nil, fmt.Errorf("unknown: %s", key)
    }
}
```

== Output

```go
type User struct {
    Name  string `json:"name"`
    Email string `json:"email"`
}

type Created struct {
    Created bool `json:"created"`
}
```

== Source

```go
type HTTPServer struct {
    Addr string
}

func (s HTTPServer) Start(ctx context.Context, cb fh.Callback[HTTPRequest]) (<-chan struct{}, error) {
    mux := http.NewServeMux()

    mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        body := make([]byte, r.ContentLength)
        r.Body.Read(body)

        event := HTTPRequest{
            Method: r.Method,
            Path:   r.URL.Path,
            Body:   body,
        }

        ctx := context.WithValue(r.Context(), writerKey, w)

        cb(ctx, event, func(err error) {
            if err != nil {
                w.WriteHeader(500)
                fmt.Fprintf(w, "Error: %v", err)
            }
        })
    })

    server := &http.Server{Addr: s.Addr, Handler: mux}
    go server.ListenAndServe()

    return ctx.Done(), nil
}
```

== Actions

```go
type GetUserHandler struct{}

func (h GetUserHandler) Process(
    ctx context.Context,
    req HTTPRequest,
    syms boolexpr.Symbols,
) (User, error) {
    return User{Name: "alice", Email: "alice@example.com"}, nil
}

type CreateUserHandler struct{}

func (h CreateUserHandler) Process(
    ctx context.Context,
    req HTTPRequest,
    syms boolexpr.Symbols,
) (Created, error) {
    return Created{Created: true}, nil
}
```

== Destination

```go
type JSONResponse[T any] struct{}

func (d JSONResponse[T]) Send(ctx context.Context, resp T) error {
    w, ok := ctx.Value(writerKey).(http.ResponseWriter)
    if !ok {
        return fmt.Errorf("missing ResponseWriter in context")
    }

    w.Header().Set("Content-Type", "application/json")
    return json.NewEncoder(w).Encode(resp)
}
```

== Rules

```go
var head fh.Rule

// GET /api/users route
head, _ = fh.Add(ctx, head, &fh.ScenarioRule[HTTPRequest, User]{
    ID:    "get_user",
    When:  httpSource,
    Given: condition.Cond[HTTPRequest](`method = "GET" and path starts_with "/api/users"`),
    Then:  GetUserHandler{},
    To:    JSONResponse[User]{},
})

// POST /api/users route
head, _ = fh.Add(ctx, head, &fh.ScenarioRule[HTTPRequest, Created]{
    ID:    "create_user",
    When:  httpSource,
    Given: condition.Cond[HTTPRequest](`method = "POST" and path starts_with "/api/users"`),
    Then:  CreateUserHandler{},
    To:    JSONResponse[Created]{},
})
```

:::

## Running

```bash
go run main.go

# Test
curl http://localhost:8080/api/users
curl -X POST http://localhost:8080/api/users -d '{"name":"bob"}'
```

## Key Concepts

- **Path-based routing** using `starts_with` operator
- **Method filtering** with conditions
- **Shared source** for multiple route rules
- **Type-safe handlers** for each endpoint
