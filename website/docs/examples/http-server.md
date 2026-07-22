# HTTP Server Example

Build an HTTP event processing pipeline with request routing and response handling.

## Overview

This example demonstrates:
- HTTP request as event source
- Path-based routing with conditions
- Request transformation
- Response generation

## Complete Example

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net/http"

    "github.com/emad-elsaid/boolexpr"
    fh "github.com/emad-elsaid/firehose"
    "github.com/emad-elsaid/firehose/condition"
)

type contextKey string

const writerKey contextKey = "writer"

// Event type
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

// Response type
type HTTPResponse struct {
    Status int
    Body   string
}

// HTTP Source — stores ResponseWriter in context
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

// Actions
type GetUserHandler struct{}

func (h GetUserHandler) Process(
    ctx context.Context,
    req HTTPRequest,
    syms boolexpr.Symbols,
) (HTTPResponse, error) {
    return HTTPResponse{
        Status: 200,
        Body:   `{"user": "alice"}`,
    }, nil
}

type CreateUserHandler struct{}

func (h CreateUserHandler) Process(
    ctx context.Context,
    req HTTPRequest,
    syms boolexpr.Symbols,
) (HTTPResponse, error) {
    return HTTPResponse{
        Status: 201,
        Body:   `{"created": true}`,
    }, nil
}

// Destination — writes to ResponseWriter from context
type JSONResponse struct{}

func (d JSONResponse) Send(ctx context.Context, resp HTTPResponse) error {
    w, ok := ctx.Value(writerKey).(http.ResponseWriter)
    if !ok {
        return fmt.Errorf("missing ResponseWriter in context")
    }

    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(resp.Status)
    return json.NewEncoder(w).Encode(resp.Body)
}

func main() {
    ctx := context.Background()

    httpSource := HTTPServer{Addr: ":8080"}

    var head fh.Rule
    var err error

    // GET /api/users route
    head, err = fh.Add(ctx, head, &fh.SQLRule[HTTPRequest, HTTPResponse]{
        ID:     "get_user",
        Select: GetUserHandler{},
        From:   httpSource,
        Where:  condition.Cond[HTTPRequest](`method = "GET" and path starts_with "/api/users"`),
        Into:   JSONResponse{},
    })
    if err != nil {
        log.Fatal(err)
    }

    // POST /api/users route
    head, err = fh.Add(ctx, head, &fh.SQLRule[HTTPRequest, HTTPResponse]{
        ID:     "create_user",
        Select: CreateUserHandler{},
        From:   httpSource,
        Where:  condition.Cond[HTTPRequest](`method = "POST" and path starts_with "/api/users"`),
        Into:   JSONResponse{},
    })
    if err != nil {
        log.Fatal(err)
    }

    doneChannels := fh.Start(ctx, head, func(err error) {
        log.Printf("Error: %v", err)
    })

    log.Println("Server listening on :8080")
    for _, ch := range doneChannels {
        <-ch
    }
}
```

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
