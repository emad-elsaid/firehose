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
    "github.com/emad-elsaid/firehose/ifs"
)

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

// HTTP Source
type HTTPServer struct {
    Addr string
}

func (s HTTPServer) Start(ctx context.Context, cb fh.Callback[HTTPRequest]) (context.Context, error) {
    mux := http.NewServeMux()
    
    mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        body := make([]byte, r.ContentLength)
        r.Body.Read(body)
        
        event := HTTPRequest{
            Method: r.Method,
            Path:   r.URL.Path,
            Body:   body,
        }
        
        cb(r.Context(), event, func(report fh.Report) {
            if report.Err != nil {
                w.WriteHeader(500)
                fmt.Fprintf(w, "Error: %v", report.Err)
                return
            }
        })
        
        w.WriteHeader(200)
        fmt.Fprintf(w, "OK")
    })
    
    server := &http.Server{Addr: s.Addr, Handler: mux}
    go server.ListenAndServe()
    
    return ctx, nil
}

// Actions
type GetUserHandler struct{}

func (h GetUserHandler) Process(
    ctx context.Context,
    req HTTPRequest,
    syms boolexpr.Symbols,
) (HTTPResponse, fh.Report) {
    return HTTPResponse{
        Status: 200,
        Body:   `{"user": "alice"}`,
    }, fh.NewReport(nil)
}

type CreateUserHandler struct{}

func (h CreateUserHandler) Process(
    ctx context.Context,
    req HTTPRequest,
    syms boolexpr.Symbols,
) (HTTPResponse, fh.Report) {
    return HTTPResponse{
        Status: 201,
        Body:   `{"created": true}`,
    }, fh.NewReport(nil)
}

// Destination
type JSONResponse struct{}

func (d JSONResponse) Send(ctx context.Context, resp HTTPResponse) fh.Report {
    log.Printf("Response: %d - %s", resp.Status, resp.Body)
    return fh.NewReport(nil)
}

func main() {
    ctx := context.Background()
    
    // Define routing rules
    apiGateway := &fh.Rule[HTTPRequest, HTTPResponse]{
        ID: "api_gateway",
        On: HTTPServer{Addr: ":8080"},
        
        SubRules: []fh.Rule[HTTPRequest, HTTPResponse]{
            {
                ID:   "get_user",
                If:   ifs.Cond[HTTPRequest](`method = "GET" and path starts_with "/api/users"`),
                Then: GetUserHandler{},
                To:   JSONResponse{},
            },
            {
                ID:   "create_user",
                If:   ifs.Cond[HTTPRequest](`method = "POST" and path starts_with "/api/users"`),
                Then: CreateUserHandler{},
                To:   JSONResponse{},
            },
        },
    }
    
    registry, err := fh.AddRule(ctx, nil, apiGateway)
    if err != nil {
        log.Fatal(err)
    }
    
    fh.Start(ctx, registry, func(err error) {
        log.Printf("Error: %v", err)
    })
    
    log.Println("Server listening on :8080")
    fh.Wait(registry, nil)
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
- **SubRules** for clean route organization
- **Type-safe handlers** for each endpoint
