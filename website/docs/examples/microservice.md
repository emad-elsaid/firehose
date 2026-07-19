# Event-Driven Microservice Example

Complete microservice architecture using event-driven patterns.

## Overview

This example demonstrates:
- Multiple event sources (HTTP, Kafka)
- Service composition
- Event transformation pipeline
- Error handling and retry logic

## Architecture

```
HTTP API → Order Created → Kafka Topic
                ↓
         Process Order → Multiple Actions
                ↓
    ├─ Send Email
    ├─ Update Inventory
    └─ Record Analytics
```

## Complete Example

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"

    fh "github.com/emad-elsaid/firehose"
    "github.com/emad-elsaid/firehose/destinations"
    "github.com/emad-elsaid/firehose/middlewares"
)

// Domain Events
type OrderCreated struct {
    OrderID    string  `json:"order_id"`
    CustomerID string  `json:"customer_id"`
    Amount     float64 `json:"amount"`
    Items      []Item  `json:"items"`
}

type Item struct {
    SKU      string  `json:"sku"`
    Quantity int     `json:"quantity"`
    Price    float64 `json:"price"`
}

type EmailNotification struct {
    To      string
    Subject string
    Body    string
}

type InventoryUpdate struct {
    SKU      string
    Quantity int
}

type AnalyticsEvent struct {
    EventType string
    Data      map[string]any
}

// HTTP Source
type OrderAPI struct {
    Addr string
}

func (api OrderAPI) Start(
    ctx context.Context,
    cb fh.Callback[OrderCreated],
) (<-chan struct{}, error) {
    http.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
        if r.Method != "POST" {
            http.Error(w, "Method not allowed", 405)
            return
        }

        var order OrderCreated
        if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
            http.Error(w, err.Error(), 400)
            return
        }

        cb(r.Context(), order, func(err error) {
            if err != nil {
                http.Error(w, err.Error(), 500)
                return
            }

            w.WriteHeader(201)
            json.NewEncoder(w).Encode(map[string]string{
                "status": "created",
                "id":     order.OrderID,
            })
        })
    })

    server := &http.Server{Addr: api.Addr}
    go server.ListenAndServe()

    return ctx.Done(), nil
}

// Actions
type CreateEmailAction struct{}

func (a CreateEmailAction) Process(
    ctx context.Context,
    order OrderCreated,
    _ boolexpr.Symbols,
) (EmailNotification, error) {
    return EmailNotification{
        To:      fmt.Sprintf("customer-%s@example.com", order.CustomerID),
        Subject: "Order Confirmation",
        Body:    fmt.Sprintf("Your order %s has been confirmed", order.OrderID),
    }, nil
}

type CreateInventoryUpdates struct{}

func (a CreateInventoryUpdates) Process(
    ctx context.Context,
    order OrderCreated,
    _ boolexpr.Symbols,
) ([]InventoryUpdate, error) {
    updates := make([]InventoryUpdate, len(order.Items))
    for i, item := range order.Items {
        updates[i] = InventoryUpdate{
            SKU:      item.SKU,
            Quantity: -item.Quantity,
        }
    }
    return updates, nil
}

type CreateAnalytics struct{}

func (a CreateAnalytics) Process(
    ctx context.Context,
    order OrderCreated,
    _ boolexpr.Symbols,
) (AnalyticsEvent, error) {
    return AnalyticsEvent{
        EventType: "order_created",
        Data: map[string]any{
            "order_id":    order.OrderID,
            "customer_id": order.CustomerID,
            "amount":      order.Amount,
            "item_count":  len(order.Items),
        },
    }, nil
}

// Destinations
type EmailService struct{}

func (s EmailService) Send(ctx context.Context, email EmailNotification) error {
    log.Printf("Sending email to %s: %s", email.To, email.Subject)
    return nil
}

type InventoryService struct{}

func (s InventoryService) Send(ctx context.Context, updates []InventoryUpdate) error {
    for _, update := range updates {
        log.Printf("Updating inventory: %s by %d", update.SKU, update.Quantity)
    }
    return nil
}

type AnalyticsService struct{}

func (s AnalyticsService) Send(ctx context.Context, event AnalyticsEvent) error {
    log.Printf("Recording analytics: %s", event.EventType)
    return nil
}

func main() {
    ctx := context.Background()

    api := &OrderAPI{Addr: ":8080"}

    // Email notification pipeline
    emailRule := &fh.SQLRule[OrderCreated, EmailNotification]{
        ID:   "send_order_email",
        Select: CreateEmailAction{},
        Into:   EmailService{},
        From:   api,
        Middlewares: []fh.Middleware[OrderCreated, EmailNotification]{
            &middlewares.Panic[OrderCreated, EmailNotification]{},
            &middlewares.Slog[OrderCreated, EmailNotification]{},
        },
    }

    // Inventory update pipeline
    inventoryRule := &fh.SQLRule[OrderCreated, []InventoryUpdate]{
        ID:   "update_inventory",
        Select: CreateInventoryUpdates{},
        Into: destinations.FromSlice[InventoryUpdate]{
            Into: InventoryService{},
        },
        From:   api,
        Middlewares: []fh.Middleware[OrderCreated, []InventoryUpdate]{
            &middlewares.Panic[OrderCreated, []InventoryUpdate]{},
        },
    }

    // Analytics pipeline
    analyticsRule := &fh.SQLRule[OrderCreated, AnalyticsEvent]{
        ID:   "record_analytics",
        Select: CreateAnalytics{},
        Into:   AnalyticsService{},
        From:   api,
    }

    // Register all rules
    head, _ := fh.Add(ctx, nil, emailRule)
    head, _ = fh.Add(ctx, head, inventoryRule)
    head, _ = fh.Add(ctx, head, analyticsRule)

    doneChannels := fh.Start(ctx, head, func(err error) {
        log.Printf("Error: %v", err)
    })

    log.Println("Microservice running on :8080")
    for _, ch := range doneChannels {
        <-ch
    }
}
```

## Testing

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{
    "order_id": "ORD-001",
    "customer_id": "CUST-123",
    "amount": 99.99,
    "items": [
      {"sku": "WIDGET-1", "quantity": 2, "price": 49.99}
    ]
  }'
```

## Key Concepts

- **Source fanout** - Single HTTP source feeding three rules
- **Multiple pipelines** - Email, inventory, analytics run independently
- **Middleware stacking** - Panic recovery and logging
- **Service composition** - Clean separation of concerns
- **Type safety** - Compiler verifies entire pipeline
