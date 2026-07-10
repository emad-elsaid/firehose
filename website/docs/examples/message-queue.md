# Message Queue Example

Process messages from Kafka/RabbitMQ with filtering and transformation.

## Overview

This example demonstrates:
- Message queue as event source
- Event filtering by topic/type
- Message transformation
- Dead letter queue handling

## Kafka Consumer Example

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    
    "github.com/IBM/sarama"
    fh "github.com/emad-elsaid/firehose"
    "github.com/emad-elsaid/firehose/condition"
)

// Event type
type OrderEvent struct {
    OrderID string
    Amount  float64
    Status  string
}

func (o OrderEvent) Get(key string) (any, error) {
    switch key {
    case "amount":
        return o.Amount, nil
    case "status":
        return o.Status, nil
    default:
        return nil, fmt.Errorf("unknown: %s", key)
    }
}

// Kafka Source
type KafkaSource struct {
    Brokers []string
    Topic   string
    GroupID string
}

func (k KafkaSource) Start(
    ctx context.Context,
    cb fh.Callback[OrderEvent],
) (context.Context, error) {
    config := sarama.NewConfig()
    config.Consumer.Return.Errors = true
    
    consumer, err := sarama.NewConsumerGroup(k.Brokers, k.GroupID, config)
    if err != nil {
        return ctx, err
    }
    
    handler := &consumerHandler{callback: cb}
    
    go func() {
        defer consumer.Close()
        
        for {
            if err := consumer.Consume(ctx, []string{k.Topic}, handler); err != nil {
                log.Printf("Kafka error: %v", err)
            }
            
            if ctx.Err() != nil {
                return
            }
        }
    }()
    
    return ctx, nil
}

type consumerHandler struct {
    callback fh.Callback[OrderEvent]
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *consumerHandler) ConsumeClaim(
    session sarama.ConsumerGroupSession,
    claim sarama.ConsumerGroupClaim,
) error {
    for message := range claim.Messages() {
        var event OrderEvent
        if err := json.Unmarshal(message.Value, &event); err != nil {
            log.Printf("Parse error: %v", err)
            continue
        }
        
        h.callback(session.Context(), event, func(report fh.Report) {
            if report.Err == nil {
                session.MarkMessage(message, "")
            } else {
                log.Printf("Processing error: %v", report.Err)
            }
        })
    }
    return nil
}

// Actions
type ProcessHighValueOrder struct{}

func (p ProcessHighValueOrder) Process(
    ctx context.Context,
    order OrderEvent,
    _ boolexpr.Symbols,
) (OrderEvent, fh.Report) {
    log.Printf("Processing high-value order: %s", order.OrderID)
    return order, fh.NewReport(nil)
}

// Destinations
type DatabaseWriter struct{}

func (d DatabaseWriter) Send(ctx context.Context, order OrderEvent) fh.Report {
    log.Printf("Saved to database: %s", order.OrderID)
    return fh.NewReport(nil)
}

type DeadLetterQueue struct{}

func (d DeadLetterQueue) Send(ctx context.Context, order OrderEvent) fh.Report {
    log.Printf("Sent to DLQ: %s", order.OrderID)
    return fh.NewReport(nil)
}

func main() {
    ctx := context.Background()
    
    kafkaSource := &KafkaSource{
        Brokers: []string{"localhost:9092"},
        Topic:   "orders",
        GroupID: "order-processor",
    }
    
    rule := &fh.Rule[OrderEvent, OrderEvent]{
        ID: "order_processor",
        From: kafkaSource,
        
        SubRules: []fh.Rule[OrderEvent, OrderEvent]{
            {
                ID:   "high_value",
                Where:   condition.Cond[OrderEvent](`amount > 1000`),
                Select: ProcessHighValueOrder{},
                Into:   DatabaseWriter{},
            },
            {
                ID:   "failed_orders",
                Where:   condition.Cond[OrderEvent](`status = "failed"`),
                Select: actions.Identity[OrderEvent]{},
                Into:   DeadLetterQueue{},
            },
        },
    }
    
    registry, _ := fh.Add(ctx, nil, rule)
    fh.Start(ctx, registry, nil)
    fh.Wait(registry, nil)
}
```

## Key Concepts

- **Message acknowledgment** based on processing success
- **High-value order filtering** with conditions
- **Dead letter queue** for failed messages
- **Single consumer group** with source fanout
