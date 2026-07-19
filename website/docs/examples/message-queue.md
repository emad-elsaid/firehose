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
) (<-chan struct{}, error) {
    config := sarama.NewConfig()
    config.Consumer.Return.Errors = true

    consumer, err := sarama.NewConsumerGroup(k.Brokers, k.GroupID, config)
    if err != nil {
        return nil, err
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

    return ctx.Done(), nil
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

        h.callback(session.Context(), event, func(err error) {
            if err == nil {
                session.MarkMessage(message, "")
            } else {
                log.Printf("Processing error: %v", err)
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
) (OrderEvent, error) {
    log.Printf("Processing high-value order: %s", order.OrderID)
    return order, nil
}

// Destinations
type DatabaseWriter struct{}

func (d DatabaseWriter) Send(ctx context.Context, order OrderEvent) error {
    log.Printf("Saved to database: %s", order.OrderID)
    return nil
}

type DeadLetterQueue struct{}

func (d DeadLetterQueue) Send(ctx context.Context, order OrderEvent) error {
    log.Printf("Sent to DLQ: %s", order.OrderID)
    return nil
}

func main() {
    ctx := context.Background()

    kafkaSource := &KafkaSource{
        Brokers: []string{"localhost:9092"},
        Topic:   "orders",
        GroupID: "order-processor",
    }

    var head fh.Rule
    var err error

    // High-value order processing
    head, err = fh.Add(ctx, head, &fh.SQLRule[OrderEvent, OrderEvent]{
        ID:     "high_value",
        From:   kafkaSource,
        Where:  condition.Cond[OrderEvent](`amount > 1000`),
        Select: ProcessHighValueOrder{},
        Into:   DatabaseWriter{},
    })
    if err != nil {
        log.Fatal(err)
    }

    // Failed order handling
    head, err = fh.Add(ctx, head, &fh.SQLRule[OrderEvent, OrderEvent]{
        ID:     "failed_orders",
        From:   kafkaSource,
        Where:  condition.Cond[OrderEvent](`status = "failed"`),
        Select: actions.Identity[OrderEvent]{},
        Into:   DeadLetterQueue{},
    })
    if err != nil {
        log.Fatal(err)
    }

    doneChannels := fh.Start(ctx, head, nil)
    for _, ch := range doneChannels {
        <-ch
    }
}
```

## Key Concepts

- **Message acknowledgment** based on processing success
- **High-value order filtering** with conditions
- **Dead letter queue** for failed messages
- **Single consumer group** with source fanout
