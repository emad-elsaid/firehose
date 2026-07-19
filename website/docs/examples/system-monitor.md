# System Monitor Example

Monitor system processes and send alerts based on conditions.

## Overview

This example demonstrates:
- Process monitoring as event source
- Multi-destination alerting
- Conditional severity escalation

## Complete Example

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/shirou/gopsutil/v3/process"
    fh "github.com/emad-elsaid/firehose"
    "github.com/emad-elsaid/firehose/condition"
)

// Event type
type ProcessEvent struct {
    Name       string
    PID        int32
    CPUPercent float64
    MemoryMB   float64
}

func (p ProcessEvent) Get(key string) (any, error) {
    switch key {
    case "name":
        return p.Name, nil
    case "cpu":
        return p.CPUPercent, nil
    case "memory":
        return p.MemoryMB, nil
    default:
        return nil, fmt.Errorf("unknown: %s", key)
    }
}

// Alert type
type Alert struct {
    Severity string
    Message  string
    Process  ProcessEvent
}

// Process Monitor Source
type ProcessMonitor struct {
    PollInterval time.Duration
}

func (pm ProcessMonitor) Start(
    ctx context.Context,
    cb fh.Callback[ProcessEvent],
) (<-chan struct{}, error) {
    go func() {
        ticker := time.NewTicker(pm.PollInterval)
        defer ticker.Stop()

        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                processes, _ := process.Processes()

                for _, proc := range processes {
                    name, _ := proc.Name()
                    cpu, _ := proc.CPUPercent()
                    mem, _ := proc.MemoryInfo()

                    event := ProcessEvent{
                        Name:       name,
                        PID:        proc.Pid,
                        CPUPercent: cpu,
                        MemoryMB:   float64(mem.RSS) / 1024 / 1024,
                    }

                    cb(ctx, event, func(err error) {
                        if err != nil {
                            log.Printf("Alert error: %v", err)
                        }
                    })
                }
            }
        }
    }()

    return ctx.Done(), nil
}

// Actions
type CreateCriticalAlert struct{}

func (c CreateCriticalAlert) Process(
    ctx context.Context,
    proc ProcessEvent,
    _ boolexpr.Symbols,
) (Alert, error) {
    return Alert{
        Severity: "critical",
        Message:  fmt.Sprintf("High resource usage: %s", proc.Name),
        Process:  proc,
    }, nil
}

type CreateWarningAlert struct{}

func (w CreateWarningAlert) Process(
    ctx context.Context,
    proc ProcessEvent,
    _ boolexpr.Symbols,
) (Alert, error) {
    return Alert{
        Severity: "warning",
        Message:  fmt.Sprintf("Elevated usage: %s", proc.Name),
        Process:  proc,
    }, nil
}

// Destinations
type PagerDuty struct{}

func (p PagerDuty) Send(ctx context.Context, alert Alert) error {
    log.Printf("[PagerDuty] %s: %s (CPU: %.1f%%, Mem: %.1fMB)",
        alert.Severity, alert.Message, alert.Process.CPUPercent, alert.Process.MemoryMB)
    return nil
}

type Slack struct{}

func (s Slack) Send(ctx context.Context, alert Alert) error {
    log.Printf("[Slack] %s: %s", alert.Severity, alert.Message)
    return nil
}

func main() {
    ctx := context.Background()

    monitor := &ProcessMonitor{PollInterval: 5 * time.Second}

    var head fh.Rule
    var err error

    // Critical CPU alerts to PagerDuty
    head, err = fh.Add(ctx, head, &fh.SQLRule[ProcessEvent, Alert]{
        ID:     "critical_cpu",
        From:   monitor,
        Where:  condition.Cond[ProcessEvent](`cpu > 80`),
        Select: CreateCriticalAlert{},
        Into:   PagerDuty{},
    })
    if err != nil {
        log.Fatal(err)
    }

    // Critical memory alerts to PagerDuty
    head, err = fh.Add(ctx, head, &fh.SQLRule[ProcessEvent, Alert]{
        ID:     "critical_memory",
        From:   monitor,
        Where:  condition.Cond[ProcessEvent](`memory > 1000`),
        Select: CreateCriticalAlert{},
        Into:   PagerDuty{},
    })
    if err != nil {
        log.Fatal(err)
    }

    // Warning CPU alerts to Slack
    head, err = fh.Add(ctx, head, &fh.SQLRule[ProcessEvent, Alert]{
        ID:     "warning_cpu",
        From:   monitor,
        Where:  condition.Cond[ProcessEvent](`cpu > 50 and cpu <= 80`),
        Select: CreateWarningAlert{},
        Into:   Slack{},
    })
    if err != nil {
        log.Fatal(err)
    }

    doneChannels := fh.Start(ctx, head, nil)

    log.Println("System monitor running...")
    for _, ch := range doneChannels {
        <-ch
    }
}
```

## Key Concepts

- **Periodic polling** for system metrics
- **Threshold-based alerting** with conditions
- **Severity-based routing** to different destinations
- **Resource monitoring** (CPU, memory)
