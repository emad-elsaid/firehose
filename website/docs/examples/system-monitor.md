# System Monitor Example

Monitor system processes and send alerts based on conditions.

## Overview

This example demonstrates:
- Process monitoring as event source
- Hierarchical alert rules
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
    "github.com/emad-elsaid/firehose/ifs"
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
) (context.Context, error) {
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
                    
                    cb(ctx, event, func(report fh.Report) {
                        if report.Err != nil {
                            log.Printf("Alert error: %v", report.Err)
                        }
                    })
                }
            }
        }
    }()
    
    return ctx, nil
}

// Actions
type CreateCriticalAlert struct{}

func (c CreateCriticalAlert) Process(
    ctx context.Context,
    proc ProcessEvent,
    _ boolexpr.Symbols,
) (Alert, fh.Report) {
    return Alert{
        Severity: "critical",
        Message:  fmt.Sprintf("High resource usage: %s", proc.Name),
        Process:  proc,
    }, fh.NewReport(nil)
}

type CreateWarningAlert struct{}

func (w CreateWarningAlert) Process(
    ctx context.Context,
    proc ProcessEvent,
    _ boolexpr.Symbols,
) (Alert, fh.Report) {
    return Alert{
        Severity: "warning",
        Message:  fmt.Sprintf("Elevated usage: %s", proc.Name),
        Process:  proc,
    }, fh.NewReport(nil)
}

// Destinations
type PagerDuty struct{}

func (p PagerDuty) Send(ctx context.Context, alert Alert) fh.Report {
    log.Printf("[PagerDuty] %s: %s (CPU: %.1f%%, Mem: %.1fMB)",
        alert.Severity, alert.Message, alert.Process.CPUPercent, alert.Process.MemoryMB)
    return fh.NewReport(nil)
}

type Slack struct{}

func (s Slack) Send(ctx context.Context, alert Alert) fh.Report {
    log.Printf("[Slack] %s: %s", alert.Severity, alert.Message)
    return fh.NewReport(nil)
}

func main() {
    ctx := context.Background()
    
    monitor := &ProcessMonitor{PollInterval: 5 * time.Second}
    
    rule := &fh.Rule[ProcessEvent, Alert]{
        ID: "system_monitor",
        From: monitor,
        
        SubRules: []fh.Rule[ProcessEvent, Alert]{
            {
                ID:   "critical_cpu",
                Where:   ifs.Cond[ProcessEvent](`cpu > 80`),
                Select: CreateCriticalAlert{},
                Into:   PagerDuty{},
            },
            {
                ID:   "critical_memory",
                Where:   ifs.Cond[ProcessEvent](`memory > 1000`),
                Select: CreateCriticalAlert{},
                Into:   PagerDuty{},
            },
            {
                ID:   "warning_cpu",
                Where:   ifs.Cond[ProcessEvent](`cpu > 50 and cpu <= 80`),
                Select: CreateWarningAlert{},
                Into:   Slack{},
            },
        },
    }
    
    registry, _ := fh.AddRule(ctx, nil, rule)
    fh.Start(ctx, registry, nil)
    
    log.Println("System monitor running...")
    fh.Wait(registry, nil)
}
```

## Key Concepts

- **Periodic polling** for system metrics
- **Threshold-based alerting** with conditions
- **Severity-based routing** to different destinations
- **Resource monitoring** (CPU, memory)
