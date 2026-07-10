# Core Types

Core type definitions for the Firehose framework.

## Rule

Defines a complete event processing pipeline.

```go
type Rule[I, O any] struct {
    ID           string
    Environments []string
    Select       Action[I, O]
    Into         Destination[O]
    From         Source[I]
    Where        Condition[I]
    Having       Condition[O]
    SubRules     []Rule[I, O]
    Middlewares  []Middleware[I, O]
}
```

### Fields

#### ID (string)
Unique identifier for the rule. Used in logs, reports, and monitoring.

```go
ID: "order_processing"
```

#### Environments ([]string)
Optional list of environments where this rule is active. If empty, rule is active in all environments.

```go
Environments: []string{"production", "staging"}
```

Rule is included only when the `ENV` environment variable matches one of the listed values.

#### Select (Action[I, O])
Transformation that converts input events (type `I`) to output events (type `O`).

```go
Select: ProcessOrder{}
Into: OrderDatabase{}
```

#### Into (Destination[O])
Destination that consumes output events of type `O`.

```go
```

#### From (Source[I])
Event source that produces events of type `I`.

```go
From: HTTPServer{Addr: ":8080"}
```

#### Where (Condition[I])
Optional condition that filters input events. If nil, all input events pass through.

```go
Where: condition.Cond[OrderEvent]("amount > 1000")
```

#### Having (Condition[O])
Optional condition that filters transformed output events before sending to destination.

```go
Having: condition.Cond[ProcessedOrder]("status = \"ready\"")
```

#### SubRules ([]Rule[I, O])
Child rules that inherit parent's source, conditions, and middlewares.

```go
SubRules: []Rule[I, O]{
    {ID: "high_value", Where: condition.Cond[I]("amount > 10000"), ...},
    {ID: "premium", Where: condition.Cond[I]("tier = premium"), ...},
}
```

#### Middlewares ([]Middleware[I, O])
Pipeline interceptors applied in registration order.

```go
Middlewares: []Middleware[I, O]{
    &middlewares.Panic[I, O]{},
    &middlewares.Slog[I, O]{},
}
```

## Report

Communicates operation results and errors.

```go
type Report struct {
    Err  error
    Rule string
}
```

### Fields

#### Err (error)
Optional error details. Can be any error type. Common errors:
- `ErrNoMatch` - condition evaluated to false
- `ConditionError` - failure in condition evaluation
- `ActionError` - failure in action processing
- `DestinationError` - failure in destination

#### Rule (string)
Rule ID where the report originated. Set automatically by the framework.

### Functions

#### NewReport

Creates a new report with an optional error.

```go
func NewReport(err error) Report
```

**Example:**

```go
if err != nil {
    return fh.NewReport(err)
}
return fh.NewReport(nil)
```

## Callback

Type alias for source callback functions.

```go
type Callback[I any] func(context.Context, I, ReportFunc)
```

Called by sources to deliver events to the pipeline.

**Parameters:**
- `context.Context` - Request context
- `I` - Event instance
- `ReportFunc` - Function to receive processing reports

## ReportFunc

Type alias for report sink functions.

```go
type ReportFunc func(Report)
```

Called by the framework to deliver processing reports back to the source.

## ErrorHandler

Type alias for error handler functions.

```go
type ErrorHandler func(error)
```

Used with `Start` and `Wait` to handle errors from source operations.

## Registry

Opaque type representing registered rules. Do not construct directly; use `Add`.

```go
type Registry interface {
    // Internal methods
}
```

## Errors

### ErrNoMatch

Indicates a condition evaluated to false (normal control flow, not an error).

```go
var ErrNoMatch = errors.New("condition did not match")
```

### ConditionError

Wraps errors from condition evaluation.

```go
type ConditionError struct {
    Err error
}

func (e ConditionError) Error() string
func (e ConditionError) Unwrap() error
```

### ActionError

Wraps errors from action processing.

```go
type ActionError struct {
    Err error
}

func (e ActionError) Error() string
func (e ActionError) Unwrap() error
```

### DestinationError

Wraps errors from destination operations.

```go
type DestinationError struct {
    Err error
}

func (e DestinationError) Error() string
func (e DestinationError) Unwrap() error
```

## Next Steps

- [Source Interface](/api/sources)
- [Action Interface](/api/actions)
- [Destination Interface](/api/destinations)
- [Middleware Interface](/api/middleware)
