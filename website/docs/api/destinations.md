# Destinations API

API reference for destination interfaces and built-in implementations.

## Destination Interface

```go
type Destination[T any] interface {
    Send(ctx context.Context, event T) error
}
```

## Built-in Destinations

### destinations.Func

Function adapter for custom destinations.

```go
import "github.com/emad-elsaid/firehose/destinations"

Into: destinations.Func[User](func(ctx context.Context, user User) error {
    err := saveToDatabase(user)
    return err
})
```

### destinations.Accumulator

Collect events in memory (useful for testing).

```go
accumulator := &destinations.Accumulator[User]{}

Into: accumulator

// Later
users := accumulator.Items()
```

### destinations.Fanout

Send to all destinations. Errors are joined.

```go
Into: destinations.Fanout[User]{
    Destinations: []fh.Destination[User]{
        Database{},
        EmailService{},
        Analytics{},
    },
}
```

### destinations.RoundRobin

Send in round-robin order.

```go
Into: &destinations.RoundRobin[User]{
    Destinations: []fh.Destination[User]{
        Shard1{},
        Shard2{},
        Shard3{},
    },
}
```

### destinations.Random

Send to a random destination using crypto/rand.

```go
Into: &destinations.Random[User]{
    Destinations: []fh.Destination[User]{
        Server1{},
        Server2{},
    },
}
```

### Channel Adapters

```go
// Consume from channel
Into: destinations.FromChan[User]{
    Into: UserProcessor{},
}

// Wrap as channel
Into: destinations.ToChan[User]{
    Into: ChannelConsumer{},
}
```

### Slice Adapters

```go
// Consume from slice
Into: destinations.FromSlice[User]{
    Into: UserProcessor{},
}

// Wrap as slice
Into: destinations.ToSlice[User]{
    Into: BatchProcessor{},
}
```
