# Custom Components

Learn how to implement custom Sources, Actions, Destinations, Conditions, and Middlewares for your specific needs.

## Custom Sources

Sources produce events and send them to a callback function.

### Interface

```go
type Source[T any] interface {
    Start(ctx context.Context, cb Callback[T]) (done context.Context, err error)
}
```

### Example: File Watcher

```go
type FileWatcher struct {
    Path string
}

type FileEvent struct {
    Path      string
    Operation string
    Timestamp time.Time
}

func (fw FileWatcher) Start(
    ctx context.Context,
    cb fh.Callback[FileEvent],
) (context.Context, error) {
    watcher, err := fsnotify.NewWatcher()
    if err != nil {
        return ctx, err
    }
    
    if err := watcher.Add(fw.Path); err != nil {
        return ctx, err
    }
    
    go func() {
        defer watcher.Close()
        
        for {
            select {
            case <-ctx.Done():
                return
                
            case event := <-watcher.Events:
                fileEvent := FileEvent{
                    Path:      event.Name,
                    Operation: event.Op.String(),
                    Timestamp: time.Now(),
                }
                
                cb(ctx, fileEvent, func(report fh.Report) {
                    if report.Err != nil {
                        log.Printf("Error processing %s: %v", event.Name, report.Err)
                    }
                })
                
            case err := <-watcher.Errors:
                log.Printf("Watcher error: %v", err)
            }
        }
    }()
    
    return ctx, nil
}
```

### Example: Kafka Consumer

```go
type KafkaSource struct {
    Brokers []string
    Topic   string
    GroupID string
}

func (k KafkaSource) Start(
    ctx context.Context,
    cb fh.Callback[[]byte],
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
    callback fh.Callback[[]byte]
}

func (h *consumerHandler) ConsumeClaim(
    session sarama.ConsumerGroupSession,
    claim sarama.ConsumerGroupClaim,
) error {
    for message := range claim.Messages() {
        h.callback(session.Context(), message.Value, func(report fh.Report) {
            if report.Err == nil {
                session.MarkMessage(message, "")
            }
        })
    }
    return nil
}
```

## Custom Actions

Actions transform input events to output events.

### Interface

```go
type Action[I, O any] interface {
    Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, Report)
}
```

### Example: HTTP API Call

```go
type APICall struct {
    BaseURL string
    Client  *http.Client
}

func (a APICall) Process(
    ctx context.Context,
    event OrderEvent,
    syms boolexpr.Symbols,
) (APIResponse, fh.Report) {
    url := fmt.Sprintf("%s/orders/%s", a.BaseURL, event.OrderID)
    
    req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
    if err != nil {
        return APIResponse{}, fh.NewReport(err)
    }
    
    resp, err := a.Client.Do(req)
    if err != nil {
        return APIResponse{}, fh.NewReport(err)
    }
    defer resp.Body.Close()
    
    var apiResp APIResponse
    if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
        return APIResponse{}, fh.NewReport(err)
    }
    
    return apiResp, fh.NewReport(nil)
}
```

### Example: Database Query

```go
type GetUserByID struct {
    DB *sql.DB
}

func (g GetUserByID) Process(
    ctx context.Context,
    event LoginEvent,
    syms boolexpr.Symbols,
) (User, fh.Report) {
    var user User
    
    err := g.DB.QueryRowContext(
        ctx,
        "SELECT id, name, email FROM users WHERE id = $1",
        event.UserID,
    ).Scan(&user.ID, &user.Name, &user.Email)
    
    if err != nil {
        return User{}, fh.NewReport(err)
    }
    
    return user, fh.NewReport(nil)
}
```

## Custom Destinations

Destinations consume events and produce side effects.

### Interface

```go
type Destination[T any] interface {
    Send(ctx context.Context, event T) Report
}
```

### Example: Email Sender

```go
type EmailSender struct {
    SMTPHost string
    SMTPPort int
    From     string
    Auth     smtp.Auth
}

func (e EmailSender) Send(ctx context.Context, email Email) fh.Report {
    msg := fmt.Sprintf(
        "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s",
        e.From, email.To, email.Subject, email.Body,
    )
    
    addr := fmt.Sprintf("%s:%d", e.SMTPHost, e.SMTPPort)
    
    err := smtp.SendMail(
        addr,
        e.Auth,
        e.From,
        []string{email.To},
        []byte(msg),
    )
    
    return fh.NewReport(err)
}
```

### Example: S3 Uploader

```go
type S3Uploader struct {
    Client *s3.Client
    Bucket string
}

func (s S3Uploader) Send(ctx context.Context, file FileData) fh.Report {
    _, err := s.Client.PutObject(ctx, &s3.PutObjectInput{
        Bucket: aws.String(s.Bucket),
        Key:    aws.String(file.Name),
        Body:   bytes.NewReader(file.Data),
    })
    
    return fh.NewReport(err)
}
```

## Custom Conditions

Conditions filter events based on custom logic.

### Interface

```go
type Condition[I any] interface {
    Evaluate(ctx context.Context, event I, syms boolexpr.Symbols) (bool, error)
}
```

### Example: Business Hours Check

```go
type BusinessHours struct {
    Start int // Hour (0-23)
    End   int // Hour (0-23)
}

func (b BusinessHours) Evaluate(
    ctx context.Context,
    event any,
    syms boolexpr.Symbols,
) (bool, error) {
    hour := time.Now().Hour()
    return hour >= b.Start && hour < b.End, nil
}

// Usage
Where: &BusinessHours{Start: 9, End: 17}
```

### Example: External Service Check

```go
type FeatureFlagCheck struct {
    Client *featureflag.Client
    Flag   string
}

func (f FeatureFlagCheck) Evaluate(
    ctx context.Context,
    event UserEvent,
    syms boolexpr.Symbols,
) (bool, error) {
    enabled, err := f.Client.IsEnabled(ctx, f.Flag, event.UserID)
    return enabled, err
}
```

## Custom Middlewares

Middlewares intercept pipeline components for cross-cutting concerns.

### Example: Timeout Middleware

```go
type TimeoutMiddleware[I, O any] struct {
    Timeout time.Duration
}

type timeoutAction[I, O any] struct {
    next    fh.Action[I, O]
    timeout time.Duration
}

func (a timeoutAction[I, O]) Process(
    ctx context.Context,
    event I,
    syms boolexpr.Symbols,
) (O, fh.Report) {
    ctx, cancel := context.WithTimeout(ctx, a.timeout)
    defer cancel()
    
    type result struct {
        out    O
        report fh.Report
    }
    
    resultChan := make(chan result, 1)
    
    go func() {
        out, report := a.next.Process(ctx, event, syms)
        resultChan <- result{out, report}
    }()
    
    select {
    case res := <-resultChan:
        return res.out, res.report
    case <-ctx.Done():
        var zero O
        return zero, fh.NewReport(ctx.Err())
    }
}

func (m TimeoutMiddleware[I, O]) WrapAction(
    ctx context.Context,
    rule *fh.Rule[I, O],
    action fh.Action[I, O],
) (fh.Action[I, O], error) {
    return timeoutAction[I, O]{
        next:    action,
        timeout: m.Timeout,
    }, nil
}

// Implement other methods (return unchanged if not wrapping)
func (m TimeoutMiddleware[I, O]) WrapCallback(...) (fh.Callback[I], error) {
    return cb, nil
}

func (m TimeoutMiddleware[I, O]) WrapDestination(...) (fh.Destination[O], error) {
    return dest, nil
}
```

### Example: Caching Middleware

```go
type CachingMiddleware[I, O any] struct {
    Cache cache.Cache[O]
    TTL   time.Duration
}

type cachingAction[I, O any] struct {
    next  fh.Action[I, O]
    cache cache.Cache[O]
    ttl   time.Duration
}

func (a cachingAction[I, O]) Process(
    ctx context.Context,
    event I,
    syms boolexpr.Symbols,
) (O, fh.Report) {
    // Generate cache key from event
    key := fmt.Sprintf("%v", event)
    
    // Check cache
    if cached, ok := a.cache.Get(key); ok {
        return cached, fh.NewReport(nil)
    }
    
    // Process
    out, report := a.next.Process(ctx, event, syms)
    
    // Cache on success
    if report.Err == nil {
        a.cache.Set(key, out, a.ttl)
    }
    
    return out, report
}
```

## Testing Custom Components

### Testing Sources

```go
func TestFileWatcher(t *testing.T) {
    tmpDir := t.TempDir()
    
    var received []FileEvent
    var mu sync.Mutex
    
    callback := func(ctx context.Context, event FileEvent, rf fh.ReportFunc) {
        mu.Lock()
        received = append(received, event)
        mu.Unlock()
        rf(fh.NewReport(nil))
    }
    
    source := FileWatcher{Path: tmpDir}
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    _, err := source.Start(ctx, callback)
    require.NoError(t, err)
    
    // Create test file
    os.WriteFile(filepath.Join(tmpDir, "test.txt"), []byte("hello"), 0644)
    
    time.Sleep(100 * time.Millisecond)
    
    mu.Lock()
    assert.GreaterOrEqual(t, len(received), 1)
    mu.Unlock()
}
```

### Testing Actions

```go
func TestAPICall(t *testing.T) {
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        json.NewEncoder(w).Encode(APIResponse{Status: "success"})
    }))
    defer server.Close()
    
    action := APICall{
        BaseURL: server.URL,
        Client:  server.Client(),
    }
    
    event := OrderEvent{OrderID: "123"}
    resp, report := action.Process(context.Background(), event, nil)
    
    assert.NoError(t, report.Err)
    assert.Equal(t, "success", resp.Status)
}
```

### Testing Destinations

```go
func TestEmailSender(t *testing.T) {
    // Use a mock SMTP server or test implementation
    sender := EmailSender{/* ... */}
    
    email := Email{
        Into:    "test@example.com",
        Subject: "Test",
        Body:    "Hello",
    }
    
    report := sender.Send(context.Background(), email)
    assert.NoError(t, report.Err)
}
```

## Best Practices

1. **Implement context handling** - Respect context cancellation
2. **Return meaningful errors** - Use typed errors when possible
3. **Keep state minimal** - Prefer stateless components
4. **Test thoroughly** - Unit test each component
5. **Document behavior** - Explain what the component does
6. **Handle edge cases** - Nil values, empty data, timeouts
7. **Use proper types** - Make invalid states unrepresentable

## Next Steps

- Learn about [Testing](/guide/testing)
- Explore [Environment Rules](/guide/environments)
- See [Best Practices](/guide/best-practices)
