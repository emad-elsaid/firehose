# Environment-Specific Rules

Deploy different rule configurations for different environments using the `Environments` field.

## Overview

Rules can be activated only in specific environments using the `Environments` field. This allows you to:

- Run different logic in production vs development
- Enable debugging rules only in staging
- Use different destinations per environment
- Test rules before production deployment

## Basic Usage

```go
rule := &fh.Rule[Event, Output]{
    ID:           "billing_processor",
    Environments: []string{"production", "staging"},
    Select:         action,
    Into:           destination,
    From:           source,
}
```

This rule activates only when the `ENV` environment variable matches `"production"` or `"staging"`.

## Environment Detection

Firehose checks the `ENV` environment variable:

```bash
# Activate production rules
export ENV=production

# Activate development rules
export ENV=development

# Activate staging rules
export ENV=staging
```

If `Environments` is empty or nil, the rule is active in all environments:

```go
rule := &fh.Rule[Event, Output]{
    ID: "always_active",
    From: source,
    // No Environments field - active everywhere
}
```

## Common Patterns

### Different Destinations

```go
rules := []*fh.Rule[Event, Event]{
    {
        ID:           "prod_events",
        Environments: []string{"production"},
        Select:         actions.Identity[Event]{},
        Into:           ProductionDatabase{},
        From:           eventSource,
    },
    {
        ID:           "dev_events",
        Environments: []string{"development"},
        Select:         actions.Identity[Event]{},
        Into:           LocalDatabase{},
        From:           eventSource,
    },
}
```

### Debug Logging

```go
debugRule := &fh.Rule[Event, Event]{
    ID:           "debug_logger",
    Environments: []string{"development", "staging"},
    Select:         actions.Identity[Event]{},
    Into:           ConsoleLogger{},
    From:           source,
}
```

### Feature Flags

```go
betaFeature := &fh.Rule[Event, Output]{
    ID:           "beta_feature",
    Environments: []string{"staging", "beta"},
    Select:         NewFeatureAction{},
    Into:           destination,
    From:           source,
}
```

## Multiple Environments

A rule can be active in multiple environments:

```go
rule := &fh.Rule[Event, Output]{
    Environments: []string{"production", "staging", "qa"},
    // Active in production, staging, and qa
}
```

## Testing Different Environments

```go
func TestEnvironmentRules(t *testing.T) {
    tests := []struct {
        name    string
        env     string
        wantLen int
    }{
        {"production", "production", 2},
        {"development", "development", 1},
        {"staging", "staging", 3},
    }
    
    for _, tc := range tests {
        t.Run(tc.name, func(t *testing.T) {
            os.Setenv("ENV", tc.env)
            defer os.Unsetenv("ENV")
            
            // Create rules with different environments
            registry := createTestRegistry(t)
            
            // Verify correct rules are active
            assert.Equal(t, tc.wantLen, countActiveRules(registry))
        })
    }
}
```

## Best Practices

1. **Use consistent names** - `production`, `staging`, `development`
2. **Set ENV explicitly** - Don't rely on defaults
3. **Test all environments** - Verify rules activate correctly
4. **Document requirements** - List required environments in README
5. **Use environment variables** - For environment-specific configuration
6. **Avoid environment logic in code** - Use Environments field instead
7. **Default to safe** - Critical rules should specify environments

## Configuration Management

### Using Config Files

```go
type Config struct {
    Environment string
    Rules       []RuleConfig
}

type RuleConfig struct {
    ID           string
    Environments []string
}

func loadConfig(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }
    
    var config Config
    if err := json.Unmarshal(data, &config); err != nil {
        return nil, err
    }
    
    return &config, nil
}
```

### Environment-Specific Config

```yaml
# config.production.yaml
environment: production
rules:
  - id: billing
    environments: [production]
    source: kafka
    destination: postgres

# config.development.yaml
environment: development
rules:
  - id: billing
    environments: [development]
    source: manual
    destination: console
```

## Deployment

### Docker

```dockerfile
FROM golang:1.21

ENV ENV=production

COPY . /app
WORKDIR /app

RUN go build -o server

CMD ["./server"]
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: firehose-app
spec:
  template:
    spec:
      containers:
      - name: app
        image: myapp:latest
        env:
        - name: ENV
          value: "production"
```

## Next Steps

- Learn about [Best Practices](/guide/best-practices)
- See [Examples](/examples/)
