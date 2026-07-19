# Environment-Specific Rules

Deploy different rule configurations for different environments using the
`Environments` field.

## Overview

Rules can be activated only in specific environments using the `Environments` field.
This allows you to:

- Run different logic in production vs development
- Enable debugging rules only in staging
- Use different destinations per environment
- Test rules before production deployment

## Basic Usage

```go
rule := &fh.SQLRule[Event, Output]{
    ID:           "billing_processor",
    Environments: []string{"production", "staging"},
    Select:         action,
    Into:           destination,
    From:           source,
}
```

This rule activates only when the `ENV` environment variable matches `"production"`
or `"staging"`.

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
rule := &fh.SQLRule[Event, Output]{
    ID: "always_active",
    From: source,
    // No Environments field - active everywhere
}
```

## Common Patterns

### Different Destinations

```go
head, _ = fh.Add(ctx, nil, &fh.SQLRule[Event, Event]{
    ID:           "prod_events",
    Environments: []string{"production"},
    Select:         actions.Identity[Event]{},
    Into:           ProductionDatabase{},
    From:           eventSource,
})
head, _ = fh.Add(ctx, head, &fh.SQLRule[Event, Event]{
    ID:           "dev_events",
    Environments: []string{"development"},
    Select:         actions.Identity[Event]{},
    Into:           LocalDatabase{},
    From:           eventSource,
})
```

### Debug Logging

```go
debugRule := &fh.SQLRule[Event, Event]{
    ID:           "debug_logger",
    Environments: []string{"development", "staging"},
    Select:         actions.Identity[Event]{},
    Into:           ConsoleLogger{},
    From:           source,
}
```

## Multiple Environments

A rule can be active in multiple environments:

```go
rule := &fh.SQLRule[Event, Output]{
    Environments: []string{"production", "staging", "qa"},
    // Active in production, staging, and qa
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

## Next Steps

- Review [Examples](/examples/)
- Check [API Reference](/api/)
