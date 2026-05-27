// Package firehose provides an event processing pipeline framework.
package firehose

import (
	"context"
	"errors"
	"fmt"
)

type (
	// Rule defines an event processing pipeline from source to destination.
	Rule[In, Out any] struct {
		When Source[In]
		If   string
		Then Action[In, Out]
		To   Destination[Out]
	}

	// Source produces events of type T.
	Source[T any] interface {
		ID() string
		Start(ctx context.Context, cb func(context.Context, T) error) (done context.Context, err error)
	}

	// Action transforms input events to output events.
	Action[In, Out any] interface {
		Process(ctx context.Context, event In) (Out, error)
	}

	// Destination consumes events of type T.
	Destination[T any] interface {
		Send(event T) error
	}

	activator func(context.Context) (context.Context, error)

	meta struct {
		activators []activator
	}

	metaKey string
)

const metaKeyValue metaKey = "firehose-meta"

func getOrSetMeta(ctx context.Context) (context.Context, *meta) {
	existing, ok := ctx.Value(metaKeyValue).(*meta)
	if !ok {
		existing = &meta{
			activators: []activator{},
		}
		ctx = context.WithValue(ctx, metaKeyValue, existing)
	}

	return ctx, existing
}

// AddRule registers a new processing rule in the context.
func AddRule[In, Out any](ctx context.Context, rule Rule[In, Out]) (context.Context, error) {
	fn := ruleToActivator(rule)
	ctx, m := getOrSetMeta(ctx)

	m.activators = append(m.activators, fn)

	return ctx, nil
}

func ruleToActivator[In, Out any](rule Rule[In, Out]) activator {
	return func(ctx context.Context) (context.Context, error) {
		return rule.When.Start(ctx, ruleToCallback(rule))
	}
}

func ruleToCallback[In, Out any](rule Rule[In, Out]) func(context.Context, In) error {
	return func(ctx context.Context, event In) error {
		out, err := rule.Then.Process(ctx, event)
		if err != nil {
			return fmt.Errorf("Action failed: %w", err)
		}

		err = rule.To.Send(out)
		if err != nil {
			return fmt.Errorf("Destination failed: %w", err)
		}

		return nil
	}
}

// Start activates all registered rules and waits for completion.
func Start(ctx context.Context) error {
	ctx, m := getOrSetMeta(ctx)
	activators := m.activators
	contexts := make([]context.Context, 0, len(activators))

	for i := range activators {
		sourceCtx, err := activators[i](ctx)
		if err != nil {
			return err
		}

		contexts = append(contexts, sourceCtx)
	}

	<-ctx.Done()

	return waitForSourcesToFinish(contexts)
}

func waitForSourcesToFinish(contexts []context.Context) error {
	errs := make([]error, 0, len(contexts))

	for _, ctx := range contexts {
		<-ctx.Done()

		err := ctx.Err()
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
