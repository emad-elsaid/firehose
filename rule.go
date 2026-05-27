package firehose

import (
	"context"
	"errors"
)

type (
	Rule[In, Out any] struct {
		When Source[In]
		If   string
		Then Action[In, Out]
		To   Destination[Out]
	}

	Source[T any] interface {
		ID() string
		Start(ctx context.Context, cb func(context.Context, T) error) (done context.Context, err error)
	}

	Action[In, Out any] interface {
		Process(ctx context.Context, event In) (Out, error)
	}

	Destination[T any] interface {
		Send(event T) error
	}

	activator func(context.Context) (context.Context, error)

	meta struct {
		activators []activator
	}

	metaKeyCtx struct{}
)

var metaKey = metaKeyCtx{}

func getOrSetMeta(ctx context.Context) (context.Context, *meta) {
	m, ok := ctx.Value(metaKey).(*meta)
	if !ok {
		m = &meta{}
		ctx = context.WithValue(ctx, metaKey, m)
	}

	return ctx, m
}

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
			return err
		}

		return rule.To.Send(out)
	}
}

func Start(ctx context.Context) error {
	ctx, m := getOrSetMeta(ctx)
	activators := m.activators
	contexts := make([]context.Context, len(activators))

	for i := range activators {
		sourceCtx, err := activators[i](ctx)
		if err != nil {
			return err
		}

		contexts[i] = sourceCtx
	}

	<-ctx.Done()

	return waitForSourcesToFinish(contexts)
}

func waitForSourcesToFinish(contexts []context.Context) error {
	errs := make([]error, 0, len(contexts))

	for _, ctx := range contexts {
		<-ctx.Done()
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
