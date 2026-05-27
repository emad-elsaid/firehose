package firehose

import (
	"context"
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
)

type activator func(context.Context) (context.Context, error)

type meta struct {
	activators []activator
}

type metaKeyCtx struct{}

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
	fn := func(ctx context.Context) (context.Context, error) {
		return rule.When.Start(ctx, ruleToCallback(rule))
	}

	ctx, m := getOrSetMeta(ctx)
	m.activators = append(m.activators, fn)

	return ctx, nil
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

	for i := range activators {
		_, err := activators[i](ctx)
		if err != nil {
			return err
		}
	}

	<-ctx.Done()

	return nil
}
