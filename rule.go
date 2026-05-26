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

func AddRule[In, Out any](ctx context.Context, rule Rule[In, Out]) error {
	_, err := rule.When.Start(ctx, ruleToCallback(rule))

	return err
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
	<-ctx.Done()

	return nil
}
