package actions

import "context"

type Yield[T any] struct{}

func (Yield[T]) Process(ctx context.Context, event T) (T, error) {
	return event, nil
}
