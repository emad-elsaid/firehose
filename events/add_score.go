package events

import "context"

type AddScore struct {
	Amount int
}

func (a AddScore) Attributes(ctx context.Context) (map[string]any, error) {
	return map[string]any{
		"amount": a.Amount,
	}, nil
}
