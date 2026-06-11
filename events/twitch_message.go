package events

import "context"

type TwitchMessage struct {
	User    string
	Message string
}

func (t TwitchMessage) Attributes(ctx context.Context) (map[string]any, error) {
	return map[string]any{
		"user":    t.User,
		"message": t.Message,
	}, nil
}
