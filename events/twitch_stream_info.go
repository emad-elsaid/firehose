package events

import "context"

type TwitchStreamInfo struct {
	Title    string
	Category string
	Tags     []string
}

func (TwitchStreamInfo) Attributes(ctx context.Context) map[string]any {
	return map[string]any{}
}
