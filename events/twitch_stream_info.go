package events

import "context"

// TwitchStreamInfo is an event that represents the information of a Twitch stream.
type TwitchStreamInfo struct {
	Title string
	Game  string
	Tags  []string
}

// Attributes returns the attributes of the Twitch stream information event.
func (TwitchStreamInfo) Attributes(_ context.Context) map[string]any {
	return map[string]any{}
}
