package events

// TwitchStreamInfo is an event that represents the information of a Twitch stream.
type TwitchStreamInfo struct {
	Title string
	Game  string
	Tags  []string
}
