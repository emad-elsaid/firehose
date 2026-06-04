package destinations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/types"
)

// ErrNoGameFound is returned when the Twitch API does not return any game data
// for the given name.
var ErrNoGameFound = errors.New("no game found with the given name")

// ErrNoUserFound is returned when the Twitch API does not return any user data.
var ErrNoUserFound = errors.New("no user found with the given credentials")

// install twitch cli https://github.com/twitchdev/twitch-cli/releases
// configure `twitch configure` with your Twitch credentials
// authentication `twitch token -u -s channel:manage:broadcast`

// TwitchStreamInfo is a destination that updates the Twitch stream information.
type TwitchStreamInfo struct{}

// Send updates the Twitch stream information using the Twitch CLI.
func (t TwitchStreamInfo) Send(ctx context.Context, event events.TwitchStreamInfo) error {
	broadcasterID, err := t.broadcasterID(ctx)
	if err != nil {
		return fmt.Errorf("failed to get broadcaster ID: %w", err)
	}

	body, err := t.request(ctx, event)
	if err != nil {
		return fmt.Errorf("failed to create Twitch API request: %w", err)
	}

	cmd := types.Cmd("twitch", "api", "patch", "channels",
		"-q", "broadcaster_id="+broadcasterID,
		"-b", string(body),
	).WithContext(ctx)

	if cmd.ExitCode() != 0 && !strings.Contains(cmd.Stdout(), "status 204") {
		return fmt.Errorf("failed to send event to Twitch API: %w,stdout: %s, stderr: %s",
			cmd.Error(),
			cmd.Stdout(),
			cmd.Stderr(),
		)
	}

	return nil
}

func (t TwitchStreamInfo) request(ctx context.Context, event events.TwitchStreamInfo) ([]byte, error) {
	request := map[string]any{}
	if event.Title != "" {
		request["title"] = event.Title
	}

	if event.Game != "" {
		gameID, err := t.gameID(ctx, event.Game)
		if err != nil {
			return nil, fmt.Errorf("failed to get game ID: %w", err)
		}

		request["game_id"] = gameID
	}

	if len(event.Tags) > 0 {
		request["tags"] = event.Tags
	}

	return json.Marshal(request)
}

func (t TwitchStreamInfo) gameID(ctx context.Context, name string) (string, error) {
	cmd := types.Cmd("twitch", "api", "get", "games", "-q", "name="+name).WithContext(ctx)

	if cmd.ExitCode() != 0 {
		return "", fmt.Errorf("failed to get game ID: %w, %s", cmd.Error(), cmd.Stderr())
	}

	stdout, err := cmd.StdoutErr()
	if err != nil {
		return "", fmt.Errorf("failed to get game ID: %w", err)
	}

	var resp struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}

	err = json.Unmarshal([]byte(stdout), &resp)
	if err != nil {
		return "", fmt.Errorf("failed to parse Twitch API response: %w", err)
	}

	if len(resp.Data) == 0 {
		return "", ErrNoGameFound
	}

	return resp.Data[0].ID, nil
}

func (t TwitchStreamInfo) broadcasterID(ctx context.Context) (string, error) {
	cmd := types.Cmd("twitch", "api", "get", "users").WithContext(ctx)

	if cmd.ExitCode() != 0 {
		return "", fmt.Errorf("failed to get broadcaster ID: %w, %s", cmd.Error(), cmd.Stderr())
	}

	stdout, err := cmd.StdoutErr()
	if err != nil {
		return "", fmt.Errorf("failed to get broadcaster ID: %w", err)
	}

	var resp struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}

	err = json.Unmarshal([]byte(stdout), &resp)
	if err != nil {
		return "", fmt.Errorf("failed to parse Twitch API response: %w", err)
	}

	if len(resp.Data) == 0 {
		return "", ErrNoUserFound
	}

	return resp.Data[0].ID, nil
}
