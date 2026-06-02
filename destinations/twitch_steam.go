package destinations

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/types"
)

// install twitch cli https://github.com/twitchdev/twitch-cli/releases
// configure `twitch configure` with your Twitch credentials
// authentication `twitch token -u -s channel:manage:broadcast`

type TwitchStreamInfo struct{}

func (t TwitchStreamInfo) Send(ctx context.Context, event events.TwitchStreamInfo) error {
	broadcasterId, err := t.broadcasterID()
	if err != nil {
		return fmt.Errorf("failed to get broadcaster ID: %w", err)
	}

	request := map[string]any{}
	if event.Title != "" {
		request["title"] = event.Title
	}

	if event.Game != "" {
		gameID, err := t.gameID(event.Game)
		if err != nil {
			return fmt.Errorf("failed to get game ID: %w", err)
		}

		request["game_id"] = gameID
	}

	if len(event.Tags) > 0 {
		request["tag_ids"] = event.Tags
	}

	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	cmd := types.Cmd("twitch", "api", "patch", "channels",
		"-q", "broadcaster_id="+broadcasterId,
		"-b", string(body),
	)

	if cmd.ExitCode() != 0 && !strings.Contains(cmd.Stdout(), "status 204") {
		return fmt.Errorf("failed to send event to Twitch API: %w,stdout: %s, stderr: %s",
			cmd.Error(),
			cmd.Stdout(),
			cmd.Stderr(),
		)
	}

	return nil
}

func (t TwitchStreamInfo) gameID(name string) (string, error) {
	cmd := types.Cmd("twitch", "api", "get", "games", "-q", "name="+name)

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
		return "", fmt.Errorf("no game data found in Twitch API response")
	}

	return resp.Data[0].ID, nil
}

func (t TwitchStreamInfo) broadcasterID() (string, error) {
	cmd := types.Cmd("twitch", "api", "get", "users")

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
		return "", fmt.Errorf("no user data found in Twitch API response")
	}

	return resp.Data[0].ID, nil
}
