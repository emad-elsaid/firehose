package sources

import (
	"testing"

	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/events"
	"github.com/stretchr/testify/require"
)

func TestTimeSource(t *testing.T) {
	t.Run("implements Source interface", func(t *testing.T) {
		require.Implements(t, (*firehose.Source[events.Time])(nil), Time{})
	})
}
