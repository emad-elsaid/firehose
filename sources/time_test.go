package sources

import (
	"testing"
	"time"

	"github.com/emad-elsaid/firehose"
	"github.com/stretchr/testify/require"
)

func TestTimeSource(t *testing.T) {
	t.Run("implements Source interface", func(t *testing.T) {
		require.Implements(t, (*firehose.Source[time.Time])(nil), Time{})
	})
}
