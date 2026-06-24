package firehose

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsValid(t *testing.T) {
	t.Run("empty rule is invalid", func(t *testing.T) {
		rule := &MockRule{}

		require.Error(t, IsValid(rule))
	})

	t.Run("rule missing source is invalid", func(t *testing.T) {
		rule := &MockRule{
			When: nil,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}
		require.Error(t, IsValid(rule))
	})

	t.Run("rule missing action is invalid", func(t *testing.T) {
		rule := &MockRule{
			When: NewMockSource[*EventMock](t),
			If:   "Attr1 == 'value'",
			Then: nil,
			To:   &MockDestination[*EventMock]{},
		}
		require.Error(t, IsValid(rule))
	})

	t.Run("rule missing destination is invalid", func(t *testing.T) {
		rule := &MockRule{
			When: NewMockSource[*EventMock](t),
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   nil,
		}
		require.Error(t, IsValid(rule))
	})
}
