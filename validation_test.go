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
			From:   nil,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}
		require.Error(t, IsValid(rule))
	})

	t.Run("rule missing action is invalid", func(t *testing.T) {
		rule := &MockRule{
			From:   NewMockSource[*EventMock](t),
			Where:  testCond[*EventMock]("Attr1 == 'value'"),
			Select: nil,
			Into:   &MockDestination[*EventMock]{},
		}
		require.Error(t, IsValid(rule))
	})

	t.Run("rule missing destination is invalid", func(t *testing.T) {
		rule := &MockRule{
			From:   NewMockSource[*EventMock](t),
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   nil,
		}
		require.Error(t, IsValid(rule))
	})
}
