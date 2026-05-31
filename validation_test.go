package firehose

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsValid(t *testing.T) {
	t.Run("empty rule is invalid", func(t *testing.T) {
		rule := &MockRule{}
		in := new(EventMock)

		require.Error(t, IsValid(t.Context(), rule, in))
	})

	t.Run("rule missing source is invalid", func(t *testing.T) {
		rule := &MockRule{
			When: nil,
			Then: &MockAction{},
			To:   &MockDestination{},
		}
		in := new(EventMock)
		require.Error(t, IsValid(t.Context(), rule, in))
	})

	t.Run("rule missing condition is valid", func(t *testing.T) {
		rule := &MockRule{
			When: newSourceMock[*EventMock]("source1"),
			Then: &MockAction{},
			To:   &MockDestination{},
		}
		in := new(EventMock)
		require.NoError(t, IsValid(t.Context(), rule, in))
	})

	t.Run("rule missing action is invalid", func(t *testing.T) {
		rule := &MockRule{
			When: newSourceMock[*EventMock]("source1"),
			If:   "Attr1 == 'value'",
			Then: nil,
			To:   &MockDestination{},
		}
		in := new(EventMock)
		require.Error(t, IsValid(t.Context(), rule, in))
	})

	t.Run("rule missing destination is invalid", func(t *testing.T) {
		rule := &MockRule{
			When: newSourceMock[*EventMock]("source1"),
			Then: &MockAction{},
			To:   nil,
		}
		in := new(EventMock)
		require.Error(t, IsValid(t.Context(), rule, in))
	})

	t.Run("rule with condition that uses non-existing attribute is invalid", func(t *testing.T) {
		rule := &MockRule{
			When: newSourceMock[*EventMock]("source1"),
			If:   "NonExistingAttr == 'value'",
			Then: &MockAction{},
			To:   &MockDestination{},
		}

		in := new(EventMock)
		in.On("Attributes", t.Context()).Return(nil).Once()
		require.Error(t, IsValid(t.Context(), rule, in))
	})
}
