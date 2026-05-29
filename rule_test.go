package firehose

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRuleCallback(t *testing.T) {
	t.Run("successful callback with action and destination", func(t *testing.T) {
		rule := &Rule[int, string]{
			When: newMockSource[int]("source1"),
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		err := rule.callback(t.Context(), 42)

		require.NoError(t, err)

		action := rule.Then.(*mockAction[int, string])
		destination := rule.To.(*mockDestination[string])

		require.Len(t, action.processed, 1)
		require.Equal(t, 42, action.processed[0])
		require.Len(t, destination.sent, 1)
	})

	t.Run("callback with action error", func(t *testing.T) {
		rule := &Rule[int, string]{
			When: newMockSource[int]("source1"),
			Then: &mockAction[int, string]{processErr: errors.New("action error")},
			To:   &mockDestination[string]{},
		}

		err := rule.callback(t.Context(), 42)

		require.Error(t, err)
		require.ErrorContains(t, err, "Action failed")

		action := rule.Then.(*mockAction[int, string])
		destination := rule.To.(*mockDestination[string])

		require.Len(t, action.processed, 1)
		require.Len(t, destination.sent, 0)
	})

	t.Run("callback with destination error", func(t *testing.T) {
		rule := &Rule[int, string]{
			When: newMockSource[int]("source1"),
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{sendErr: errors.New("destination error")},
		}

		err := rule.callback(t.Context(), 42)

		require.Error(t, err)
		require.ErrorContains(t, err, "Destination failed")

		action := rule.Then.(*mockAction[int, string])
		destination := rule.To.(*mockDestination[string])

		require.Len(t, action.processed, 1)
		require.Len(t, destination.sent, 0)
	})

	t.Run("callback chains to next rule with same source", func(t *testing.T) {
		source := newMockSource[int]("source1")

		rule1 := &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		rule2 := &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		// Manually set up the same-source chain
		rule1.nextSameSource = rule2
		rule2.prevSameSource = rule1

		err := rule1.callback(t.Context(), 42)

		require.NoError(t, err)

		action1 := rule1.Then.(*mockAction[int, string])
		destination1 := rule1.To.(*mockDestination[string])

		require.Len(t, action1.processed, 1)
		require.Equal(t, 42, action1.processed[0])
		require.Len(t, destination1.sent, 1)

		// Check that the second rule in the chain was also called
		action2 := rule2.Then.(*mockAction[int, string])
		destination2 := rule2.To.(*mockDestination[string])

		require.Len(t, action2.processed, 1)
		require.Equal(t, 42, action2.processed[0])
		require.Len(t, destination2.sent, 1)
	})

	t.Run("callback chain stops on action error in first rule", func(t *testing.T) {
		source := newMockSource[int]("source1")

		rule1 := &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{processErr: errors.New("action error")},
			To:   &mockDestination[string]{},
		}

		rule2 := &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		rule1.nextSameSource = rule2
		rule2.prevSameSource = rule1

		err := rule1.callback(t.Context(), 42)

		require.Error(t, err)
		require.ErrorContains(t, err, "Action failed")

		action1 := rule1.Then.(*mockAction[int, string])
		require.Len(t, action1.processed, 1)

		// Second rule should not be called
		action2 := rule2.Then.(*mockAction[int, string])
		require.Len(t, action2.processed, 0)
	})

	t.Run("callback chain propagates error from second rule", func(t *testing.T) {
		source := newMockSource[int]("source1")

		rule1 := &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		rule2 := &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{processErr: errors.New("second rule error")},
			To:   &mockDestination[string]{},
		}

		rule1.nextSameSource = rule2
		rule2.prevSameSource = rule1

		err := rule1.callback(t.Context(), 42)

		require.Error(t, err)
		require.ErrorContains(t, err, "Action failed")

		action1 := rule1.Then.(*mockAction[int, string])
		destination1 := rule1.To.(*mockDestination[string])

		require.Len(t, action1.processed, 1)
		require.Len(t, destination1.sent, 1)

		// Second rule was called but failed
		action2 := rule2.Then.(*mockAction[int, string])
		require.Len(t, action2.processed, 1)
	})

	t.Run("callback with three rules in chain", func(t *testing.T) {
		source := newMockSource[int]("source1")

		rule1 := &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		rule2 := &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		rule3 := &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		rule1.nextSameSource = rule2
		rule2.prevSameSource = rule1
		rule2.nextSameSource = rule3
		rule3.prevSameSource = rule2

		err := rule1.callback(t.Context(), 42)

		require.NoError(t, err)

		// All three rules should have processed the event
		action1 := rule1.Then.(*mockAction[int, string])
		require.Len(t, action1.processed, 1)

		action2 := rule2.Then.(*mockAction[int, string])
		require.Len(t, action2.processed, 1)

		action3 := rule3.Then.(*mockAction[int, string])
		require.Len(t, action3.processed, 1)
	})

	t.Run("callback with incompatible next rule type", func(t *testing.T) {
		rule := &Rule[int, string]{
			When: newMockSource[int]("source1"),
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		// Create a mock sourceRegistry with incompatible type
		rule.nextSameSource = &mockIncompatibleSourceRegistry{}

		err := rule.callback(t.Context(), 42)

		require.NoError(t, err)

		action := rule.Then.(*mockAction[int, string])
		destination := rule.To.(*mockDestination[string])

		require.Len(t, action.processed, 1)
		require.Len(t, destination.sent, 1)
	})
}
