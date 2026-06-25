package firehose

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestRemoveRule(t *testing.T) {
	t.Run("returns error when registry is nil", func(t *testing.T) {
		rule := &Rule[*EventMock, *EventMock]{
			ID: "rule1",
		}

		newRegistry, err := RemoveRule[*EventMock, *EventMock](nil, rule)

		require.ErrorIs(t, err, ErrRuleNotFound)
		require.Nil(t, newRegistry)
	})

	t.Run("returns error when rule not found in registry", func(t *testing.T) {
		rule1 := &Rule[*EventMock, *EventMock]{
			ID:   "rule1",
			When: NewMockSource[*EventMock](t),
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		rule2 := &Rule[*EventMock, *EventMock]{
			ID:   "rule2",
			When: NewMockSource[*EventMock](t),
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		registry, err := AddRule(
			t.Context(),
			nil,
			rule1,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		newRegistry, err := RemoveRule(registry, rule2)

		require.ErrorIs(t, err, ErrRuleNotFound)
		require.Equal(t, registry, newRegistry)
	})

	t.Run("removes single rule from registry", func(t *testing.T) {
		rule := &Rule[*EventMock, *EventMock]{
			ID:   "rule1",
			When: NewMockSource[*EventMock](t),
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		registry, err := AddRule(
			t.Context(),
			nil,
			rule,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)
		require.NotNil(t, registry)

		newRegistry, err := RemoveRule(registry, rule)

		require.NoError(t, err)
		require.Nil(t, newRegistry)
	})

	t.Run("removes first rule from multi-rule registry", func(t *testing.T) {
		rule1 := &Rule[*EventMock, *EventMock]{
			ID:   "rule1",
			When: NewMockSource[*EventMock](t),
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		rule2 := &Rule[*EventMock, *EventMock]{
			ID:   "rule2",
			When: NewMockSource[*EventMock](t),
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		registry, err := AddRule(
			t.Context(),
			nil,
			rule1,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		registry, err = AddRule(
			t.Context(),
			registry,
			rule2,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		newRegistry, err := RemoveRule(registry, rule1)

		require.NoError(t, err)
		require.NotNil(t, newRegistry)
		require.Equal(t, rule2, newRegistry)

		// Verify rule2 is the only rule
		require.Equal(t, rule2, newRegistry.getNext())
		require.Equal(t, rule2, newRegistry.getPrev())
	})

	t.Run("removes middle rule from multi-rule registry", func(t *testing.T) {
		rule1 := &Rule[*EventMock, *EventMock]{
			ID:   "rule1",
			When: NewMockSource[*EventMock](t),
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		rule2 := &Rule[*EventMock, *EventMock]{
			ID:   "rule2",
			When: NewMockSource[*EventMock](t),
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		rule3 := &Rule[*EventMock, *EventMock]{
			ID:   "rule3",
			When: NewMockSource[*EventMock](t),
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		registry, err := AddRule(
			t.Context(),
			nil,
			rule1,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		registry, err = AddRule(
			t.Context(),
			registry,
			rule2,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		registry, err = AddRule(
			t.Context(),
			registry,
			rule3,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		newRegistry, err := RemoveRule(registry, rule2)

		require.NoError(t, err)
		require.NotNil(t, newRegistry)
		require.Equal(t, rule1, newRegistry)

		// Verify circular list: rule1 <-> rule3 <-> rule1
		require.Equal(t, rule3, newRegistry.getNext())
		require.Equal(t, rule3, newRegistry.getPrev())
		require.Equal(t, rule1, newRegistry.getNext().getNext())
	})

	t.Run("removes last rule from multi-rule registry", func(t *testing.T) {
		rule1 := &Rule[*EventMock, *EventMock]{
			ID:   "rule1",
			When: NewMockSource[*EventMock](t),
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		rule2 := &Rule[*EventMock, *EventMock]{
			ID:   "rule2",
			When: NewMockSource[*EventMock](t),
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		registry, err := AddRule(
			t.Context(),
			nil,
			rule1,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		registry, err = AddRule(
			t.Context(),
			registry,
			rule2,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		newRegistry, err := RemoveRule(registry, rule2)

		require.NoError(t, err)
		require.NotNil(t, newRegistry)
		require.Equal(t, rule1, newRegistry)

		// Verify rule1 is the only rule
		require.Equal(t, rule1, newRegistry.getNext())
		require.Equal(t, rule1, newRegistry.getPrev())
	})

	t.Run("removes rules with same source maintains same-source chain", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)

		rule1 := &Rule[*EventMock, *EventMock]{
			ID:   "rule1",
			When: source,
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		rule2 := &Rule[*EventMock, *EventMock]{
			ID:   "rule2",
			When: source,
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		rule3 := &Rule[*EventMock, *EventMock]{
			ID:   "rule3",
			When: source,
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		registry, err := AddRule(
			t.Context(),
			nil,
			rule1,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		registry, err = AddRule(
			t.Context(),
			registry,
			rule2,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		registry, err = AddRule(
			t.Context(),
			registry,
			rule3,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		// Verify same-source chain before removal: rule1 -> rule2 -> rule3
		require.Equal(t, rule2, rule1.getSourceRegistry().getNextSameSource().getRegistry())
		require.Equal(t, rule3, rule2.getSourceRegistry().getNextSameSource().getRegistry())

		// Remove middle rule
		newRegistry, err := RemoveRule(registry, rule2)

		require.NoError(t, err)
		require.NotNil(t, newRegistry)

		// Verify same-source chain after removal: rule1 -> rule3
		require.Equal(t, rule3, rule1.getSourceRegistry().getNextSameSource().getRegistry())
		require.Equal(t, rule1, rule3.getSourceRegistry().getPrevSameSource().getRegistry())
		require.Nil(t, rule3.getSourceRegistry().getNextSameSource())

		// Verify rule2's pointers are cleared
		require.Nil(t, rule2.getSourceRegistry().getNextSameSource())
		require.Nil(t, rule2.getSourceRegistry().getPrevSameSource())
	})

	t.Run("removes rule with subrules", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)

		rule := &Rule[*EventMock, *EventMock]{
			ID:   "parent",
			When: source,
			SubRules: []Rule[*EventMock, *EventMock]{
				{
					ID:   "child1",
					Then: NewMockAction[*EventMock, *EventMock](t),
					To:   NewMockDestination[*EventMock](t),
				},
				{
					ID:   "child2",
					Then: NewMockAction[*EventMock, *EventMock](t),
					To:   NewMockDestination[*EventMock](t),
				},
			},
		}

		registry, err := AddRule(
			t.Context(),
			nil,
			rule,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)
		require.NotNil(t, registry)

		// Count rules before removal
		count := 0
		for current := registry; current != nil; {
			count++
			current = current.getNext()
			if current == registry {
				break
			}
		}
		require.Equal(t, 2, count) // 2 child rules (parent is not activatable)

		// Remove the parent rule (should remove all subrules)
		newRegistry, err := RemoveRule(registry, rule)

		require.NoError(t, err)
		require.Nil(t, newRegistry) // All rules removed
	})

	t.Run("removes rule with nested subrules recursively", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)

		rule := &Rule[*EventMock, *EventMock]{
			ID:   "grandparent",
			When: source,
			SubRules: []Rule[*EventMock, *EventMock]{
				{
					ID:   "parent1",
					Then: NewMockAction[*EventMock, *EventMock](t),
					To:   NewMockDestination[*EventMock](t),
					SubRules: []Rule[*EventMock, *EventMock]{
						{
							ID:   "child1-1",
							Then: NewMockAction[*EventMock, *EventMock](t),
							To:   NewMockDestination[*EventMock](t),
						},
						{
							ID:   "child1-2",
							Then: NewMockAction[*EventMock, *EventMock](t),
							To:   NewMockDestination[*EventMock](t),
						},
					},
				},
				{
					ID:   "parent2",
					Then: NewMockAction[*EventMock, *EventMock](t),
					To:   NewMockDestination[*EventMock](t),
					SubRules: []Rule[*EventMock, *EventMock]{
						{
							ID:   "child2-1",
							Then: NewMockAction[*EventMock, *EventMock](t),
							To:   NewMockDestination[*EventMock](t),
						},
					},
				},
			},
		}

		registry, err := AddRule(
			t.Context(),
			nil,
			rule,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)
		require.NotNil(t, registry)

		// Count rules before removal
		count := 0
		for current := registry; current != nil; {
			count++
			current = current.getNext()
			if current == registry {
				break
			}
		}
		// Should have: parent1, child1-1, child1-2, parent2, child2-1 = 5 rules
		require.Equal(t, 5, count)

		// Remove the grandparent rule (should recursively remove all descendants)
		newRegistry, err := RemoveRule(registry, rule)

		require.NoError(t, err)
		require.Nil(t, newRegistry) // All rules removed
	})
}

func TestRemoveRule_Concurrency(t *testing.T) {
	t.Run("can remove started rule", func(t *testing.T) {
		mockSource := NewMockSource[*EventMock](t)
		mockAction := NewMockAction[*EventMock, *EventMock](t)
		mockDest := NewMockDestination[*EventMock](t)

		rule := &Rule[*EventMock, *EventMock]{
			ID:   "rule1",
			When: mockSource,
			Then: mockAction,
			To:   mockDest,
		}

		// Setup mock to return a context - match callback function type
		mockSource.EXPECT().Start(
			mock.MatchedBy(func(ctx context.Context) bool {
				return ctx != nil
			}),
			mock.MatchedBy(func(cb Callback[*EventMock]) bool {
				return cb != nil
			}),
		).Return(context.Background(), nil).Once()

		registry, err := AddRule(
			t.Context(),
			nil,
			rule,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		errChan := make(chan error, 1)
		Start(t.Context(), registry, errChan)

		// Verify rule is started
		require.NotNil(t, rule.ctx)

		// Remove the rule
		newRegistry, err := RemoveRule(registry, rule)

		require.NoError(t, err)
		require.Nil(t, newRegistry)
	})

	t.Run("transfers context to next rule with same source when removing first rule", func(t *testing.T) {
		mockSource := NewMockSource[*EventMock](t)

		rule1 := &Rule[*EventMock, *EventMock]{
			ID:   "rule1",
			When: mockSource,
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		rule2 := &Rule[*EventMock, *EventMock]{
			ID:   "rule2",
			When: mockSource, // Same source
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		// Setup mock to return a context
		mockSource.EXPECT().Start(
			mock.MatchedBy(func(ctx context.Context) bool {
				return ctx != nil
			}),
			mock.MatchedBy(func(cb Callback[*EventMock]) bool {
				return cb != nil
			}),
		).Return(context.Background(), nil).Once()

		registry, err := AddRule(
			t.Context(),
			nil,
			rule1,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		registry, err = AddRule(
			t.Context(),
			registry,
			rule2,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		errChan := make(chan error, 1)
		Start(t.Context(), registry, errChan)

		// Verify rule1 is started (has context)
		require.NotNil(t, rule1.ctx)
		originalCtx := rule1.ctx

		// Verify rule2 doesn't have context (it's not the first)
		require.Nil(t, rule2.ctx)

		// Remove the first rule
		newRegistry, err := RemoveRule(registry, rule1)

		require.NoError(t, err)
		require.NotNil(t, newRegistry)
		require.Equal(t, rule2, newRegistry)

		// Verify context was transferred to rule2
		require.NotNil(t, rule2.ctx)
		require.Equal(t, originalCtx, rule2.ctx)
	})

	t.Run("does not transfer context when removing non-first rule with same source", func(t *testing.T) {
		mockSource := NewMockSource[*EventMock](t)

		rule1 := &Rule[*EventMock, *EventMock]{
			ID:   "rule1",
			When: mockSource,
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		rule2 := &Rule[*EventMock, *EventMock]{
			ID:   "rule2",
			When: mockSource, // Same source
			Then: NewMockAction[*EventMock, *EventMock](t),
			To:   NewMockDestination[*EventMock](t),
		}

		// Setup mock to return a context
		mockSource.EXPECT().Start(
			mock.MatchedBy(func(ctx context.Context) bool {
				return ctx != nil
			}),
			mock.MatchedBy(func(cb Callback[*EventMock]) bool {
				return cb != nil
			}),
		).Return(context.Background(), nil).Once()

		registry, err := AddRule(
			t.Context(),
			nil,
			rule1,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		registry, err = AddRule(
			t.Context(),
			registry,
			rule2,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		errChan := make(chan error, 1)
		Start(t.Context(), registry, errChan)

		// Verify rule1 is started (has context)
		require.NotNil(t, rule1.ctx)
		originalCtx := rule1.ctx

		// Verify rule2 doesn't have context
		require.Nil(t, rule2.ctx)

		// Remove the second rule (not the first with same source)
		newRegistry, err := RemoveRule(registry, rule2)

		require.NoError(t, err)
		require.NotNil(t, newRegistry)
		require.Equal(t, rule1, newRegistry)

		// Verify rule1 still has its context
		require.NotNil(t, rule1.ctx)
		require.Equal(t, originalCtx, rule1.ctx)
	})

	t.Run("events still work after removing first rule with same source", func(t *testing.T) {
		mockSource := NewMockSource[*EventMock](t)
		mockAction1 := NewMockAction[*EventMock, *EventMock](t)
		mockAction2 := NewMockAction[*EventMock, *EventMock](t)
		mockDest1 := NewMockDestination[*EventMock](t)
		mockDest2 := NewMockDestination[*EventMock](t)

		rule1 := &Rule[*EventMock, *EventMock]{
			ID:   "rule1",
			When: mockSource,
			Then: mockAction1,
			To:   mockDest1,
		}

		rule2 := &Rule[*EventMock, *EventMock]{
			ID:   "rule2",
			When: mockSource,
			Then: mockAction2,
			To:   mockDest2,
		}

		var capturedCallback Callback[*EventMock]

		mockSource.EXPECT().Start(
			mock.MatchedBy(func(ctx context.Context) bool {
				return ctx != nil
			}),
			mock.MatchedBy(func(cb Callback[*EventMock]) bool {
				capturedCallback = cb
				return cb != nil
			}),
		).Return(context.Background(), nil).Once()

		registry, err := AddRule(
			t.Context(),
			nil,
			rule1,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		registry, err = AddRule(
			t.Context(),
			registry,
			rule2,
			nil,
			nil,
			nil,
			&EventMock{},
			&EventMock{},
		)
		require.NoError(t, err)

		errChan := make(chan error, 1)
		Start(t.Context(), registry, errChan)

		// Remove rule1 (the first with this source)
		registry, err = RemoveRule(registry, rule1)
		require.NoError(t, err)
		require.NotNil(t, rule2.ctx) // Context transferred to rule2

		// Now emit an event through the captured callback
		// The source still has rule1's callback, so both rules will process
		event := &EventMock{}
		event.On("Attributes", mock.Anything).Return(map[string]any{}, nil)

		// Both rule1 and rule2 will process (rule1 still has valid Then/To)
		mockAction1.On("Process", mock.Anything, event, mock.Anything).Return(event, Report{Status: "success"})
		mockDest1.On("Send", mock.Anything, event).Return(Report{Status: "success"})
		mockAction2.On("Process", mock.Anything, event, mock.Anything).Return(event, Report{Status: "success"})
		mockDest2.On("Send", mock.Anything, event).Return(Report{Status: "success"})

		reports := make(chan Report, 10)
		capturedCallback(t.Context(), event, reports)
		close(reports)

		// Collect reports
		var reportList []Report
		for r := range reports {
			reportList = append(reportList, r)
		}

		// Should process through both rules (rule1 is removed from registry but still processes via callback)
		require.Equal(t, 2, len(reportList), "Should have processed both rules")

		// Verify both mocks were called
		mockAction1.AssertExpectations(t)
		mockDest1.AssertExpectations(t)
		mockAction2.AssertExpectations(t)
		mockDest2.AssertExpectations(t)
	})
}
