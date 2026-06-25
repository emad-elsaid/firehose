package firehose

import (
	"context"
	"errors"
)

// ErrRuleNotFound is returned when attempting to remove a rule that doesn't exist in the registry.
var ErrRuleNotFound = errors.New("rule not found in registry")

// RemoveRule removes a rule and its subrules recursively from the registry.
// Returns the updated registry and an error if neither the rule nor any of its subrules are found in the registry.
// If the removed rule is the registry head, returns the next rule as the new head (or nil if it was the only rule).
func RemoveRule[I, O Event](registry Registry, rule *Rule[I, O]) (Registry, error) {
	if registry == nil {
		return nil, ErrRuleNotFound
	}

	// Recursively remove all subrules and their descendants
	registry, removedCount := removeRuleAndDescendants(registry, rule)

	// If nothing was removed, the rule and all its subrules were not in the registry
	if removedCount == 0 {
		return registry, ErrRuleNotFound
	}

	return registry, nil
}

// removeRuleAndDescendants recursively removes a rule and all its subrules from the registry.
// Returns the updated registry and the count of removed rules.
func removeRuleAndDescendants[I, O Event](registry Registry, rule *Rule[I, O]) (Registry, int) {
	removedCount := 0

	// Recursively process subrules first (depth-first)
	for i := range rule.SubRules {
		subrule := &rule.SubRules[i]

		var count int

		registry, count = removeRuleAndDescendants(registry, subrule)
		removedCount += count
	}

	// Try to remove the rule itself from registry
	// (it might not be in the registry if it's not activatable)
	if ruleFoundInRegistry(registry, rule) {
		unlinkSameSourceRule(rule)
		registry = unlinkRule(registry, rule)
		removedCount++
	}

	return registry, removedCount
}

// ruleFoundInRegistry checks if a rule exists in the registry.
func ruleFoundInRegistry(registry Registry, rule Registry) bool {
	for current := registry; current != nil; {
		if current == rule {
			return true
		}

		current = current.getNext()
		if current == registry {
			break
		}
	}

	return false
}

// unlinkRule removes a rule from the global circular doubly-linked list.
// Returns the new registry head (or nil if the list becomes empty).
func unlinkRule(registry Registry, rule Registry) Registry {
	next := rule.getNext()
	prev := rule.getPrev()

	// Single rule in registry
	if next == rule {
		rule.setNext(nil)
		rule.setPrev(nil)

		return nil
	}

	// Unlink the rule
	prev.setNext(next)
	next.setPrev(prev)

	rule.setNext(nil)
	rule.setPrev(nil)

	// If we're removing the head, return the next rule as new head
	if registry == rule {
		return next
	}

	return registry
}

// unlinkSameSourceRule removes a rule from the same-source chain.
// If the rule is the first in the same-source chain (has ctx set), transfers the context to the next rule.
func unlinkSameSourceRule(rule sourceRegistry) {
	next := rule.getNextSameSource()
	prev := rule.getPrevSameSource()

	// If this is the first rule with this source (no prev), transfer context to next
	isFirst := prev == nil
	transferCtxIfFirstRule(rule, prev, next)

	// No same-source chain
	if next == nil && prev == nil {
		return
	}

	// Update next's prev pointer
	if next != nil {
		next.setPrevSameSource(prev)
	}

	// Update prev's next pointer
	if prev != nil {
		prev.setNextSameSource(next)
	}

	// Clear pointers on the removed rule
	clearSameSourcePointers(rule, isFirst)
}

// clearSameSourcePointers clears the same-source chain pointers on a removed rule.
// If the rule is the first with its source, keeps nextSameSource for callback forwarding.
func clearSameSourcePointers(rule sourceRegistry, isFirst bool) {
	// EXCEPT: if this is the first rule (no prev), keep nextSameSource
	// so that if the source still calls this rule's callback, it forwards to the next rule
	if !isFirst {
		rule.setNextSameSource(nil)
	}

	rule.setPrevSameSource(nil)
}

// transferCtxIfFirstRule transfers context from first rule to next if applicable.
func transferCtxIfFirstRule(rule, prev, next sourceRegistry) {
	if prev == nil && next != nil {
		ruleCtx := rule.getRegistry().getCtx()
		if ruleCtx != nil {
			// Transfer context to the next rule with same source
			transferContextToRule(ruleCtx, next.getRegistry())
		}
	}
}

// transferContextToRule sets the context on a rule (used when transferring from removed rule).
func transferContextToRule(ctx context.Context, reg Registry) {
	// We need to access the ctx field, but it's not exposed via interface
	// We'll use type assertion to access it
	type contextHolder interface {
		setCtx(c context.Context)
	}

	if ch, ok := reg.(contextHolder); ok {
		ch.setCtx(ctx)
	}
}
