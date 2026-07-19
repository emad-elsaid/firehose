package firehose

import (
	"context"
	"os"
	"slices"

	"github.com/go-playground/validator/v10"
)

// Add registers a new processing rule in the context.
func Add(ctx context.Context, head Rule, rule Rule) (Rule, error) {
	err := isValid(rule)
	if err != nil {
		return nil, err
	}

	if !isEnvironmentEnabled(rule.GetEnvironments(), os.Getenv("ENV")) {
		return head, nil
	}

	err = rule.Init(ctx)
	if err != nil {
		return nil, err
	}

	return addToHead(head, rule), nil
}

func isEnvironmentEnabled(environments []string, currentEnvironment string) bool {
	if len(environments) == 0 {
		return true
	}

	return slices.Contains(environments, currentEnvironment)
}

func addToHead(head Rule, rule Rule) Rule {
	if head == nil {
		rule.SetNext(rule)
		rule.SetPrev(rule)

		return rule
	}

	tail := head.GetPrev()
	sameSourceTail := getSameSourceTail(head, rule.GetSource())

	linkRule(rule, head, tail)
	linkSameSourceRule(rule, sameSourceTail)

	return head
}

func linkRule(rule Rule, head Rule, tail Rule) {
	rule.SetNext(head)
	head.SetPrev(rule)

	if tail != nil {
		rule.SetPrev(tail)
		tail.SetNext(rule)
	}
}

func linkSameSourceRule(rule Rule, sameSourceTail Rule) {
	if sameSourceTail == nil {
		return
	}

	rule.SetPrevSameSource(sameSourceTail)
	sameSourceTail.SetNextSameSource(rule)
}

func getSameSourceTail(head Rule, source any) Rule {
	tail := head.GetPrev()
	for current := tail; current != nil; {
		currentSource := current.GetSource()
		if currentSource == source {
			return current
		}

		current = current.GetPrev()
		if current == tail {
			break
		}
	}

	return nil
}

// Start activates all registered rules and returns the done channels.
func Start(ctx context.Context, head Rule, errFunc ErrorHandler) []<-chan struct{} {
	var doneChannels []<-chan struct{}

	for current := head; current != nil; {
		done, err := current.Start(ctx)
		if err != nil {
			if errFunc != nil {
				errFunc(err)
			}
		} else if done != nil {
			doneChannels = append(doneChannels, done)
		}

		current = current.GetNext()
		if current == head {
			break
		}
	}

	return doneChannels
}

// isValid validates the rule's fields.
func isValid(rule Rule) error {
	validatorInstance := validator.New(validator.WithRequiredStructEnabled())

	return validatorInstance.Struct(rule)
}
