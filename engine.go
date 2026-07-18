package firehose

import (
	"context"
	"os"
	"slices"

	"github.com/go-playground/validator/v10"
)

// Add registers a new processing rule in the context.
func Add[I, O any](ctx context.Context, registry Registry, rule *Rule[I, O]) (Registry, error) {
	err := isValid(rule)
	if err != nil {
		return nil, err
	}

	if !isEnvironmentEnabled(rule.Environments, os.Getenv("ENV")) {
		return registry, nil
	}

	err = rule.Init(ctx)
	if err != nil {
		return nil, err
	}

	return addToRegistry(registry, rule), nil
}

func isEnvironmentEnabled(environments []string, currentEnvironment string) bool {
	if len(environments) == 0 {
		return true
	}

	return slices.Contains(environments, currentEnvironment)
}

func addToRegistry[I, O any](registry Registry, rule *Rule[I, O]) Registry {
	if registry == nil {
		rule.setNext(rule)
		rule.setPrev(rule)

		return rule
	}

	tail := registry.getPrev()
	sameSourceTail := getSameSourceTail(registry, rule.From)

	linkRule(rule, registry, tail)
	linkSameSourceRule(rule, sameSourceTail)

	return registry
}

func linkRule(rule Registry, head Registry, tail Registry) {
	rule.setNext(head)
	head.setPrev(rule)

	if tail != nil {
		rule.setPrev(tail)
		tail.setNext(rule)
	}
}

func linkSameSourceRule(rule Registry, sameSourceTail Registry) {
	if sameSourceTail == nil {
		return
	}

	rule.setPrevSameSource(sameSourceTail)
	sameSourceTail.setNextSameSource(rule)
}

func getSameSourceTail(registry Registry, source any) Registry {
	tail := registry.getPrev()
	for current := tail; current != nil; {
		currentSource := current.getSource()
		if currentSource == source {
			return current
		}

		current = current.getPrev()
		if current == tail {
			break
		}
	}

	return nil
}

// Start activates all registered rules and returns the done channels.
func Start(ctx context.Context, registry Registry, errFunc ErrorHandler) []<-chan struct{} {
	var doneChannels []<-chan struct{}

	for current := registry; current != nil; {
		done, err := current.start(ctx)
		if err != nil {
			if errFunc != nil {
				errFunc(err)
			}
		} else if done != nil {
			doneChannels = append(doneChannels, done)
		}

		current = current.getNext()
		if current == registry {
			break
		}
	}

	return doneChannels
}

// isValid validates the rule's fields.
func isValid[I, O any](rule *Rule[I, O]) error {
	validatorInstance := validator.New(validator.WithRequiredStructEnabled())

	return validatorInstance.Struct(rule)
}
