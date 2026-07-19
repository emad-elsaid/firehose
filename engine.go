package firehose

import (
	"context"
	"os"
	"slices"

	"github.com/go-playground/validator/v10"
)

// Add registers a new processing rule in the context.
func Add(ctx context.Context, registry Registry, rule Registry) (Registry, error) {
	err := isValid(rule)
	if err != nil {
		return nil, err
	}

	if !isEnvironmentEnabled(rule.GetEnvironments(), os.Getenv("ENV")) {
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

func addToRegistry(registry Registry, rule Registry) Registry {
	if registry == nil {
		rule.SetNext(rule)
		rule.SetPrev(rule)

		return rule
	}

	tail := registry.GetPrev()
	sameSourceTail := getSameSourceTail(registry, rule.GetSource())

	linkRule(rule, registry, tail)
	linkSameSourceRule(rule, sameSourceTail)

	return registry
}

func linkRule(rule Registry, head Registry, tail Registry) {
	rule.SetNext(head)
	head.SetPrev(rule)

	if tail != nil {
		rule.SetPrev(tail)
		tail.SetNext(rule)
	}
}

func linkSameSourceRule(rule Registry, sameSourceTail Registry) {
	if sameSourceTail == nil {
		return
	}

	rule.SetPrevSameSource(sameSourceTail)
	sameSourceTail.SetNextSameSource(rule)
}

func getSameSourceTail(registry Registry, source any) Registry {
	tail := registry.GetPrev()
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
func Start(ctx context.Context, registry Registry, errFunc ErrorHandler) []<-chan struct{} {
	var doneChannels []<-chan struct{}

	for current := registry; current != nil; {
		done, err := current.Start(ctx)
		if err != nil {
			if errFunc != nil {
				errFunc(err)
			}
		} else if done != nil {
			doneChannels = append(doneChannels, done)
		}

		current = current.GetNext()
		if current == registry {
			break
		}
	}

	return doneChannels
}

// isValid validates the rule's fields.
func isValid(rule Registry) error {
	validatorInstance := validator.New(validator.WithRequiredStructEnabled())

	return validatorInstance.Struct(rule)
}
