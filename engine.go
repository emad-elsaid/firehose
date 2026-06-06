package firehose

import (
	"context"
	"slices"
)

// AddRule registers a new processing rule in the context.
func AddRule[In, Out Event](
	ctx context.Context,
	registry Registry,
	rule *Rule[In, Out],
	inInstance In,
	outInstance Out,
) (Registry, error) {
	err := IsValid(rule)
	if err != nil {
		return nil, err
	}

	err = wrapActionMiddlewares(ctx, rule, inInstance)
	if err != nil {
		return nil, err
	}

	err = wrapDestinationMiddlewares(ctx, rule, outInstance)
	if err != nil {
		return nil, err
	}

	return addRuleToRegistry(registry, rule), nil
}

func wrapActionMiddlewares[In, Out Event](
	ctx context.Context,
	rule *Rule[In, Out],
	inInstance In,
) error {
	actionMiddlewares := []ActionMiddleware[In, Out]{
		&PanicActionMiddleware[In, Out]{downstream: nil},
		&IfActionMiddleware[In, Out]{parsedIf: nil, downstream: nil},
	}

	for _, v := range slices.Backward(actionMiddlewares) {
		var err error

		rule.Then, err = v.Wrap(ctx, *rule, rule.Then, inInstance)
		if err != nil {
			return err
		}
	}

	return nil
}

func wrapDestinationMiddlewares[In, Out Event](ctx context.Context, rule *Rule[In, Out], out Out) error {
	destinationMiddlewares := []DestinationMiddleware[In, Out]{
		&PanicDestinationMiddleware[In, Out]{downstream: nil},
	}

	for _, v := range slices.Backward(destinationMiddlewares) {
		var err error

		rule.To, err = v.Wrap(ctx, *rule, rule.To, out)
		if err != nil {
			return err
		}
	}

	return nil
}

func addRuleToRegistry[In, Out Event](registry Registry, rule *Rule[In, Out]) Registry {
	if registry == nil {
		rule.setNext(rule)
		rule.setPrev(rule)

		return rule
	}

	tail := registry.getPrev()
	sameSourceTail := getSameSourceTail(registry, rule.When)

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

func linkSameSourceRule(rule sourceRegistry, sameSourceTail sourceRegistry) {
	if sameSourceTail == nil {
		return
	}

	rule.setPrevSameSource(sameSourceTail)
	sameSourceTail.setNextSameSource(rule)
}

func getSameSourceTail(registry Registry, source any) sourceRegistry {
	tail := registry.getPrev()
	for current := tail; current != nil; {
		currentSource := current.getSource()
		if currentSource == source {
			return current.getSourceRegistry()
		}

		current = current.getPrev()
		if current == tail {
			break
		}
	}

	return nil
}

// Start activates all registered rules.
func Start(ctx context.Context, registry Registry, errChan chan<- error) {
	for current := registry; current != nil; {
		err := current.start(ctx)
		if err != nil {
			errChan <- err
		}

		current = current.getNext()
		if current == registry {
			break
		}
	}
}

// Wait blocks until all rules have completed processing, and sends any errors
// that occurred during processing to the provided error channel.
func Wait(registry Registry, errChan chan<- error) {
	for current := registry; current != nil; {
		ctx := current.getCtx()

		if ctx != nil {
			<-ctx.Done()

			err := ctx.Err()
			if err != nil {
				errChan <- err
			}
		}

		current = current.getNext()
		if current == registry {
			break
		}
	}

	close(errChan)
}
