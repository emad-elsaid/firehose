package firehose

import (
	"context"
	"slices"
)

// AddRule registers a new processing rule in the context.
func AddRule[I, O Event](
	ctx context.Context,
	registry Registry,
	rule *Rule[I, O],
	callbackMiddlewares func() []CallbackMiddleware[I, O],
	actionsMiddlewares func() []ActionMiddleware[I, O],
	destinationsMiddlewares func() []DestinationMiddleware[I, O],
	inInstance I,
	outInstance O,
) (Registry, error) {
	err := IsValid(rule)
	if err != nil {
		return nil, err
	}

	err = wrapCallbackMiddlewares(ctx, rule, inInstance, callbackMiddlewares)
	if err != nil {
		return nil, err
	}

	err = wrapActionMiddlewares(ctx, rule, inInstance, actionsMiddlewares)
	if err != nil {
		return nil, err
	}

	err = wrapDestinationMiddlewares(ctx, rule, outInstance, destinationsMiddlewares)
	if err != nil {
		return nil, err
	}

	return addRuleToRegistry(registry, rule), nil
}

func wrapCallbackMiddlewares[I, O Event](
	ctx context.Context,
	rule *Rule[I, O],
	inInstance I,
	callbackMiddlewares func() []CallbackMiddleware[I, O],
) error {
	if callbackMiddlewares == nil {
		return nil
	}

	rule.wrappedCallback = rule.callback

	for _, v := range slices.Backward(callbackMiddlewares()) {
		var err error

		rule.wrappedCallback, err = v.Wrap(ctx, *rule, rule.wrappedCallback, inInstance)
		if err != nil {
			return err
		}
	}

	return nil
}

func wrapActionMiddlewares[I, O Event](
	ctx context.Context,
	rule *Rule[I, O],
	inInstance I,
	actionMiddlewares func() []ActionMiddleware[I, O],
) error {
	if actionMiddlewares == nil {
		return nil
	}

	for _, v := range slices.Backward(actionMiddlewares()) {
		var err error

		rule.Then, err = v.Wrap(ctx, *rule, rule.Then, inInstance)
		if err != nil {
			return err
		}
	}

	return nil
}

func wrapDestinationMiddlewares[I, O Event](
	ctx context.Context,
	rule *Rule[I, O],
	out O,
	destinationMiddlewares func() []DestinationMiddleware[I, O],
) error {
	if destinationMiddlewares == nil {
		return nil
	}

	for _, v := range slices.Backward(destinationMiddlewares()) {
		var err error

		rule.To, err = v.Wrap(ctx, *rule, rule.To, out)
		if err != nil {
			return err
		}
	}

	return nil
}

func addRuleToRegistry[I, O Event](registry Registry, rule *Rule[I, O]) Registry {
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
