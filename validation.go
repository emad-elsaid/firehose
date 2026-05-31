package firehose

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/emad-elsaid/boolexpr"
	"github.com/go-playground/validator/v10"
)

// IsValid validates the rule's fields and its condition if provided.
func IsValid[In, Out Event](ctx context.Context, rule *Rule[In, Out], instance In) error {
	var validate = validator.New(validator.WithRequiredStructEnabled())

	err := validate.Struct(rule)
	if err != nil {
		return err
	}

	return isValidCondition(ctx, rule, instance)
}

func isValidCondition[In, Out Event](ctx context.Context, rule *Rule[In, Out], instance In) error {
	if rule.If == "" {
		return nil
	}

	err := rule.parseCondition()
	if err != nil {
		return err
	}

	symsList := boolexpr.ListSymbols(*rule.parsedIf)
	attrs := slices.Collect(maps.Keys(instance.Attributes(ctx)))

	for _, sym := range symsList {
		if !slices.Contains(attrs, sym) {
			return fmt.Errorf("%w: symbol: %s", boolexpr.ErrSymbolNotFound, sym)
		}
	}

	return nil
}
