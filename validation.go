package firehose

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/emad-elsaid/boolexpr"
	"github.com/go-playground/validator/v10"
)

var validate = validator.New(validator.WithRequiredStructEnabled())

func IsValid[In, Out Event](ctx context.Context, rule *Rule[In, Out]) error {
	err := validate.Struct(rule)
	if err != nil {
		return err
	}

	return isValidCondition(ctx, rule)
}

func isValidCondition[In, Out Event](ctx context.Context, rule *Rule[In, Out]) error {
	if rule.If == "" {
		return nil
	}

	err := rule.parseCondition()
	if err != nil {
		return err
	}

	symsList := boolexpr.ListSymbols(*rule.parsedIf)

	var in In
	attrs := slices.Collect(maps.Keys(in.Attributes(ctx)))

	for _, sym := range symsList {
		if !slices.Contains(attrs, sym) {
			return fmt.Errorf("%w: symbol: %s", boolexpr.ErrSymbolNotFound, sym)
		}
	}

	return nil
}
