package firehose

import (
	"context"

	"github.com/go-playground/validator/v10"
)

// IsValid validates the rule's fields.
func IsValid[In, Out Event](ctx context.Context, rule *Rule[In, Out]) error {
	var validate = validator.New(validator.WithRequiredStructEnabled())

	return validate.Struct(rule)
}
