package firehose

import (
	"github.com/go-playground/validator/v10"
)

// IsValid validates the rule's fields.
func IsValid[I, O Event](rule *Rule[I, O]) error {
	var validate = validator.New(validator.WithRequiredStructEnabled())

	return validate.Struct(rule)
}
