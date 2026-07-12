package firehose

// RuleError wraps an error with the rule ID that produced it.
type RuleError struct {
	Rule string
	Err  error
}

func (e RuleError) Error() string {
	if e.Rule == "" {
		return e.Err.Error()
	}

	return e.Rule + ": " + e.Err.Error()
}

func (e RuleError) Unwrap() error { return e.Err }

// NewRuleError creates an error with the given rule and underlying error.
func NewRuleError(rule string, err error) error {
	if err == nil {
		return nil
	}

	return RuleError{Rule: rule, Err: err}
}
