package firehose

// Report represents the result of processing an event through a rule.
type Report struct {
	Rule string
	Err  error
}

// NewSuccessReport creates a new Report for a successful operation.
func NewSuccessReport() Report {
	return Report{Rule: "", Err: nil}
}

// NewReport creates a new Report with the given error.
func NewReport(err error) Report {
	return Report{Rule: "", Err: err}
}

// NewRuleReport creates a new Report with the given rule and error.
func NewRuleReport(rule string, err error) Report {
	return Report{Rule: rule, Err: err}
}

func (r Report) String() string {
	if r.Err == nil {
		if r.Rule == "" {
			return "Success"
		}

		return "Success " + r.Rule
	}

	if r.Rule == "" {
		return r.Err.Error()
	}

	return r.Rule + ": " + r.Err.Error()
}
