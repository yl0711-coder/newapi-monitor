package monitor

import "fmt"

// Attribute publication races without logging user IDs, amounts or secrets.
type financeFactChangeError struct {
	Stage  string
	Source string
}

func (e *financeFactChangeError) Error() string {
	return fmt.Sprintf("%s (stage=%s source=%s)", errFinanceFactsChanged, e.Stage, e.Source)
}

func (e *financeFactChangeError) Unwrap() error { return errFinanceFactsChanged }

func financeFactChange(stage, source string) error {
	return &financeFactChangeError{Stage: stage, Source: source}
}
