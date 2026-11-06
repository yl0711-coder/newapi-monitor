package monitor

import "fmt"

// MonthlyPreparationCoverageIssue describes the first range defect reached by
// the existing exact calculator. It is not an exhaustive collection plan or a
// source attestation. Intervals are half-open; no amount is apportioned here.
// For a split, From/To is the required subwindow and WindowFrom/WindowTo is
// the retained aggregate that cannot be interpolated. Overlaps need review,
// not a new query that would double count the same interval.
type MonthlyPreparationCoverageIssue struct {
	Kind       string `json:"kind"`
	From       int64  `json:"from"`
	To         int64  `json:"to"`
	WindowFrom int64  `json:"window_from,omitempty"`
	WindowTo   int64  `json:"window_to,omitempty"`
	Source     string `json:"source,omitempty"`
}

type monthlyPreparationCoverageError struct {
	issue   MonthlyPreparationCoverageIssue
	message string
}

func (e *monthlyPreparationCoverageError) Error() string { return e.message }

func monthlyCoverageError(source string, cursor, start, end int64) error {
	kind, label := source+"_gap", "消费"
	from, to := cursor, start
	if start < cursor {
		kind = source + "_overlap"
		from, to = start, min(cursor, end)
	}
	if source == "funding" {
		label = "额度变动"
	}
	return &monthlyPreparationCoverageError{
		issue:   MonthlyPreparationCoverageIssue{Kind: kind, From: from, To: to},
		message: fmt.Sprintf("%s覆盖缺口或重叠: %d 至 %d", label, cursor, start),
	}
}
