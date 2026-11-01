package monthlysource

import "errors"

// SplitUsageWindow plans smaller queries only. It never retries, collects,
// asserts coverage or apportions money/counts. Each child must be independently
// read and retained; normal month reconciliation still detects every gap.
// One-hour minimum bounds a day to at most 16 successive binary leaves.
func SplitUsageWindow(r UsageWindowRequest) ([2]UsageWindowRequest, error) {
	var children [2]UsageWindowRequest
	if !validUsageWindowRequest(r) || r.To-r.From < 7200 {
		return children, errors.New("invalid usage split scope or minimum window reached")
	}
	middle := r.From + (r.To-r.From)/2
	children = [2]UsageWindowRequest{r, r}
	children[0].To = middle
	children[1].From = middle
	return children, nil
}
