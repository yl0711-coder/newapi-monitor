package monthlysource

import (
	"context"
	"errors"
	"sort"
	"time"
)

const fundingMonthWindowLimit = 4096
const fundingMonthAccountWindowsLimit = 150000
const fundingMonthRecordsLimit = 100000
const fundingMonthReferenceLimit = 200000

var fundingMonthZone = time.FixedZone("CST", 8*60*60)
var fundingMonthSlots = make(chan struct{}, 1)
var ErrFundingMonthBusy = errors.New("monthly funding assembly already running")

// FundingWindowObservation is process-local and immutable through its public
// API. JSON/report round-trips deliberately lose the private observation: a
// report is not a trusted replacement for checking the retained source pages.
type FundingWindowObservation struct {
	data *fundingWindowObservationData
}

func (FundingWindowObservation) MarshalJSON() ([]byte, error) {
	return nil, errors.New("funding observations are process-local; serialize Comparison for reporting only")
}

func (o *FundingWindowObservation) UnmarshalJSON([]byte) error {
	if o != nil {
		*o = FundingWindowObservation{}
	}
	return errors.New("funding observations must be rebuilt by auditing retained pages, not JSON")
}

type fundingWindowObservationData struct {
	comparison               FundingScanComparison
	scan                     FundingScan
	olderEndMS, newerStartMS int64
	olderFence, newerFence   int64
	newerRecords             map[int64]fundingObservedRecord
}

func newFundingWindowObservation(a, b fundingAuditedScan, comparison FundingScanComparison) FundingWindowObservation {
	// a/b/result were allocated by the private audit and are not returned to
	// callers. Keeping b's compact identities avoids retaining any source text.
	return FundingWindowObservation{data: &fundingWindowObservationData{
		comparison: comparison, scan: b.scan, olderEndMS: a.lastMS, newerStartMS: b.firstMS,
		olderFence: a.fence, newerFence: b.fence, newerRecords: b.records,
	}}
}

// Comparison returns a detached report; changing it cannot alter the window
// used by ReviewFundingMonth. This method does not expose raw audit metadata.
func (o FundingWindowObservation) Comparison() FundingScanComparison {
	if o.data == nil {
		return FundingScanComparison{}
	}
	r := o.data.comparison
	r.Changes = append([]FundingObservedChange(nil), r.Changes...)
	for i := range r.Changes {
		r.Changes[i].AffectedUserIDs = append([]int64(nil), r.Changes[i].AffectedUserIDs...)
	}
	r.Accounts = append([]FundingAccountComparison(nil), r.Accounts...)
	for i := range r.Accounts {
		r.Accounts[i].ChangedLogIDs = append([]int64(nil), r.Accounts[i].ChangedLogIDs...)
		r.Accounts[i].ReviewLogIDs = append([]int64(nil), r.Accounts[i].ReviewLogIDs...)
		r.Accounts[i].SeparateLedgerLogIDs = append([]int64(nil), r.Accounts[i].SeparateLedgerLogIDs...)
	}
	r.UnscopedChangedLogIDs = append([]int64(nil), r.UnscopedChangedLogIDs...)
	r.UnscopedReviewLogIDs = append([]int64(nil), r.UnscopedReviewLogIDs...)
	return r
}

type FundingMonthRequest struct {
	Month          string
	SourceIdentity string
	Rules          FundingRules
	UserIDs        []int64
}

type FundingInterval struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

// All statuses/seconds below describe source observations only. There is no
// financial Complete/Publishable flag or automatic conversion to bill evidence.
type FundingMonthReview struct {
	Month          string                       `json:"month"`
	Timezone       string                       `json:"timezone"`
	SourceIdentity string                       `json:"source_identity"`
	From           int64                        `json:"from"`
	To             int64                        `json:"to"`
	MissingWindows []FundingInterval            `json:"missing_windows"`
	Windows        []FundingMonthWindowVersion  `json:"windows"`
	Conflicts      []FundingCrossWindowConflict `json:"conflicts"`
	Accounts       []FundingMonthAccountReview  `json:"accounts"`
}

// Shared once per month, not copied into every account's window result.
type FundingMonthWindowVersion struct {
	From        int64  `json:"from"`
	To          int64  `json:"to"`
	OlderScanID string `json:"older_scan_id"`
	OlderHeadID string `json:"older_head_id"`
	NewerScanID string `json:"newer_scan_id"`
	NewerHeadID string `json:"newer_head_id"`
}

type FundingCrossWindowConflict struct {
	LogID           int64             `json:"log_id"`
	Windows         []FundingInterval `json:"windows"`
	AffectedUserIDs []int64           `json:"affected_user_ids"`
	ScopeUncertain  bool              `json:"scope_uncertain"`
}

type FundingMonthAccountReview struct {
	UserID          int64                        `json:"user_id"`
	ObservedSeconds int64                        `json:"observed_seconds"`
	MatchingSeconds int64                        `json:"matching_seconds"`
	SelectedSeconds int64                        `json:"selected_seconds"`
	Windows         []FundingAccountWindowReview `json:"windows"`
}

type FundingAccountWindowReview struct {
	// Selected means this account selected the NEWER observation in the
	// shared Windows[WindowIndex]; false selects neither, never both.
	WindowIndex       int      `json:"window_index"`
	Selected          bool     `json:"selected"`
	Issues            []string `json:"issues,omitempty"`
	ChangedLogIDs     []int64  `json:"changed_log_ids,omitempty"`
	ReviewLogIDs      []int64  `json:"review_log_ids,omitempty"`
	SeparateLogIDs    []int64  `json:"separate_log_ids,omitempty"`
	ConflictingLogIDs []int64  `json:"conflicting_log_ids,omitempty"`
	UnscopedLogIDs    []int64  `json:"unscoped_log_ids,omitempty"`
}

// ReviewFundingMonth is a bounded, process-local assembly step, with NO DB,
// source queries, writes, schedule or publication. Each window must first have
// been produced by ReviewFundingWindow. Month boundaries follow Usage's UTC+8
// closed-natural-month convention. Incomplete coverage is explicitly reported;
// malformed/overlapping/mixed-scope input errors discard the whole result.
func ReviewFundingMonth(parent context.Context, request FundingMonthRequest, windows []FundingWindowObservation) (FundingMonthReview, error) {
	return reviewFundingMonth(parent, request, windows, time.Now())
}

func reviewFundingMonth(parent context.Context, request FundingMonthRequest, windows []FundingWindowObservation, now time.Time) (FundingMonthReview, error) {
	if err := parent.Err(); err != nil {
		return FundingMonthReview{}, err
	}
	select {
	case fundingMonthSlots <- struct{}{}:
		defer func() { <-fundingMonthSlots }()
	default:
		return FundingMonthReview{}, ErrFundingMonthBusy
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	start, err := time.ParseInLocation("2006-01", request.Month, fundingMonthZone)
	current := time.Date(now.In(fundingMonthZone).Year(), now.In(fundingMonthZone).Month(), 1, 0, 0, 0, 0, fundingMonthZone)
	ids, idsErr := walletIDs(request.UserIDs)
	if err != nil || start.Format("2006-01") != request.Month || start.Year() < 2020 || !start.Before(current) || idsErr != nil || !validDigest(request.SourceIdentity) || request.Rules.Producer != FundingProducerRC4RC26 || request.Rules.QuotaPerUnit != 500000 {
		return FundingMonthReview{}, errors.New("invalid funding month, source, rules or account scope")
	}
	from, to := start.Unix(), start.AddDate(0, 1, 0).Unix()
	ordered, err := validateFundingMonthWindows(ctx, request, ids, windows, from, to)
	if err != nil {
		return FundingMonthReview{}, err
	}
	r := FundingMonthReview{Month: request.Month, Timezone: "UTC+08:00", SourceIdentity: request.SourceIdentity, From: from, To: to}
	cursor := from
	for _, w := range ordered {
		if w.scan.From > cursor {
			r.MissingWindows = append(r.MissingWindows, FundingInterval{From: cursor, To: w.scan.From})
		}
		cursor = w.scan.To
		c := w.comparison
		r.Windows = append(r.Windows, FundingMonthWindowVersion{From: c.From, To: c.To, OlderScanID: c.OlderScanID, OlderHeadID: c.OlderHeadID, NewerScanID: c.NewerScanID, NewerHeadID: c.NewerHeadID})
	}
	if cursor < to {
		r.MissingWindows = append(r.MissingWindows, FundingInterval{From: cursor, To: to})
	}
	conflicts, err := fundingMonthConflicts(ctx, ordered, ids)
	if err != nil {
		return FundingMonthReview{}, err
	}
	r.Conflicts = conflicts
	byAccount, err := fundingConflictIndex(ctx, conflicts)
	if err != nil {
		return FundingMonthReview{}, err
	}
	references := 0
	for accountIndex, id := range ids {
		account := FundingMonthAccountReview{UserID: id}
		for windowIndex, w := range ordered {
			if err := ctx.Err(); err != nil {
				return FundingMonthReview{}, err
			}
			comparison := w.comparison
			a := comparison.Accounts[accountIndex]
			// Count a conservative upper bound BEFORE copying account-specific
			// issue lists; ambiguous records must not expand 200-fold unbounded.
			references += len(a.ChangedLogIDs) + len(a.ReviewLogIDs) + len(a.SeparateLedgerLogIDs) + len(comparison.UnscopedChangedLogIDs) + len(comparison.UnscopedReviewLogIDs) + len(byAccount[id][w.scan.From]) + len(byAccount[0][w.scan.From])
			if references > fundingMonthReferenceLimit {
				return FundingMonthReview{}, errors.New("funding month issue-reference budget exceeded; split account batches")
			}
			window := fundingAccountMonthWindow(w, a, fundingUnionIDs(byAccount[id][w.scan.From], byAccount[0][w.scan.From]))
			window.WindowIndex = windowIndex
			duration := w.scan.To - w.scan.From
			account.ObservedSeconds += duration
			if a.ObservationsMatch && len(window.ConflictingLogIDs) == 0 {
				account.MatchingSeconds += duration
			}
			if len(window.Issues) == 0 {
				window.Selected = true
				account.SelectedSeconds += duration
			}
			account.Windows = append(account.Windows, window)
		}
		r.Accounts = append(r.Accounts, account)
	}
	if err := ctx.Err(); err != nil {
		return FundingMonthReview{}, err
	}
	return r, nil
}

func validateFundingMonthWindows(ctx context.Context, request FundingMonthRequest, ids []int64, windows []FundingWindowObservation, from, to int64) ([]*fundingWindowObservationData, error) {
	if len(windows) > fundingMonthWindowLimit || len(windows)*len(ids) > fundingMonthAccountWindowsLimit {
		return nil, errors.New("funding month window budget exceeded; split account batches or review collection plan")
	}
	ordered := make([]*fundingWindowObservationData, 0, len(windows))
	seen := make(map[string]bool)
	totalRecords := 0
	var oldEnd, newStart, oldFence, newFence int64
	for i, observation := range windows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		w := observation.data
		if w == nil {
			return nil, errors.New("funding window must come from a full local audit, not a report or zero value")
		}
		s := w.scan
		if s.SourceIdentity != request.SourceIdentity || s.Rules != request.Rules || s.From < from || s.To > to || len(s.UserIDs) != len(ids) {
			return nil, errors.New("funding month source, rules, bounds or scope mismatch")
		}
		for j, id := range ids {
			if s.UserIDs[j] != id {
				return nil, errors.New("funding month cannot mix account scopes")
			}
		}
		for _, scanID := range []string{w.comparison.OlderScanID, w.comparison.NewerScanID} {
			if seen[scanID] {
				return nil, errors.New("funding month cannot reuse scan versions")
			}
			seen[scanID] = true
		}
		totalRecords += len(w.newerRecords) + len(w.comparison.Changes)
		if totalRecords > fundingMonthRecordsLimit {
			return nil, errors.New("funding month identity budget exceeded")
		}
		oldEnd, oldFence = max(oldEnd, w.olderEndMS), max(oldFence, w.olderFence)
		if i == 0 || w.newerStartMS < newStart {
			newStart = w.newerStartMS
		}
		if i == 0 || w.newerFence < newFence {
			newFence = w.newerFence
		}
		ordered = append(ordered, w)
	}
	// No mixing a completed pair for Monday with a later first pass for Tuesday.
	// Both full sweeps are still observations, not one atomic source snapshot.
	if len(windows) > 0 && (newStart <= oldEnd || newFence < oldFence) {
		return nil, errors.New("funding month requires two ordered scan sweeps with nonregressing fences")
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].scan.From < ordered[j].scan.From })
	cursor := from
	for _, w := range ordered {
		if w.scan.From < cursor {
			return nil, errors.New("funding month overlapping or duplicate windows")
		}
		cursor = w.scan.To
	}
	return ordered, nil
}

func fundingMonthConflicts(ctx context.Context, windows []*fundingWindowObservationData, ids []int64) ([]FundingCrossWindowConflict, error) {
	type occurrence struct {
		window int
		record fundingObservedRecord
	}
	first := make(map[int64]occurrence)
	duplicates := make(map[int64][]occurrence)
	for i, w := range windows {
		for id, record := range w.newerRecords {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			item := occurrence{window: i, record: record}
			if previous, found := first[id]; found {
				if len(duplicates[id]) == 0 {
					duplicates[id] = append(duplicates[id], previous)
				}
				duplicates[id] = append(duplicates[id], item)
			} else {
				first[id] = item
			}
		}
	}
	logIDs := make([]int64, 0, len(duplicates))
	for id := range duplicates {
		logIDs = append(logIDs, id)
	}
	sort.Slice(logIDs, func(i, j int) bool { return logIDs[i] < logIDs[j] })
	var result []FundingCrossWindowConflict
	for _, id := range logIDs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conflict := FundingCrossWindowConflict{LogID: id}
		affected := make(map[int64]bool)
		for _, item := range duplicates[id] {
			w := windows[item.window]
			conflict.Windows = append(conflict.Windows, FundingInterval{From: w.scan.From, To: w.scan.To})
			if item.record.targetKnown {
				affected[item.record.target] = true
			} else if item.record.status != FundingNonWallet {
				conflict.ScopeUncertain = true
			}
		}
		for _, userID := range ids {
			if affected[userID] {
				conflict.AffectedUserIDs = append(conflict.AffectedUserIDs, userID)
			}
		}
		result = append(result, conflict)
	}
	return result, nil
}

func fundingConflictIndex(ctx context.Context, conflicts []FundingCrossWindowConflict) (map[int64]map[int64][]int64, error) {
	index := make(map[int64]map[int64][]int64)
	references := 0
	for _, conflict := range conflicts {
		ids := conflict.AffectedUserIDs
		if conflict.ScopeUncertain {
			ids = append(append([]int64(nil), ids...), 0) // zero is the unscoped bucket, not an account
		}
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			references += len(conflict.Windows)
			if references > fundingMonthReferenceLimit {
				return nil, errors.New("funding month conflict-reference budget exceeded; split account batches")
			}
			if index[id] == nil {
				index[id] = make(map[int64][]int64)
			}
			for _, window := range conflict.Windows {
				index[id][window.From] = append(index[id][window.From], conflict.LogID)
			}
		}
	}
	return index, nil
}

func fundingAccountMonthWindow(w *fundingWindowObservationData, account FundingAccountComparison, conflicts []int64) FundingAccountWindowReview {
	c := w.comparison
	r := FundingAccountWindowReview{
		ChangedLogIDs: append([]int64(nil), account.ChangedLogIDs...), ReviewLogIDs: append([]int64(nil), account.ReviewLogIDs...),
		SeparateLogIDs: append([]int64(nil), account.SeparateLedgerLogIDs...), ConflictingLogIDs: append([]int64(nil), conflicts...),
		UnscopedLogIDs: fundingUnionIDs(c.UnscopedChangedLogIDs, c.UnscopedReviewLogIDs),
	}
	if !account.ObservationsMatch {
		r.Issues = append(r.Issues, "observations_changed")
	}
	if len(r.ReviewLogIDs) > 0 {
		r.Issues = append(r.Issues, "funding_needs_review")
	}
	if len(r.SeparateLogIDs) > 0 {
		r.Issues = append(r.Issues, "separate_ledger_evidence_required")
	}
	if len(r.UnscopedLogIDs) > 0 {
		r.Issues = append(r.Issues, "unresolved_account_scope")
	}
	if len(r.ConflictingLogIDs) > 0 {
		r.Issues = append(r.Issues, "cross_window_identity_conflict")
	}
	return r
}

func fundingUnionIDs(a, b []int64) []int64 {
	if len(a)+len(b) == 0 {
		return nil
	}
	seen := make(map[int64]bool)
	for _, ids := range [][]int64{a, b} {
		for _, id := range ids {
			seen[id] = true
		}
	}
	result := make([]int64, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}
