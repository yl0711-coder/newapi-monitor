package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"slices"
	"sort"
	"time"
)

const usageMonthWindowLimit = 4096
const usageMonthBytesLimit = 64 << 20
const usageMonthBasis = "matching_aggregate_logs_not_wallet_settlement"

var usageMonthSlots = make(chan struct{}, 1)
var ErrUsageMonthBusy = errors.New("monthly usage audit already running")

// UsageWindowVersion selects explicit retained captures; neither report flags
// nor caller-supplied totals are accepted as source observations.
type UsageWindowVersion struct {
	OlderID string `json:"older_id"`
	NewerID string `json:"newer_id"`
}

// UsageMonthObservation can only be constructed by re-reading the local store.
// It is NOT a bill approval, a wallet cutoff, or evidence of successful refunds.
// Aggregate equality cannot detect offsetting edits or prior log retention.
type UsageMonthObservation struct{ report *UsageMonthReport }

func (UsageMonthObservation) MarshalJSON() ([]byte, error) {
	return nil, errors.New("serialize usage Report only; audit observations are process-local")
}

func (o *UsageMonthObservation) UnmarshalJSON([]byte) error {
	if o != nil {
		*o = UsageMonthObservation{}
	}
	return errors.New("usage observations must be rebuilt from retained captures")
}

type UsageMonthReport struct {
	Month          string                    `json:"month"`
	Timezone       string                    `json:"timezone"`
	Basis          string                    `json:"basis"`
	SourceIdentity string                    `json:"source_identity"`
	From           int64                     `json:"from"`
	To             int64                     `json:"to"`
	Accounts       []UsageMonthAccountReport `json:"accounts"`
}

type UsageMonthAccountReport struct {
	UserID          int64                    `json:"user_id"`
	ObservedSeconds int64                    `json:"observed_seconds"`
	MatchingSeconds int64                    `json:"matching_seconds"`
	MissingWindows  []FundingInterval        `json:"missing_windows"`
	Windows         []UsageMonthWindowReport `json:"windows"`
	// Nil for gaps, changed aggregates or missing accounts. An explicit empty
	// Sources list means an observed empty month, never a missing-data zero.
	Totals *UsageMonthTotals `json:"totals"`
}

type UsageMonthWindowReport struct {
	From     int64              `json:"from"`
	To       int64              `json:"to"`
	Versions UsageWindowVersion `json:"versions"`
	Matching bool               `json:"matching"`
	Issues   []string           `json:"issues"`
	// Only matching aggregates are included. Unknown sources remain explicit;
	// they cannot be silently projected into the wallet ledger.
	Rows []UsageWindowRow `json:"rows"`
}

type UsageMonthTotals struct {
	Sources []UsageMonthSourceTotal `json:"sources"`
}

type UsageMonthSourceTotal struct {
	BillingSource  string `json:"billing_source"`
	ConsumeQuota   int64  `json:"consume_quota"`
	RefundQuota    int64  `json:"refund_quota"`
	ConsumeRecords int64  `json:"consume_records"`
	RefundRecords  int64  `json:"refund_records"`
}

// Report returns a detached diagnostic view, not reconstructible audit proof.
func (o UsageMonthObservation) Report() UsageMonthReport {
	if o.report == nil {
		return UsageMonthReport{}
	}
	r := *o.report
	r.Accounts = slices.Clone(r.Accounts)
	for i := range r.Accounts {
		a := &r.Accounts[i]
		a.MissingWindows = slices.Clone(a.MissingWindows)
		a.Windows = slices.Clone(a.Windows)
		for j := range a.Windows {
			a.Windows[j].Rows = slices.Clone(a.Windows[j].Rows)
			a.Windows[j].Issues = slices.Clone(a.Windows[j].Issues)
		}
		if a.Totals != nil {
			a.Totals = &UsageMonthTotals{Sources: slices.Clone(a.Totals.Sources)}
		}
	}
	return r
}

// ReviewUsageMonth reads only a separate, explicitly initialized local receiver.
// One bounded read transaction protects the whole audit from mixed SQLite
// views. It does not connect to MySQL, start captures, migrate, write or publish.
func ReviewUsageMonth(parent context.Context, db *sql.DB, request FundingMonthRequest, versions []UsageWindowVersion) (UsageMonthObservation, error) {
	return reviewUsageMonth(parent, db, request, versions, time.Now())
}

func reviewUsageMonth(parent context.Context, db *sql.DB, request FundingMonthRequest, versions []UsageWindowVersion, now time.Time) (UsageMonthObservation, error) {
	var empty UsageMonthObservation
	if parent == nil || db == nil {
		return empty, errors.New("usage month requires a context and local receiver")
	}
	if err := parent.Err(); err != nil {
		return empty, err
	}
	start, err := time.ParseInLocation("2006-01", request.Month, fundingMonthZone)
	current := time.Date(now.In(fundingMonthZone).Year(), now.In(fundingMonthZone).Month(), 1, 0, 0, 0, 0, fundingMonthZone)
	ids, idsErr := walletIDs(request.UserIDs)
	if err != nil || start.Format("2006-01") != request.Month || start.Year() < 2020 || !start.Before(current) || idsErr != nil ||
		!validDigest(request.SourceIdentity) || request.Rules.Producer != FundingProducerRC4RC26 || request.Rules.QuotaPerUnit != 500000 || len(versions) > usageMonthWindowLimit {
		return empty, errors.New("invalid usage month, source, rules, accounts or window budget")
	}
	seen := make(map[string]bool, len(versions)*2)
	for _, v := range versions {
		for _, id := range []string{v.OlderID, v.NewerID} {
			if !validDigest(id) || seen[id] {
				return empty, errors.New("invalid or reused usage observation version")
			}
			seen[id] = true
		}
	}
	select {
	case usageMonthSlots <- struct{}{}:
		defer func() { <-usageMonthSlots }()
	default:
		return empty, ErrUsageMonthBusy
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return empty, err
	}
	defer func() { _ = tx.Rollback() }()
	r := UsageMonthReport{Month: request.Month, Timezone: "UTC+08:00", Basis: usageMonthBasis, SourceIdentity: request.SourceIdentity,
		From: start.Unix(), To: start.AddDate(0, 1, 0).Unix(), Accounts: make([]UsageMonthAccountReport, len(ids))}
	for i, id := range ids {
		r.Accounts[i] = UsageMonthAccountReport{UserID: id, MissingWindows: []FundingInterval{}, Windows: []UsageMonthWindowReport{}}
	}
	usedBytes := 0
	if err := appendUsageMonthWindows(ctx, tx, request, versions, now, &r, &usedBytes); err != nil {
		return empty, err
	}
	for i := range r.Accounts {
		if err := assembleUsageAccount(ctx, &r.Accounts[i], r.From, r.To); err != nil {
			return empty, err
		}
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	return UsageMonthObservation{report: &r}, nil
}

// Shared ORIGINAL pair/window rules. The explicit partition reader also uses
// this helper with one transaction and one cumulative payload budget.
func appendUsageMonthWindows(ctx context.Context, tx *sql.Tx, request FundingMonthRequest, versions []UsageWindowVersion, now time.Time, r *UsageMonthReport, usedBytes *int) error {
	// Keep both the existing transaction snapshot and cumulative byte limit.
	// Only SQL round trips are batched; every original is still independently
	// hash/canonical/identity validated before the unchanged month audit.
	for from := 0; from < len(versions); from += 64 {
		part := versions[from:min(from+64, len(versions))]
		originals, err := loadUsageMonthBatch(ctx, tx, part, usedBytes)
		if err != nil {
			return err
		}
		if err := appendLoadedUsageMonthWindows(ctx, request, part, originals, now, r); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func appendLoadedUsageMonthWindows(ctx context.Context, request FundingMonthRequest, versions []UsageWindowVersion, originals map[string]UsageWindowObservation, now time.Time, r *UsageMonthReport) error {
	accounts := make(map[int64]int, len(r.Accounts))
	for i, a := range r.Accounts {
		accounts[a.UserID] = i
	}
	for _, v := range versions {
		a, aOK := originals[v.OlderID]
		b, bOK := originals[v.NewerID]
		if !aOK || !bOK {
			return errors.New("usage month original observation missing")
		}
		index, ok := accounts[a.Request.UserID]
		if !ok || a.Request != b.Request || a.Request.SourceIdentity != request.SourceIdentity || a.Request.Rules != request.Rules ||
			a.Request.From < r.From || a.Request.To > r.To || b.StartedAtMS < a.CompletedAtMS || b.StartedAtMS-a.CompletedAtMS < 300000 || b.CompletedAtMS > now.UnixMilli() {
			return errors.New("usage month scope, calendar or independent capture order mismatch")
		}
		w := UsageMonthWindowReport{From: a.Request.From, To: a.Request.To, Versions: v, Matching: slices.Equal(a.Rows, b.Rows), Issues: []string{}, Rows: []UsageWindowRow{}}
		if !w.Matching {
			w.Issues = append(w.Issues, "aggregate_changed")
		} else {
			w.Rows = slices.Clone(b.Rows)
		}
		// Live wallet balances may legitimately differ between captures. They
		// are deliberately neither compared nor used as historical boundaries.
		if !a.Wallet.Exists || !b.Wallet.Exists {
			w.Issues = append(w.Issues, "account_missing")
		}
		for _, row := range w.Rows {
			if row.BillingSource != "wallet" && row.BillingSource != "subscription" {
				issue := "billing_source_" + row.BillingSource
				if !slices.Contains(w.Issues, issue) {
					w.Issues = append(w.Issues, issue)
				}
			}
		}
		r.Accounts[index].Windows = append(r.Accounts[index].Windows, w)
	}
	return ctx.Err()
}

func assembleUsageAccount(ctx context.Context, a *UsageMonthAccountReport, from, to int64) error {
	sort.Slice(a.Windows, func(i, j int) bool { return a.Windows[i].From < a.Windows[j].From })
	cursor, usable := from, true
	totals := make(map[string]UsageMonthSourceTotal)
	for _, w := range a.Windows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if w.From < cursor {
			return errors.New("usage month windows overlap; cannot count the same interval twice")
		}
		if w.From > cursor {
			a.MissingWindows = append(a.MissingWindows, FundingInterval{From: cursor, To: w.From})
		}
		cursor = w.To
		a.ObservedSeconds += w.To - w.From
		if w.Matching {
			a.MatchingSeconds += w.To - w.From
		}
		if !w.Matching || slices.Contains(w.Issues, "account_missing") {
			usable = false
		}
		for _, row := range w.Rows {
			t := totals[row.BillingSource]
			t.BillingSource = row.BillingSource
			amount, records := &t.ConsumeQuota, &t.ConsumeRecords
			if row.Type == 6 {
				amount, records = &t.RefundQuota, &t.RefundRecords
			}
			if row.Quota > math.MaxInt64-*amount || row.Records > math.MaxInt64-*records {
				return errors.New("usage month integer total overflow")
			}
			*amount += row.Quota
			*records += row.Records
			totals[row.BillingSource] = t
		}
	}
	if cursor < to {
		a.MissingWindows = append(a.MissingWindows, FundingInterval{From: cursor, To: to})
	}
	if !usable || len(a.MissingWindows) != 0 {
		return nil
	}
	a.Totals = &UsageMonthTotals{Sources: make([]UsageMonthSourceTotal, 0, len(totals))}
	for _, total := range totals {
		a.Totals.Sources = append(a.Totals.Sources, total)
	}
	sort.Slice(a.Totals.Sources, func(i, j int) bool { return a.Totals.Sources[i].BillingSource < a.Totals.Sources[j].BillingSource })
	return nil
}
