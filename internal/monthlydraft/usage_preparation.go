package monthlydraft

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// UsagePreparation contains private, machine-produced inputs, not a saved bill
// or publication approval. The caller must still select these exact bytes via
// PrepareGenerationInput and supply independently established balance, funding,
// directory, wallet-scope and actually-applied-refund evidence.
type UsagePreparation struct {
	Plan        []byte
	Preparation []byte
	UsageSHA256 string
}

// PrepareUsageSpans fills ONLY absent monthly consumption spans from explicitly
// selected retained observations. It never overwrites existing monthly money,
// infers old wallet scope, changes independent evidence, writes files/SQL,
// starts collection or queues a task. Sources must be sealed local files;
// sourceID must identify this Generation's observation DB, not an unrelated
// file whose hash happens to be valid. No customer endpoint calls this helper.
func (g *Generation) PrepareUsageSpans(parent context.Context, plan GenerationPlan, template []byte, sourceID string) (UsagePreparation, error) {
	var empty UsagePreparation
	if parent == nil || g == nil || g.funding == nil || g.root == nil || sourceID == "" {
		return empty, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	p, _, input, err := canonicalGeneration(plan, template, time.Now())
	if err != nil {
		return empty, err
	}
	if p.ObservationStoreSourceID != "" && p.ObservationStoreSourceID != sourceID {
		return empty, ErrGenerationInput
	}
	g, releaseSource, err := g.bindObservationStore(ctx, p, input)
	if err != nil {
		return empty, err
	}
	defer releaseSource()
	if !planHasUsage(p) {
		return empty, ErrGenerationInput
	}
	var source monitor.MonthlyPreparationSource
	for _, s := range input.Sources {
		if s.ID == sourceID {
			source = s
		}
	}
	if source.ID == "" {
		return empty, ErrGenerationInput
	}
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return empty, err
	}
	if err := g.sealedUsageSource(ctx, source); err != nil {
		return empty, err
	}
	observed, err := g.reviewPlanUsage(ctx, p, input)
	if err != nil {
		return empty, err
	}
	report := observed.Report()
	if len(report.Accounts) != len(input.Accounts) {
		return empty, ErrGenerationEvidence
	}
	accounts := make(map[int64]int, len(input.Accounts))
	for i, a := range input.Accounts {
		if _, exists := accounts[a.UserID]; exists {
			return empty, ErrGenerationInput
		}
		accounts[a.UserID] = i
		// Keep independent baseline/history bridge spans, but don't silently
		// replace prior monthly amounts, even if they appear to match.
		for _, span := range a.Spans {
			if span.From < report.To && span.To > report.From {
				return empty, ErrGenerationInput
			}
		}
	}
	for _, account := range report.Accounts {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		i, exists := accounts[account.UserID]
		if !exists || account.Totals == nil || len(account.MissingWindows) != 0 ||
			account.ObservedSeconds != report.To-report.From || account.MatchingSeconds != report.To-report.From {
			return empty, ErrGenerationEvidence
		}
		a := &input.Accounts[i]
		if futureZeroOrigin(*a, report.To) {
			if !emptyObservedBeforeOrigin(*a, account, report.To) {
				return empty, ErrGenerationEvidence
			}
			continue // Keep the genuine later origin; no pre-origin spans.
		}
		for _, window := range account.Windows {
			// Reuse the exact-money/window/source rules of the generation gate.
			// Totals for an entire window are exact; it is never interpolated.
			var consume, refund, requests int64
			for _, row := range window.Rows {
				if row.BillingSource != "wallet" || row.UserID != account.UserID || row.Quota < 0 || row.Records <= 0 {
					return empty, ErrGenerationEvidence
				}
				var ok bool
				switch row.Type {
				case 2:
					consume, ok = usagePreparationAdd(consume, row.Quota)
					if !ok {
						return empty, ErrGenerationEvidence
					}
					requests, ok = usagePreparationAdd(requests, row.Records)
				case 6:
					refund, ok = usagePreparationAdd(refund, row.Quota)
				default:
					return empty, ErrGenerationEvidence
				}
				if !ok || requests > 9007199254740991 {
					return empty, ErrGenerationEvidence
				}
			}
			a.Spans = append(a.Spans, monitor.MonthlyPreparationSpan{UserID: a.UserID, From: window.From, To: window.To,
				ConsumeQuota: consume, RefundQuota: refund, Requests: &requests, Tokens: nil,
				Source: sourceID + "#usage:" + window.Versions.OlderID + ":" + window.Versions.NewerID})
		}
		sort.Slice(a.Spans, func(i, j int) bool { return a.Spans[i].From < a.Spans[j].From })
		if len(a.Spans) > 25000 || !sameUsageSpans(ctx, *a, account, report.From, report.To) {
			if err := ctx.Err(); err != nil {
				return empty, err
			}
			return empty, ErrGenerationEvidence
		}
		delete(accounts, account.UserID)
	}
	raw, err := json.Marshal(input)
	if err != nil || len(raw) > monitor.MonthlyPreparationInputLimit {
		return empty, ErrGenerationCapacity
	}
	p.PreparationSHA256 = generationHash(raw)
	_, canonical, _, err := canonicalGeneration(p, raw, time.Now())
	if err != nil {
		return empty, err
	}
	if err := g.sealedUsageSource(ctx, source); err != nil {
		return empty, err
	}
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	reviewRaw, err := json.Marshal(report)
	if err != nil {
		return empty, err
	}
	return UsagePreparation{Plan: canonical, Preparation: raw, UsageSHA256: generationHash(reviewRaw)}, nil
}

func usagePreparationAdd(a, b int64) (int64, bool) {
	const maxInt64 = int64(1<<63 - 1)
	if a < 0 || b < 0 || b > maxInt64-a {
		return 0, false
	}
	return a + b, true
}

func (g *Generation) sealedUsageSource(ctx context.Context, source monitor.MonthlyPreparationSource) error {
	rel, err := g.relativeSource(source.Path)
	if err != nil {
		return ErrGenerationIntegrity
	}
	info, err := g.root.Lstat(rel)
	if err != nil || !info.Mode().IsRegular() {
		return ErrGenerationIntegrity
	}
	// A main-file digest cannot attest uncheckpointed WAL/journal contents.
	// Never checkpoint a source or delete its sidecars in order to accept it.
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := g.root.Lstat(rel + suffix); !os.IsNotExist(err) {
			return ErrGenerationIntegrity
		}
	}
	var path string
	if err := g.funding.QueryRowContext(ctx, "SELECT file FROM pragma_database_list WHERE name='main'").Scan(&path); err != nil {
		return err
	}
	// Same inode alone is insufficient: another hardlink can have different
	// SQLite sidecars. Require the opened database's resolved filename as well.
	actual, err := filepath.EvalSymlinks(path)
	if err != nil {
		return ErrGenerationIntegrity
	}
	selected, err := filepath.EvalSymlinks(source.Path)
	if err != nil || actual != selected {
		return ErrGenerationIntegrity
	}
	opened, err := os.Stat(path)
	if err != nil || !os.SameFile(info, opened) {
		return ErrGenerationIntegrity
	}
	return ctx.Err()
}
