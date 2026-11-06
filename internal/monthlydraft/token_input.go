package monthlydraft

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

type TokenUsageSourceSelection struct {
	UserID   int64  `json:"user_id"`
	SourceID string `json:"source_id"`
}

type TokenUsagePreparation struct {
	Plan, Preparation []byte
	TokenSHA256       string
}

func canonicalTokenSelections(selections []TokenUsageSourceSelection, input monitor.MonthlyPreparationInput, ids []int64) ([]TokenUsageSourceSelection, error) {
	if len(selections) == 0 {
		for _, a := range input.Accounts {
			if len(a.TokenMonths) != 0 {
				return nil, ErrGenerationInput
			}
		}
		return nil, nil
	}
	if len(selections) != len(ids) {
		return nil, ErrGenerationInput
	}
	sources := map[string]bool{}
	for _, s := range input.Sources {
		sources[s.ID] = true
	}
	result := append([]TokenUsageSourceSelection{}, selections...)
	sort.Slice(result, func(i, j int) bool { return result[i].UserID < result[j].UserID })
	used := map[string]bool{}
	for i, s := range result {
		if s.UserID != ids[i] || !validScopeSourceID(s.SourceID) || !sources[s.SourceID] || used[s.SourceID] {
			return nil, ErrGenerationInput
		}
		used[s.SourceID] = true
	}
	return result, nil
}

func tokenInputPresent(p GenerationPlan, input monitor.MonthlyPreparationInput) bool {
	sources := map[int64]string{}
	for _, s := range p.TokenUsageSources {
		sources[s.UserID] = s.SourceID
	}
	for _, a := range input.Accounts {
		if len(p.TokenUsageSources) == 0 {
			if len(a.TokenMonths) != 0 {
				return false
			}
			continue
		}
		if len(a.TokenMonths) != 1 || a.TokenMonths[0].Month != p.Funding.Month || a.TokenMonths[0].UserID != a.UserID || tokenReferenceSource(a.TokenMonths[0].Source) != sources[a.UserID] {
			return false
		}
	}
	return true
}

func tokenSource(input monitor.MonthlyPreparationInput, id string) (monitor.MonthlyPreparationSource, error) {
	for _, s := range input.Sources {
		if s.ID == id {
			return s, nil
		}
	}
	return monitor.MonthlyPreparationSource{}, ErrGenerationInput
}

func (g *Generation) loadTokenArchive(ctx context.Context, s monitor.MonthlyPreparationSource, p GenerationPlan, userID int64) (monthlysource.TokenUsageMonthArchive, monthlysource.TokenUsageMonthReport, error) {
	var empty monthlysource.TokenUsageMonthArchive
	var noReport monthlysource.TokenUsageMonthReport
	rel, err := g.relativeSource(s.Path)
	if err != nil {
		return empty, noReport, err
	}
	info, err := g.root.Lstat(rel)
	if err != nil {
		return empty, noReport, ErrGenerationIntegrity
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return empty, noReport, ErrGenerationIntegrity
	}
	raw, err := readArchiveFile(ctx, g.root, rel, monthlysource.TokenUsageArchiveLimit)
	if err != nil || generationHash(raw) != s.SHA256 {
		if ctx.Err() != nil {
			return empty, noReport, ctx.Err()
		}
		return empty, noReport, ErrGenerationIntegrity
	}
	archive, report, err := monthlysource.DecodeTokenUsageMonthArchive(ctx, raw, time.Now())
	if err != nil {
		if ctx.Err() != nil {
			return empty, noReport, ctx.Err()
		}
		return empty, noReport, ErrGenerationEvidence
	}
	if archive.UserID != userID || archive.Month != p.Funding.Month || archive.SourceIdentity != p.Funding.SourceIdentity || archive.Rules != p.Funding.Rules {
		return empty, noReport, ErrGenerationEvidence
	}
	if _, err := g.verifyFile(ctx, s, monthlysource.TokenUsageArchiveLimit); err != nil {
		return empty, noReport, err
	}
	current, err := g.root.Lstat(rel)
	if err != nil || !os.SameFile(info, current) {
		return empty, noReport, ErrGenerationIntegrity
	}
	currentStat, ok := current.Sys().(*syscall.Stat_t)
	if !ok || currentStat.Nlink != 1 || currentStat.Uid != uint32(os.Geteuid()) || current.Mode().Perm()&0077 != 0 {
		return empty, noReport, ErrGenerationIntegrity
	}
	return archive, report, ctx.Err()
}

func projectTokenReport(report monthlysource.TokenUsageMonthReport, sourceID string) monitor.MonthlyBillTokenUsage {
	raw, _ := json.Marshal(report)
	t := monitor.MonthlyBillTokenUsage{UserID: report.UserID, Month: report.Month, Source: sourceID + "#token-month:" + generationHash(raw), Rows: []monitor.MonthlyBillTokenEvidence{}}
	for _, r := range report.Rows {
		t.Rows = append(t.Rows, monitor.MonthlyBillTokenEvidence{TokenID: r.TokenID, Name: r.Name, ConsumeQuota: r.ConsumeQuota, RefundQuota: r.RefundQuota})
	}
	return t
}

func sameTokenUsageSpans(ctx context.Context, a monitor.MonthlyPreparationAccount, archive monthlysource.TokenUsageMonthArchive, report monthlysource.TokenUsageMonthReport) bool {
	observed := monthlysource.UsageMonthAccountReport{UserID: a.UserID, Totals: &monthlysource.UsageMonthTotals{}, ObservedSeconds: report.To - report.From, MatchingSeconds: report.To - report.From, MissingWindows: []monthlysource.FundingInterval{}, Windows: []monthlysource.UsageMonthWindowReport{}}
	for _, p := range archive.Pairs {
		observed.Windows = append(observed.Windows, monthlysource.UsageMonthWindowReport{From: p.Newer.Request.From, To: p.Newer.Request.To, Matching: true, Rows: p.Newer.Totals})
	}
	sort.Slice(observed.Windows, func(i, j int) bool { return observed.Windows[i].From < observed.Windows[j].From })
	return sameUsageSpans(ctx, a, observed, report.From, report.To)
}

// Audit source -> complete windows -> each user's original usage spans ->
// exact token rows. Never accept matching organization/month net totals alone.
func (g *Generation) auditTokenInput(ctx context.Context, p GenerationPlan, input monitor.MonthlyPreparationInput) (string, error) {
	if err := g.requireObservationStore(p); err != nil {
		return "", err
	}
	if len(p.TokenUsageSources) == 0 {
		if !tokenInputPresent(p, input) {
			return "", ErrGenerationInput
		}
		return "", nil
	}
	if !tokenInputPresent(p, input) {
		return "", ErrGenerationEvidence
	}
	accounts := map[int64]monitor.MonthlyPreparationAccount{}
	for _, a := range input.Accounts {
		accounts[a.UserID] = a
	}
	baseline := map[int64]monthlysource.UsageMonthAccountReport{}
	if planHasUsage(p) {
		observed, err := g.reviewPlanUsage(ctx, p, input)
		if err != nil {
			return "", err
		}
		for _, a := range observed.Report().Accounts {
			baseline[a.UserID] = a
		}
	}
	var captured monthlysource.CapturedMonthSelection
	var capturedSHA string
	if p.CapturedUsageSourceID != "" {
		var err error
		captured, capturedSHA, _, err = g.capturedUsageSelection(ctx, p, input)
		if err != nil {
			return "", err
		}
	}
	reports := []monthlysource.TokenUsageMonthReport{}
	owners := map[int64]int64{}
	rows := 0
	for i, selected := range p.TokenUsageSources {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		s, err := tokenSource(input, selected.SourceID)
		if err != nil {
			return "", err
		}
		archive, report, err := g.loadTokenArchive(ctx, s, p, selected.UserID)
		if err != nil {
			return "", err
		}
		if planHasUsage(p) && !tokenSourceMatchesBaseline(archive, baseline[selected.UserID], report) {
			return "", ErrGenerationEvidence
		}
		if p.CapturedUsageSourceID != "" && !capturedArchiveReferencesMatch(archive, captured.Tokens[i], capturedSHA) {
			return "", ErrGenerationEvidence
		}
		a, ok := accounts[selected.UserID]
		expected := projectTokenReport(report, s.ID)
		if !ok || !sameTokenUsageSpans(ctx, a, archive, report) || !reflect.DeepEqual(a.TokenMonths, []monitor.MonthlyBillTokenUsage{expected}) {
			return "", ErrGenerationEvidence
		}
		for _, r := range report.Rows {
			rows++
			if rows > 10000 {
				return "", ErrGenerationCapacity
			}
			if r.TokenID > 0 {
				if owner, exists := owners[r.TokenID]; exists && owner != selected.UserID {
					return "", ErrGenerationEvidence
				}
				owners[r.TokenID] = selected.UserID
			}
		}
		reports = append(reports, report)
		delete(accounts, selected.UserID)
	}
	if len(accounts) != 0 {
		return "", ErrGenerationEvidence
	}
	raw, err := json.Marshal(reports)
	if err != nil {
		return "", err
	}
	return generationHash(raw), ctx.Err()
}

// When independent usage captures exist, require identical original group
// metadata as well as amounts/counts. Do not mix two source revisions whose
// monthly sums happen to cancel. No hourly interpolation of a daily capture.
func tokenSourceMatchesBaseline(archive monthlysource.TokenUsageMonthArchive, base monthlysource.UsageMonthAccountReport, report monthlysource.TokenUsageMonthReport) bool {
	if base.UserID != archive.UserID || base.Totals == nil || len(base.MissingWindows) != 0 || base.ObservedSeconds != report.To-report.From || base.MatchingSeconds != report.To-report.From || len(base.Windows) != len(archive.Pairs) {
		return false
	}
	pairs := append([]monthlysource.TokenUsageWindowPair{}, archive.Pairs...)
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Newer.Request.From < pairs[j].Newer.Request.From })
	for i, p := range pairs {
		w := base.Windows[i]
		if !w.Matching || len(w.Issues) != 0 || w.From != p.Newer.Request.From || w.To != p.Newer.Request.To || !reflect.DeepEqual(w.Rows, p.Newer.Totals) {
			return false
		}
	}
	return true
}

var tokenPreparationSlots = make(chan struct{}, 1)

// Fill only an absent breakdown from the explicitly selected original-source
// archives. Existing user money, usage statistics and balances are untouched.
// No file/DB writes, source capture, task submission or bill approval occurs.
func (g *Generation) PrepareTokenUsageInput(parent context.Context, plan GenerationPlan, template []byte) (TokenUsagePreparation, error) {
	var empty TokenUsagePreparation
	if parent == nil || g == nil || g.root == nil {
		return empty, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	select {
	case tokenPreparationSlots <- struct{}{}:
		defer func() { <-tokenPreparationSlots }()
	default:
		return empty, ErrGenerationBusy
	}
	p, _, input, err := canonicalGeneration(plan, template, time.Now())
	if err != nil {
		return empty, err
	}
	g, releaseSource, err := g.bindObservationStore(ctx, p, input)
	if err != nil {
		return empty, err
	}
	defer releaseSource()
	if len(p.TokenUsageSources) == 0 {
		return empty, ErrGenerationInput
	}
	for _, a := range input.Accounts {
		if len(a.TokenMonths) != 0 {
			return empty, ErrGenerationInput
		}
	}
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return empty, err
	}
	accounts := map[int64]int{}
	for i, a := range input.Accounts {
		accounts[a.UserID] = i
	}
	for _, selected := range p.TokenUsageSources {
		s, err := tokenSource(input, selected.SourceID)
		if err != nil {
			return empty, err
		}
		archive, report, err := g.loadTokenArchive(ctx, s, p, selected.UserID)
		if err != nil {
			return empty, err
		}
		i, ok := accounts[selected.UserID]
		if !ok || !sameTokenUsageSpans(ctx, input.Accounts[i], archive, report) {
			return empty, ErrGenerationEvidence
		}
		input.Accounts[i].TokenMonths = []monitor.MonthlyBillTokenUsage{projectTokenReport(report, s.ID)}
	}
	raw, err := json.Marshal(input)
	if err != nil || len(raw) > monitor.MonthlyPreparationInputLimit {
		return empty, ErrGenerationCapacity
	}
	p.PreparationSHA256 = generationHash(raw)
	p, canonical, _, err := canonicalGeneration(p, raw, time.Now())
	if err != nil {
		return empty, err
	}
	digest, err := g.auditTokenInput(ctx, p, input)
	if err != nil {
		return empty, err
	}
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return empty, err
	}
	return TokenUsagePreparation{Plan: canonical, Preparation: raw, TokenSHA256: digest}, ctx.Err()
}

// Keep reference spelling fixed, rather than allowing another retained source
// with the same-looking label to take over a token-aware task.
func tokenReferenceSource(ref string) string { id, _, _ := strings.Cut(ref, "#"); return id }
