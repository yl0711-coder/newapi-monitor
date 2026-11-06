package monthlydraft

import (
	"context"
	"encoding/json"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Selection is private operator metadata, never a customer-supplied balance or
// flag asserting that a read occurred at a settled historical month boundary.
type WalletObservationSelection struct {
	SourceID      string   `json:"source_id"`
	IDs           []string `json:"ids"`
	ReuseSourceID string   `json:"reuse_source_id,omitempty"`
}

func canonicalWalletSelection(s *WalletObservationSelection, sources []monitor.MonthlyPreparationSource) (*WalletObservationSelection, error) {
	if s == nil {
		return nil, nil // byte-compatible legacy plans and artifacts
	}
	if (s.ReuseSourceID == "" && (len(s.IDs) == 0 || len(s.IDs) > 64)) || (s.ReuseSourceID != "" && (len(s.IDs) != 0 || !validScopeSourceID(s.ReuseSourceID) || s.ReuseSourceID == s.SourceID)) {
		return nil, ErrGenerationInput
	}
	found := false
	for _, source := range sources {
		found = found || source.ID == s.SourceID
	}
	if !found {
		return nil, ErrGenerationInput
	}
	if s.ReuseSourceID != "" {
		exists := false
		for _, source := range sources {
			exists = exists || source.ID == s.ReuseSourceID
		}
		if !exists {
			return nil, ErrGenerationInput
		}
	}
	out := &WalletObservationSelection{SourceID: s.SourceID, IDs: slices.Clone(s.IDs), ReuseSourceID: s.ReuseSourceID}
	sort.Strings(out.IDs)
	for i, id := range out.IDs {
		if !generationHex(id, 32) || (i > 0 && out.IDs[i-1] == id) {
			return nil, ErrGenerationInput
		}
	}
	return out, nil
}

type walletInputBalance struct {
	ID      string                            `json:"observation_id"`
	Balance monitor.MonthlyPreparationBalance `json:"balance"`
}

var walletInputSlots = make(chan struct{}, 1)

// Read a bounded selection from the existing sealed, read-only local receiver.
// No MySQL connection, source writes, schema initialization or recapture occurs.
func (g *Generation) selectedWalletBalances(parent context.Context, p GenerationPlan, input monitor.MonthlyPreparationInput) ([]walletInputBalance, string, error) {
	if err := g.requireObservationStore(p); err != nil {
		return nil, "", err
	}
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	selection, err := canonicalWalletSelection(p.WalletObservations, input.Sources)
	if err != nil || selection == nil {
		return nil, "", ErrGenerationInput
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	select {
	case walletInputSlots <- struct{}{}:
		defer func() { <-walletInputSlots }()
	default:
		return nil, "", ErrGenerationBusy
	}
	var source monitor.MonthlyPreparationSource
	for _, s := range input.Sources {
		if s.ID == selection.SourceID {
			source = s
		}
	}
	if err := g.sealedUsageSource(ctx, source); err != nil {
		return nil, "", err
	}
	if selection.ReuseSourceID != "" {
		return g.selectedReusedWalletBalances(ctx, p, input, selection)
	}
	ids := slices.Clone(p.Funding.UserIDs)
	slices.Sort(ids)
	month, err := time.ParseInLocation("2006-01", p.Funding.Month, time.FixedZone("Usage-UTC+8", 8*3600))
	if err != nil {
		return nil, "", ErrGenerationInput
	}
	end, now := month.AddDate(0, 1, 0).Unix(), time.Now().UnixMilli()
	var out []walletInputBalance
	seen := make(map[[2]int64]bool)
	for _, id := range selection.IDs {
		o, err := monthlysource.LoadWalletObservation(ctx, g.funding, id)
		if err != nil {
			return nil, "", err // busy/cancellation is not proven bad money
		}
		at := o.CompletedAtMS / 1000 // observation second, NOT a source-log cutoff
		if o.SourceIdentity != p.Funding.SourceIdentity || o.CompletedAtMS > now || at < end || len(o.Users) != len(ids) {
			return nil, "", ErrGenerationEvidence
		}
		for i, user := range o.Users {
			if user.UserID != ids[i] || !user.Exists || user.BalanceQuota == nil || user.UsedQuota == nil || seen[[2]int64{user.UserID, at}] {
				return nil, "", ErrGenerationEvidence
			}
			seen[[2]int64{user.UserID, at}] = true
			out = append(out, walletInputBalance{ID: id, Balance: monitor.MonthlyPreparationBalance{
				UserID: user.UserID, At: at, BalanceQuota: *user.BalanceQuota, UsedQuota: *user.UsedQuota,
				Source: selection.SourceID + "#wallet:" + id + ":user:" + strconv.FormatInt(user.UserID, 10), TimeBasis: "collection_observation",
			}})
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if err := g.sealedUsageSource(ctx, source); err != nil {
		return nil, "", err
	}
	// Include complete validated original capture times and values via their
	// immutable payload IDs; the detached normalized observations are exact ints.
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, "", err
	}
	return out, generationHash(raw), nil
}

func (g *Generation) auditWalletInput(ctx context.Context, p GenerationPlan, input monitor.MonthlyPreparationInput) (string, error) {
	if err := g.auditAutomaticHistoryBinding(ctx, p, input); err != nil {
		return "", err
	}
	if p.WalletObservations == nil {
		return "", nil
	}
	balances, digest, err := g.selectedWalletBalances(ctx, p, input)
	if err != nil {
		return "", err
	}
	expected := make(map[[2]int64]monitor.MonthlyPreparationBalance, len(balances))
	for _, b := range balances {
		expected[[2]int64{b.Balance.UserID, b.Balance.At}] = b.Balance
	}
	prefix := p.WalletObservations.SourceID + "#wallet:"
	isWalletRef := func(ref string) bool {
		return strings.HasPrefix(ref, prefix) || (p.WalletObservations.ReuseSourceID != "" && strings.HasPrefix(ref, p.WalletObservations.ReuseSourceID+"#token-wallet:"))
	}
	for _, a := range input.Accounts {
		if isWalletRef(a.Baseline.Source) || isWalletRef(a.Baseline.CutoffSource) || isWalletRef(a.BasisSource) {
			return "", ErrGenerationEvidence
		}
		for _, refund := range a.WalletMonths {
			if isWalletRef(refund.Source) {
				return "", ErrGenerationEvidence // observed wallet != refund applied
			}
		}
		for _, s := range a.Spans {
			if isWalletRef(s.Source) {
				return "", ErrGenerationEvidence
			}
		}
		for _, m := range a.Movements {
			if isWalletRef(m.Source) {
				return "", ErrGenerationEvidence
			}
		}
		for _, c := range a.MovementCoverage {
			if isWalletRef(c.Source) {
				return "", ErrGenerationEvidence
			}
		}
		for _, snapshot := range a.Snapshots {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			key := [2]int64{a.UserID, snapshot.At}
			want, exists := expected[key]
			if isWalletRef(snapshot.CutoffSource) || ((exists || isWalletRef(snapshot.Source)) && (!exists || snapshot != want || snapshot.At <= a.Baseline.At)) {
				return "", ErrGenerationEvidence
			}
			if exists {
				delete(expected, key)
			}
		}
	}
	if len(expected) != 0 {
		return "", ErrGenerationEvidence
	}
	return digest, ctx.Err()
}

// WalletPreparation returns private input bytes only. It neither queues tasks
// nor supplies a historical opening/closing balance or actual-refund evidence.
type WalletPreparation struct {
	Plan, Preparation []byte
	WalletSHA256      string
}

// PrepareWalletObservations appends observations without replacing any existing
// snapshot at that second. A repeated call uses the ORIGINAL template; feeding
// the filled output back in is deliberately not an instruction to overwrite.
func (g *Generation) PrepareWalletObservations(parent context.Context, plan GenerationPlan, template []byte) (WalletPreparation, error) {
	var empty WalletPreparation
	if parent == nil || g == nil || g.funding == nil || g.root == nil {
		return empty, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	p, _, input, err := canonicalGeneration(plan, template, time.Now())
	if err != nil {
		return empty, err
	}
	g, releaseSource, err := g.bindObservationStore(ctx, p, input)
	if err != nil {
		return empty, err
	}
	defer releaseSource()
	if p.WalletObservations == nil {
		return empty, ErrGenerationInput
	}
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return empty, err
	}
	balances, _, err := g.selectedWalletBalances(ctx, p, input)
	if err != nil {
		return empty, err
	}
	accounts := make(map[int64]int, len(input.Accounts))
	for i, a := range input.Accounts {
		accounts[a.UserID] = i
	}
	for _, b := range balances {
		i, exists := accounts[b.Balance.UserID]
		if !exists {
			return empty, ErrGenerationEvidence
		}
		a := &input.Accounts[i]
		for _, s := range a.Snapshots {
			if s.At == b.Balance.At {
				return empty, ErrGenerationInput
			}
		}
		a.Snapshots = append(a.Snapshots, b.Balance)
		if len(a.Snapshots) > 1000 {
			return empty, ErrGenerationCapacity
		}
	}
	for i := range input.Accounts {
		sort.Slice(input.Accounts[i].Snapshots, func(j, k int) bool { return input.Accounts[i].Snapshots[j].At < input.Accounts[i].Snapshots[k].At })
	}
	digest, err := g.auditWalletInput(ctx, p, input)
	if err != nil {
		return empty, err
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
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return WalletPreparation{Plan: canonical, Preparation: raw, WalletSHA256: digest}, nil
}
