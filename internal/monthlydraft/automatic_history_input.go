package monthlydraft

// Source composition for the explicit original-history runtime mode. Legacy
// callers keep the strict no-policy wrapper and its unchanged input format.
import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// A process-local intermediate, not a selected generation or customer bill.
// In particular, historical origins remain arithmetic origins, not observed
// month-boundary balances. Residuals are never folded into logged consumption.
type automaticHistoryInput struct {
	observed        ObservedPreparation
	originals       auditedAutomaticHistory
	resolutionUsers []int64
}

func (automaticHistoryInput) MarshalJSON() ([]byte, error) {
	return nil, errors.New("automatic history input is not publication evidence")
}

func (a *automaticHistoryInput) UnmarshalJSON([]byte) error {
	if a != nil {
		*a = automaticHistoryInput{}
	}
	return errors.New("automatic history input must be rebuilt from original files")
}

// Re-audit originals on every invocation, then use the existing absent-only
// target-month normalizers. No history JSON, operator amounts, current wallet
// as a historical snapshot, task writes or source queries are accepted here.
func (g *Generation) prepareAutomaticHistoryInput(parent context.Context, ref monthlysource.MonthlyObservationFile, req SourcePreparationRequest) (automaticHistoryInput, error) {
	return g.prepareAutomaticHistoryInputWithPolicy(parent, ref, req, nil)
}

func (g *Generation) prepareAutomaticHistoryInputWithPolicy(parent context.Context, ref monthlysource.MonthlyObservationFile, req SourcePreparationRequest, policy *monthlysource.MonthlyObservationFile) (automaticHistoryInput, error) {
	var empty automaticHistoryInput
	if parent == nil {
		return empty, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	audited, err := g.auditAutomaticHistoryForPlan(ctx, ref, req, policy)
	if err != nil {
		return empty, err
	}
	plan, template, unresolved, err := projectAutomaticHistoryInput(ctx, audited, req)
	if err != nil {
		return empty, err
	}
	observed, err := g.PrepareObservedInput(ctx, plan, template, plan.ObservationStoreSourceID)
	if err != nil {
		return empty, err
	}
	// Compare the target after its independent normalizers have run, as well as
	// preserving the historical part. A future composition error must not quietly
	// turn the raw chain into different charges or zero-filled token usage.
	if err := verifyAutomaticHistoryInput(ctx, observed, audited, req); err != nil {
		return empty, err
	}
	if err := g.verifyFiles(ctx, audited.sources); err != nil {
		return empty, err
	}
	return automaticHistoryInput{observed: observed, originals: audited, resolutionUsers: unresolved}, ctx.Err()
}

func projectAutomaticHistoryInput(ctx context.Context, audited auditedAutomaticHistory, req SourcePreparationRequest) (GenerationPlan, []byte, []int64, error) {
	var zero GenerationPlan
	if ctx == nil || audited.target < 0 || audited.target >= len(audited.months) || !audited.history.MatchesCollectionScope(audited.scope) {
		return zero, nil, nil, ErrGenerationEvidence
	}
	r := audited.history.Report()
	target := audited.months[audited.target]
	owner := audited.scope
	if len(audited.sources) > 512 {
		return zero, nil, nil, ErrGenerationCapacity
	}
	if req.Owner.GroupID != owner.GroupID || req.Owner.AuthVersion != owner.AuthVersion ||
		req.Owner.MemberManifest != owner.MemberManifest || req.Owner.MemberFingerprint != owner.MemberFingerprint ||
		owner.Funding.Month != req.Month || target.material.Funding.Month != req.Month || len(r.Accounts) != len(owner.Funding.UserIDs) ||
		len(target.sources) != len(target.material.Tokens)+3 {
		return zero, nil, nil, ErrGenerationEvidence
	}
	start, err := time.ParseInLocation("2006-01", r.FromMonth, time.FixedZone("Usage-UTC+8", 8*3600))
	if err != nil {
		return zero, nil, nil, ErrGenerationEvidence
	}
	input := monitor.MonthlyPreparationInput{Version: 1, Sources: append([]monitor.MonthlyPreparationSource(nil), audited.sources...)}
	var unresolved []int64
	for i, user := range r.Accounts {
		if err := ctx.Err(); err != nil {
			return zero, nil, nil, err
		}
		if user.UserID != owner.Funding.UserIDs[i] || len(user.Months) != len(audited.months) {
			return zero, nil, nil, ErrGenerationEvidence
		}
		origin := automaticHistorySourceID + "#derived-origin:user:" + strconv.FormatInt(user.UserID, 10)
		// Names/directory scope are intentionally not invented. They must be
		// supplied by the separate trusted directory/scope composition step.
		a := monitor.MonthlyPreparationAccount{UserID: user.UserID, GroupID: owner.GroupID,
			Months: []string{req.Month}, CounterBasis: "net_consumption", BasisSource: origin,
			Baseline: monitor.MonthlyPreparationBalance{UserID: user.UserID, At: start.Unix(), BalanceQuota: user.OriginBalanceQuota, UsedQuota: user.OriginUsedQuota, Source: origin},
			Spans:    []monitor.MonthlyPreparationSpan{}, Movements: []monitor.MonthlyPreparationMovement{},
			MovementCoverage: []monitor.MonthlyPreparationCoverage{}, Snapshots: []monitor.MonthlyPreparationBalance{}}
		if h := automaticOriginDecision(audited, user.UserID); h != nil {
			// Logical pre-recognition ledger origin, NOT an observed wallet.
			// Raw signed arithmetic and raw current wallets remain unchanged.
			a.Baseline.BalanceQuota, a.Baseline.UsedQuota = 0, 0
			a.Baseline.Source = automaticOriginPolicySourceID + "#registered-ledger-origin:user:" + strconv.FormatInt(user.UserID, 10)
			a.BasisSource = a.Baseline.Source
		}
		if user.OriginRequiresResolution {
			unresolved = append(unresolved, user.UserID)
		}
		for j, month := range user.Months {
			file := audited.months[j]
			if len(file.sources) < 3 || month.Month != file.material.Funding.Month || month.SelectionSHA256 != file.sources[1].SHA256 {
				return zero, nil, nil, ErrGenerationEvidence
			}
			from, err := time.ParseInLocation("2006-01", month.Month, start.Location())
			if err != nil {
				return zero, nil, nil, ErrGenerationEvidence
			}
			to := from.AddDate(0, 1, 0).Unix()
			sourceID := file.sources[2].ID
			a.MovementCoverage = append(a.MovementCoverage, monitor.MonthlyPreparationCoverage{UserID: user.UserID, From: from.Unix(), To: to, Source: sourceID + "#reviewed-funding-month:" + month.Month})
			if month.Month == req.Month {
				continue
			} // Existing normalizers alone fill the target.
			a.Spans = append(a.Spans, monitor.MonthlyPreparationSpan{UserID: user.UserID, From: from.Unix(), To: to,
				ConsumeQuota: month.ConsumeQuota, RefundQuota: month.RefundQuota, Source: sourceID + "#reviewed-history-month:" + month.Month})
			for _, m := range month.Movements {
				a.Movements = append(a.Movements, monitor.MonthlyPreparationMovement{UserID: user.UserID, ID: m.LogID, At: m.At, DeltaQuota: m.DeltaQuota,
					Source: sourceID + "#funding:" + m.PageID + ":row:" + m.SourceDigest + ":id:" + strconv.FormatInt(m.LogID, 10)})
			}
		}
		input.Accounts = append(input.Accounts, a)
	}
	plan := GenerationPlan{Version: target.material.FundingVersion, Owner: req.Owner, Funding: target.material.Funding,
		AutomaticHistorySourceID: automaticHistorySourceID,
		Windows:                  target.material.FundingWindows, ObservationStoreSourceID: target.sources[2].ID, CapturedUsageSourceID: target.sources[1].ID,
		WalletObservations: &WalletObservationSelection{SourceID: target.sources[2].ID, IDs: target.material.WalletIDs}}
	if audited.policy != nil {
		plan.AutomaticHistoryPolicySourceID = automaticOriginPolicySourceID
	}
	for j, token := range target.material.Tokens {
		plan.TokenUsageSources = append(plan.TokenUsageSources, TokenUsageSourceSelection{UserID: token.UserID, SourceID: target.sources[j+3].ID})
	}
	raw, err := json.Marshal(input)
	if err != nil || len(raw) > monitor.MonthlyPreparationInputLimit {
		return zero, nil, nil, ErrGenerationCapacity
	}
	plan.PreparationSHA256 = generationHash(raw)
	plan, _, _, err = canonicalGeneration(plan, raw, time.Now())
	if err != nil {
		return zero, nil, nil, err
	}
	return plan, raw, unresolved, ctx.Err()
}

func verifyAutomaticHistoryInput(ctx context.Context, observed ObservedPreparation, audited auditedAutomaticHistory, req SourcePreparationRequest) error {
	var plan GenerationPlan
	if json.Unmarshal(observed.Plan, &plan) != nil {
		return ErrGenerationEvidence
	}
	_, _, input, err := canonicalGeneration(plan, observed.Preparation, time.Now())
	if err != nil {
		return err
	}
	// Rebuild the untouched historical portion, not an editable cached report.
	expectedPlan, template, _, err := projectAutomaticHistoryInput(ctx, audited, req)
	if err != nil {
		return err
	}
	var before monitor.MonthlyPreparationInput
	if json.Unmarshal(template, &before) != nil || len(input.Accounts) != len(before.Accounts) {
		return ErrGenerationEvidence
	}
	expectedPlan.PreparationSHA256 = generationHash(observed.Preparation)
	_, expectedPlanRaw, _, err := canonicalGeneration(expectedPlan, observed.Preparation, time.Now())
	if err != nil {
		return err
	}
	if string(expectedPlanRaw) != string(observed.Plan) {
		return ErrGenerationEvidence
	}
	wantSources, _ := json.Marshal(before.Sources)
	gotSources, _ := json.Marshal(input.Sources)
	if string(wantSources) != string(gotSources) {
		return ErrGenerationEvidence
	}
	report := audited.history.Report()
	start, err := time.ParseInLocation("2006-01", req.Month, time.FixedZone("Usage-UTC+8", 8*3600))
	if err != nil {
		return ErrGenerationEvidence
	}
	end := start.AddDate(0, 1, 0).Unix()
	for i, a := range input.Accounts {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Compare all pre/post-target fields as canonical JSON. Target spans,
		// movements, raw collection observations and token rows are independently
		// filled and audited by PrepareObservedInput, not copied from the report.
		kept := a
		kept.Spans = []monitor.MonthlyPreparationSpan{}
		kept.Movements = []monitor.MonthlyPreparationMovement{}
		targetMonth := report.Accounts[i].Months[audited.target]
		targetFunding := make(map[int64]monthlysource.FundingObservedMovement, len(targetMonth.Movements))
		for _, m := range targetMonth.Movements {
			targetFunding[m.LogID] = m
		}
		var consume, refund int64
		for _, s := range a.Spans {
			if s.From >= start.Unix() && s.To <= end {
				var ok bool
				consume, ok = usagePreparationAdd(consume, s.ConsumeQuota)
				if !ok {
					return ErrGenerationEvidence
				}
				refund, ok = usagePreparationAdd(refund, s.RefundQuota)
				if !ok {
					return ErrGenerationEvidence
				}
			} else {
				kept.Spans = append(kept.Spans, s)
			}
		}
		for _, m := range a.Movements {
			if m.At < start.Unix() || m.At >= end {
				kept.Movements = append(kept.Movements, m)
			} else {
				original, exists := targetFunding[m.ID]
				source := audited.months[audited.target].sources[2].ID + "#funding:" + original.PageID + ":row:" + original.SourceDigest + ":id:" + strconv.FormatInt(original.LogID, 10)
				if !exists || m.At != original.At || m.DeltaQuota != original.DeltaQuota || m.Source != source {
					return ErrGenerationEvidence
				}
				delete(targetFunding, m.ID)
			}
		}
		if len(targetFunding) != 0 {
			return ErrGenerationEvidence
		}
		// Only the selected original collection observations belong here, not
		// an extra arithmetic snapshot labeled as an observed historical cutoff.
		prefix := audited.months[audited.target].sources[2].ID + "#wallet:"
		if len(a.Snapshots) != len(audited.months[audited.target].material.WalletIDs) {
			return ErrGenerationEvidence
		}
		for _, snapshot := range a.Snapshots {
			if !strings.HasPrefix(snapshot.Source, prefix) || snapshot.TimeBasis != "collection_observation" || snapshot.CutoffSource != "" {
				return ErrGenerationEvidence
			}
		}
		kept.Snapshots = []monitor.MonthlyPreparationBalance{}
		kept.TokenMonths = nil
		kept.MonthStatistics = nil
		want, _ := json.Marshal(before.Accounts[i])
		got, _ := json.Marshal(kept)
		if string(want) != string(got) || consume != report.Accounts[i].Months[audited.target].ConsumeQuota || refund != report.Accounts[i].Months[audited.target].RefundQuota {
			return ErrGenerationEvidence
		}
	}
	return ctx.Err()
}
