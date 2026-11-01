package monthlydraft

import (
	"bytes"
	"context"
	"math/big"
	"strconv"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

const automaticOriginPolicySourceID = "automatic-history-policy"

// Standing business treatment previously accepted by the owner. This document
// carries no user amounts/approval flags. Exact native policy bytes and fresh
// originals are required; neither its file hash nor JSON alone is approval.
func automaticOriginPolicyBytes() []byte {
	return []byte(`{"version":1,"mode":"standing_owner_policy_not_source_snapshot_or_publication","accepted_on":"2026-10-26","policy":"historical-wallet-recognition-v1","direction":"customer_beneficial_equal_opposite_cash_and_counter","absolute_quota_limit":2500000,"ratio_denominator":10000,"recognition":"first_closed_eligible_month_not_proven_incident_month","preserve_original_logs":true}`)
}

type automaticOriginPolicyProof struct {
	source    monitor.MonthlyPreparationSource
	decisions map[int64]*monitor.MonthlyHistoricalBalanceRecognition
}

func (g *Generation) auditAutomaticHistoryForPlan(ctx context.Context, ref monthlysource.MonthlyObservationFile, req SourcePreparationRequest, policy *monthlysource.MonthlyObservationFile) (auditedAutomaticHistory, error) {
	if policy == nil {
		return g.auditAutomaticHistorySources(ctx, ref, req)
	}
	var empty auditedAutomaticHistory
	body, err := g.readPrivatePreparationFile(ctx, *policy, 16<<10)
	if err != nil {
		return empty, err
	}
	if !bytes.Equal(body, automaticOriginPolicyBytes()) {
		return empty, ErrGenerationEvidence
	}
	audited, err := g.auditAutomaticHistoryResidualSources(ctx, ref, req)
	if err != nil {
		return empty, err
	}
	r, err := classifyAutomaticOrigins(ctx, audited, req)
	if err != nil {
		return empty, err
	}
	p := &automaticOriginPolicyProof{source: monitor.MonthlyPreparationSource{ID: automaticOriginPolicySourceID, Path: policy.Path, SHA256: policy.SHA256}, decisions: map[int64]*monitor.MonthlyHistoricalBalanceRecognition{}}
	zone := time.FixedZone("Usage-UTC+8", 8*3600)
	accepted := time.Date(2026, 10, 26, 0, 0, 0, 0, zone).Unix()
	for _, a := range r.accounts {
		if a.Status == "no_difference" {
			continue
		}
		if a.Status != "bounded_policy_candidate_not_approval" || len(a.EligibleRecognitionMonths) == 0 {
			return empty, ErrGenerationEvidence
		}
		month := a.EligibleRecognitionMonths[0]
		at, err := time.ParseInLocation("2006-01", month, zone)
		if err != nil {
			return empty, ErrGenerationEvidence
		}
		decisionAt := at.AddDate(0, 1, 0).Unix()
		if decisionAt < accepted {
			decisionAt = accepted
		}
		if decisionAt > time.Now().Unix() {
			return empty, ErrGenerationEvidence
		}
		p.decisions[a.UserID] = &monitor.MonthlyHistoricalBalanceRecognition{Version: 1, UserID: a.UserID, GroupID: req.Owner.GroupID, RecognitionMonth: month, Quota: a.OriginBalanceQuota, DecisionAt: decisionAt,
			DecisionSource: automaticOriginPolicySourceID + "#accepted-standing-treatment:user:" + strconv.FormatInt(a.UserID, 10), DecisionSHA256: policy.SHA256}
	}
	if len(p.decisions) == 0 {
		return empty, ErrGenerationInput
	}
	for _, s := range audited.sources {
		if s.ID == p.source.ID || s.Path == p.source.Path {
			return empty, ErrGenerationIntegrity
		}
	}
	audited.policy = p
	audited.sources = append(audited.sources, p.source)
	if err := g.verifyFiles(ctx, audited.sources); err != nil {
		return empty, err
	}
	return audited, ctx.Err()
}

func selectedAutomaticOriginPolicy(p GenerationPlan, input monitor.MonthlyPreparationInput) (*monthlysource.MonthlyObservationFile, error) {
	if p.AutomaticHistoryPolicySourceID == "" {
		return nil, nil
	}
	if p.AutomaticHistoryPolicySourceID != automaticOriginPolicySourceID || p.AutomaticHistorySourceID != automaticHistorySourceID {
		return nil, ErrGenerationInput
	}
	for _, s := range input.Sources {
		if s.ID == p.AutomaticHistoryPolicySourceID {
			return &monthlysource.MonthlyObservationFile{Path: s.Path, SHA256: s.SHA256}, nil
		}
	}
	return nil, ErrGenerationIntegrity
}

// Small helper for exact plan/source replay, never a deserialized capability.
func automaticOriginDecision(a auditedAutomaticHistory, user int64) *monitor.MonthlyHistoricalBalanceRecognition {
	if a.policy == nil {
		return nil
	}
	h := a.policy.decisions[user]
	if h == nil {
		return nil
	}
	copy := *h
	return &copy
}

func (automaticOriginPolicyProof) MarshalJSON() ([]byte, error) { return nil, ErrGenerationEvidence }
func (p *automaticOriginPolicyProof) UnmarshalJSON([]byte) error {
	if p != nil {
		*p = automaticOriginPolicyProof{}
	}
	return ErrGenerationEvidence
}

func addOriginQuota(target *int64, amount int64) error {
	n := new(big.Int).Add(big.NewInt(*target), big.NewInt(amount))
	if amount < 0 || !n.IsInt64() {
		return ErrGenerationCapacity
	}
	*target = n.Int64()
	return nil
}

func automaticOriginTargetFiles(a auditedAutomaticHistory, req SourcePreparationRequest) (CapturedSourceFiles, error) {
	if a.target < 0 || a.target >= len(a.months) {
		return CapturedSourceFiles{}, ErrGenerationEvidence
	}
	m := a.months[a.target]
	if len(m.sources) < 3 || m.material.Funding.Month != req.Month {
		return CapturedSourceFiles{}, ErrGenerationEvidence
	}
	return CapturedSourceFiles{Mode: capturedSourceFilesMode, Material: monthlysource.MonthlyObservationFile{Path: m.sources[0].Path, SHA256: m.sources[0].SHA256}, Selection: monthlysource.MonthlyObservationFile{Path: m.sources[1].Path, SHA256: m.sources[1].SHA256}}, nil
}
