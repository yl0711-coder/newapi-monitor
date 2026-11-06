package monthlydraft

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

var (
	ErrGenerationInput        = errors.New("invalid local generation plan")
	ErrGenerationChanged      = errors.New("generation authorization or source version changed")
	ErrGenerationEvidence     = errors.New("generation evidence incomplete or reconciliation failed")
	ErrGenerationIntegrity    = errors.New("generation retained evidence integrity failure")
	ErrGenerationCapacity     = errors.New("generation receiver capacity exceeded")
	ErrGenerationBusy         = errors.New("local generation already running")
	ErrGenerationRulesChanged = errors.New("generation calculation rules changed; prepare a new version")
)

const generationVersion = "local-monthly-draft-v7-token-source-audit" // Bump if calculation/format semantics change.
const generationPlanLimit = 2 << 20
const generationArtifactLimit = 8 << 20
const generationReceiverLimit = 64 << 20
const generationRecordLimit = 32

// GenerationPlan contains references, not editable "verified/complete" flags.
// Retained funding pages and explicitly selected usage captures are re-audited;
// normalized balance/refund/wallet semantics still need independent attestation
// before any customer publication. Legacy funding-only plans remain supported.
type GenerationPlan struct {
	// Version 1 references legacy <=1h funding scans; explicit version 2
	// references exact UTC+8-day scans. Neither changes monetary rules.
	Version            int                                       `json:"version"`
	CalculationVersion string                                    `json:"calculation_version,omitempty"`
	Owner              monthlytasks.Owner                        `json:"owner"`
	Funding            monthlysource.FundingMonthRequest         `json:"funding"`
	PreparationSHA256  string                                    `json:"preparation_sha256"`
	Windows            []monthlysource.FundingMonthWindowVersion `json:"windows"`
	// Optional immutable observation DB selected from input.Sources. Empty keeps
	// the legacy deployment-wide source; new independent captures must pin one
	// sealed DB per input rather than append to a shared hash-pinned file.
	ObservationStoreSourceID string `json:"observation_store_source_id,omitempty"`
	// Optional for compatibility with retained funding-only drafts. When set,
	// the exact capture IDs are part of the immutable plan/task identity and
	// usage is re-audited before draft creation, reuse and automatic handoff.
	UsageWindows []monthlysource.UsageWindowVersion `json:"usage_windows,omitempty"`
	// Explicit alternative: immutable complete captured index in input.Sources.
	// Partitions are derived from these SAME bytes, not editable amount caches.
	CapturedUsageSourceID string `json:"captured_usage_source_id,omitempty"`
	// Raw wallet observations remain collection observations, never historical
	// cutoffs or actual-refund proof. Exact IDs bind their additional audit.
	WalletObservations *WalletObservationSelection `json:"wallet_observations,omitempty"`
	// Optional exact metadata selection. Its ID as well as the source digest
	// belongs to task identity; changing startup configuration cannot silently
	// select another historical scope for an already queued task.
	AutomaticScopeSourceID string `json:"automatic_scope_source_id,omitempty"`
	// Explicit opt-in to original-source history audits. Omitted legacy plans
	// keep their exact bytes; a new source binding cannot be silently removed.
	AutomaticHistorySourceID       string                      `json:"automatic_history_source_id,omitempty"`
	AutomaticHistoryPolicySourceID string                      `json:"automatic_history_policy_source_id,omitempty"`
	TokenUsageSources              []TokenUsageSourceSelection `json:"token_usage_sources,omitempty"`
}

// GenerationArtifact is PRIVATE. It includes source references and internal
// reconciliation issues; never return it from a customer HTTP endpoint.
type GenerationArtifact struct {
	Version        int                              `json:"version"`
	Mode           string                           `json:"mode"`
	PlanSHA256     string                           `json:"plan_sha256"`
	FundingSHA256  string                           `json:"funding_review_sha256"`
	UsageSHA256    string                           `json:"usage_review_sha256,omitempty"`
	WalletSHA256   string                           `json:"wallet_review_sha256,omitempty"`
	TokenSHA256    string                           `json:"token_review_sha256,omitempty"`
	Reconciliation monitor.MonthlyPreparationReport `json:"reconciliation"`
	// Optional binding to the existing independently validated historical
	// reconstruction candidate. The raw reconciliation report is NOT rewritten
	// into an original month-boundary observation or an editable balance.
	ScopeReview *GenerationScopeReview `json:"selected_scope_review,omitempty"`
}

func generationHash(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func generationHex(s string, n int) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == n && hex.EncodeToString(b) == s
}

func generationMemberHash(ids []int64) string {
	h := sha256.New()
	for _, id := range ids {
		_, _ = h.Write([]byte(strconv.FormatInt(id, 10) + ","))
	}
	return hex.EncodeToString(h.Sum(nil)[:16]) // Existing Usage convention.
}

func canonicalGeneration(p GenerationPlan, raw []byte, now time.Time) (GenerationPlan, []byte, monitor.MonthlyPreparationInput, error) {
	input, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil {
		return p, nil, input, err
	}
	if err := canonicalAutomaticHistoryBinding(p, input); err != nil {
		return p, nil, input, err
	}
	if p.ObservationStoreSourceID != "" {
		found := false
		for _, source := range input.Sources {
			found = found || source.ID == p.ObservationStoreSourceID
		}
		if !validScopeSourceID(p.ObservationStoreSourceID) || !found ||
			(p.WalletObservations != nil && p.WalletObservations.SourceID != p.ObservationStoreSourceID) {
			return p, nil, input, ErrGenerationInput
		}
	}
	if p.AutomaticScopeSourceID != "" {
		found := false
		for _, source := range input.Sources {
			found = found || source.ID == p.AutomaticScopeSourceID
		}
		if !validScopeSourceID(p.AutomaticScopeSourceID) || !found {
			return p, nil, input, ErrGenerationInput
		}
	}
	// Bind the calculation version into both the retained plan ID and task key.
	// A rules-hash-only bump would still reuse an old artifact under the same
	// plan ID. Old stored bytes fail canonical equality; no cached money is rewritten.
	if p.CalculationVersion == "" {
		p.CalculationVersion = generationVersion
	} else if p.CalculationVersion != generationVersion {
		return p, nil, input, ErrGenerationInput
	}
	zone := time.FixedZone("Usage-UTC+8", 8*3600)
	month, err := time.ParseInLocation("2006-01", p.Funding.Month, zone)
	if err != nil || month.Format("2006-01") != p.Funding.Month || month.Year() < 2020 || month.AddDate(0, 1, 0).After(now) ||
		(p.Version != 1 && p.Version != 2) || p.Owner.GroupID <= 0 || p.Owner.AuthVersion < 0 || !generationHex(p.Owner.MemberManifest, 32) ||
		p.PreparationSHA256 != generationHash(raw) || !generationHex(p.Funding.SourceIdentity, 32) ||
		p.Funding.Rules.Producer != monthlysource.FundingProducerRC4RC26 || p.Funding.Rules.QuotaPerUnit != 500000 ||
		len(p.Funding.UserIDs) == 0 || len(p.Funding.UserIDs) > 200 || len(p.Windows) == 0 || len(p.Windows) > 4096 ||
		len(p.Funding.UserIDs)*len(p.Windows) > 150000 || len(input.Accounts) != len(p.Funding.UserIDs) {
		return p, nil, input, ErrGenerationInput
	}
	p.Funding.UserIDs = append([]int64(nil), p.Funding.UserIDs...)
	sort.Slice(p.Funding.UserIDs, func(i, j int) bool { return p.Funding.UserIDs[i] < p.Funding.UserIDs[j] })
	ids := map[int64]bool{}
	for _, id := range p.Funding.UserIDs {
		if id <= 0 || ids[id] {
			return p, nil, input, ErrGenerationInput
		}
		ids[id] = true
	}
	if p.Owner.MemberFingerprint != generationMemberHash(p.Funding.UserIDs) {
		return p, nil, input, ErrGenerationInput
	}
	for _, a := range input.Accounts {
		if !ids[a.UserID] || a.GroupID != p.Owner.GroupID || len(a.Months) != 1 || a.Months[0] != p.Funding.Month {
			return p, nil, input, ErrGenerationInput
		}
		delete(ids, a.UserID)
	}
	p.Windows = append([]monthlysource.FundingMonthWindowVersion(nil), p.Windows...)
	sort.Slice(p.Windows, func(i, j int) bool { return p.Windows[i].From < p.Windows[j].From })
	cursor, end := month.Unix(), month.AddDate(0, 1, 0).Unix()
	scans := map[string]bool{}
	for _, w := range p.Windows {
		if monthlysource.ValidateFundingWindow(p.Version, w.From, w.To) != nil || w.From != cursor || w.To <= w.From || w.To > end ||
			!generationHex(w.OlderScanID, 16) || !generationHex(w.NewerScanID, 16) ||
			!generationHex(w.OlderHeadID, 32) || !generationHex(w.NewerHeadID, 32) ||
			w.OlderScanID == w.NewerScanID || scans[w.OlderScanID] || scans[w.NewerScanID] {
			return p, nil, input, ErrGenerationInput
		}
		scans[w.OlderScanID], scans[w.NewerScanID] = true, true
		cursor = w.To
	}
	if cursor != end {
		return p, nil, input, ErrGenerationInput
	}
	if len(p.UsageWindows) > 4096 {
		return p, nil, input, ErrGenerationCapacity
	}
	if p.CapturedUsageSourceID != "" {
		found := false
		for _, s := range input.Sources {
			found = found || s.ID == p.CapturedUsageSourceID
		}
		if !found || !validScopeSourceID(p.CapturedUsageSourceID) || len(p.UsageWindows) != 0 || p.ObservationStoreSourceID == "" || p.WalletObservations == nil || p.WalletObservations.ReuseSourceID != "" || len(p.WalletObservations.IDs) != 2 || len(p.TokenUsageSources) != len(p.Funding.UserIDs) {
			return p, nil, input, ErrGenerationInput
		}
	}
	p.UsageWindows = append([]monthlysource.UsageWindowVersion(nil), p.UsageWindows...)
	sort.Slice(p.UsageWindows, func(i, j int) bool { return p.UsageWindows[i].OlderID < p.UsageWindows[j].OlderID })
	usageIDs := make(map[string]bool, len(p.UsageWindows)*2)
	for _, w := range p.UsageWindows {
		for _, id := range []string{w.OlderID, w.NewerID} {
			if !generationHex(id, 32) || usageIDs[id] {
				return p, nil, input, ErrGenerationInput
			}
			usageIDs[id] = true
		}
	}
	if p.WalletObservations, err = canonicalWalletSelection(p.WalletObservations, input.Sources); err != nil {
		return p, nil, input, err
	}
	if p.TokenUsageSources, err = canonicalTokenSelections(p.TokenUsageSources, input, p.Funding.UserIDs); err != nil {
		return p, nil, input, err
	}
	encoded, err := json.Marshal(p)
	if err != nil || len(encoded) > generationPlanLimit {
		return p, nil, input, ErrGenerationInput
	}
	return p, encoded, input, nil
}

func generationRequest(p GenerationPlan, planRaw []byte) monthlytasks.Request {
	return monthlytasks.Request{Owner: p.Owner, Month: p.Funding.Month, SourceIdentity: p.Funding.SourceIdentity,
		InputSHA256: generationHash(planRaw), RulesSHA256: generationRulesHash(p.Funding.Rules)}
}

func generationRulesHash(rules monthlysource.FundingRules) string {
	raw, _ := json.Marshal(rules)
	return generationHash(append([]byte(generationVersion+":"), raw...))
}

// Match exactly the one ruleset accepted by canonicalGeneration. This check
// uses small task metadata only; polling must not load/reconcile source files.
func currentGenerationRules(request monthlytasks.Request) bool {
	return request.RulesSHA256 == generationRulesHash(monthlysource.FundingRules{
		Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000,
	})
}

func generationAllAccounts(p GenerationPlan, r monitor.MonthlyPreparationReport) bool {
	if r.Mode != "local_reconciliation_draft_not_publication" || r.InputSHA256 != p.PreparationSHA256 || len(r.Accounts) != len(p.Funding.UserIDs) {
		return false
	}
	ids := map[int64]bool{}
	for _, id := range p.Funding.UserIDs {
		ids[id] = true
	}
	for _, a := range r.Accounts {
		if !ids[a.UserID] || a.GroupID != p.Owner.GroupID || a.Error != "" || len(a.Months) != 1 ||
			a.Months[0].Month != p.Funding.Month || a.Months[0].Status != "reconciled_draft" || a.Months[0].Amounts == nil {
			return false
		}
		delete(ids, a.UserID)
	}
	return true
}
