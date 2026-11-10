package monitor

import (
	"encoding/json"
	"errors"
	"github.com/yl0711-coder/newapi-monitor/internal/metering"
	"math/big"
	"strings"
)

const tokenDetectionVersion = "token-detection-v1"

// A production-sized retained queue needs more than one short ten-second
// turn to amortise the per-request reference and capacity checks.  The caller
// still supplies a shorter context in tests and on shutdown, so this remains a
// hard upper bound rather than an unbounded worker.
const tokenDetectionRoundSeconds = 30
const tokenDetectionMaxRows = 100000
const tokenDetectionMaxBytes = 256 << 20
const tokenDetectionMaxReferences = 5000

// Live snapshots and compact historical anchors have separate budgets.
const tokenDetectionMaxReferenceRecords = 100000
const tokenDetectionRetention int64 = 90 * 86400

// A retained detection commit can include report-index triggers and frozen
// payload bookkeeping. Keep its maximum writer hold below the collection
// lane's local budget; the worker still processes the configured row batch
// through several fair transactions.
const tokenDetectionMaxCommitRows = 32

// Enabling requires a versioned policy. Empty thresholds leave rules unset;
// no experimental case threshold becomes a global production default.
type tokenDetectionPolicy struct {
	Version             string               `json:"version"`
	MinSamples          int                  `json:"min_samples"`
	MinChannels         int                  `json:"min_channels"`
	LookbackSeconds     int64                `json:"lookback_seconds"`
	SizeTolerance       int64                `json:"size_tolerance_bytes"`
	SizeRelative        string               `json:"size_relative_tolerance"`
	InputRatio          string               `json:"input_ratio"`
	MinInput            int64                `json:"min_input"`
	CacheShareDrop      string               `json:"cache_share_drop"`
	CacheIntensityRatio string               `json:"cache_intensity_ratio"`
	CacheMinSustained   int                  `json:"cache_min_sustained"`
	PricingAdapter      string               `json:"pricing_adapter"`
	SourceContract      *tokenSourceContract `json:"source_contract,omitempty"`
}

func parseTokenDetectionPolicy(raw string) (tokenDetectionPolicy, error) {
	var p tokenDetectionPolicy
	if _, err := metering.Object([]byte(raw), 4096, 3); err != nil {
		return p, errors.New("invalid token detection policy")
	}
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return p, errors.New("invalid token detection policy")
	}
	if !tokenMeteringName.MatchString(p.Version) || p.MinSamples < 3 || p.MinSamples > 64 || p.MinChannels < 2 || p.MinChannels > p.MinSamples || p.LookbackSeconds < 60 || p.LookbackSeconds > 3600 || p.SizeTolerance < 0 || p.SizeTolerance > 64 || p.MinInput < 0 || p.CacheMinSustained < 2 || p.CacheMinSustained > 50 {
		return p, errors.New("token detection policy bounds")
	}
	if !tokenPolicyRatio(p.SizeRelative, "0", "1", false) || !tokenPolicyRatio(p.InputRatio, "1", "1000", true) || !tokenPolicyRatio(p.CacheShareDrop, "0", "1", true) || !tokenPolicyRatio(p.CacheIntensityRatio, "1", "1000", true) {
		return p, errors.New("token detection threshold bounds")
	}
	if (p.CacheShareDrop == "") != (p.CacheIntensityRatio == "") {
		return p, errors.New("cache thresholds must be specified together")
	}
	if p.PricingAdapter != "" && p.PricingAdapter != "newapi-text-v1" {
		return p, errors.New("unsupported token pricing adapter")
	}
	if !tokenSourceContractValid(p.SourceContract) {
		return p, errors.New("invalid token source contract")
	}
	return p, nil
}
func tokenPolicyRatio(v, low, high string, optional bool) bool {
	if v == "" {
		return optional
	}
	if len(v) > 32 {
		return false
	}
	b, _ := json.Marshal(v)
	f := metering.Number(b, false, true)
	if f.State != "valid" {
		return false
	}
	n, _ := new(big.Rat).SetString(f.Value)
	a, _ := new(big.Rat).SetString(low)
	z, _ := new(big.Rat).SetString(high)
	return n.Cmp(a) >= 0 && n.Cmp(z) <= 0 && (low != "1" || n.Cmp(a) > 0)
}
func validateTokenDetectionSettings(s Settings) error {
	if !s.TokenDetectionEnabled {
		return nil
	}
	if !tokenMeteringName.MatchString(s.TokenFactsSourceEpoch) {
		return errors.New("token detection requires source epoch")
	}
	if err := validateTokenProcessingResources(s); err != nil {
		return err
	}
	_, err := parseTokenDetectionPolicy(s.TokenDetectionPolicy)
	return err
}
func validateTokenProcessingResources(s Settings) error {
	if s.TokenProcessingPollSeconds != 0 && (s.TokenProcessingPollSeconds < 10 || s.TokenProcessingPollSeconds > 300) ||
		s.TokenDetectionWorkers < 0 || s.TokenDetectionWorkers > 2 ||
		s.TokenDetectionWorkMilliseconds != 0 && (s.TokenDetectionWorkMilliseconds < 50 || s.TokenDetectionWorkMilliseconds > 750) ||
		s.TokenDetectionCommitRows < 0 || s.TokenDetectionCommitRows > tokenDetectionMaxCommitRows ||
		s.TokenProcessingBatchRows != 0 && (s.TokenProcessingBatchRows < 50 || s.TokenProcessingBatchRows > 500) ||
		s.TokenDetectionMaxRows < 0 || s.TokenDetectionMaxRows > 50_000_000 ||
		s.TokenDetectionMaxBytes < 0 || s.TokenDetectionMaxBytes > 128<<30 ||
		s.TokenDetectionMaxReferences < 0 || s.TokenDetectionMaxReferences > 5_000_000 ||
		s.TokenDetectionMaxReferenceRecords < 0 || s.TokenDetectionMaxReferenceRecords > 50_000_000 ||
		s.TokenStoreMinFreeBytes < 0 || s.TokenStoreMinFreeBytes != 0 && s.TokenStoreMinFreeBytes < 2<<30 ||
		s.TokenStoreMaxUsedPercent != 0 && (s.TokenStoreMaxUsedPercent < 50 || s.TokenStoreMaxUsedPercent > 99) ||
		s.TokenDetectionRetentionDays != 0 && (s.TokenDetectionRetentionDays < 4 || s.TokenDetectionRetentionDays > 90) {
		return errors.New("token processing resource settings invalid")
	}
	if s.TokenDetectionRetentionBasis != "" && s.TokenDetectionRetentionBasis != "first_evaluated" && s.TokenDetectionRetentionBasis != "last_evaluated" {
		return errors.New("token retention basis invalid")
	}
	refs, records := s.tokenDetectionReferenceLimits()
	if records < refs {
		return errors.New("token reference records must cover active reference capacity")
	}
	return nil
}
func (s Settings) tokenDetectionWorkers() int { return max(1, min(2, s.TokenDetectionWorkers)) }
func (s Settings) tokenDetectionCommitLimit() int {
	if s.TokenDetectionCommitRows == 0 {
		return tokenDetectionMaxCommitRows
	}
	return max(1, min(tokenDetectionMaxCommitRows, s.TokenDetectionCommitRows))
}
func (s Settings) tokenDetectionWorkMS() int {
	if s.TokenDetectionWorkMilliseconds == 0 {
		return 250
	}
	return max(50, min(750, s.TokenDetectionWorkMilliseconds))
}
func (s Settings) tokenDetectionReferenceLimits() (int64, int64) {
	refs, records := s.TokenDetectionMaxReferences, s.TokenDetectionMaxReferenceRecords
	if refs == 0 {
		refs = tokenDetectionMaxReferences
	}
	if records == 0 {
		records = tokenDetectionMaxReferenceRecords
	}
	return refs, records
}
func (s Settings) tokenProcessingPollSeconds() int {
	if s.TokenProcessingPollSeconds < 10 || s.TokenProcessingPollSeconds > 300 {
		return 60
	}
	return s.TokenProcessingPollSeconds
}
func (s Settings) tokenProcessingBatchRows() int {
	if s.TokenProcessingBatchRows < 50 || s.TokenProcessingBatchRows > 500 {
		return 50
	}
	return s.TokenProcessingBatchRows
}
func (s Settings) tokenDetectionLimits() (int64, int64) {
	rows, bytes := s.TokenDetectionMaxRows, s.TokenDetectionMaxBytes
	if rows == 0 {
		rows = tokenDetectionMaxRows
	}
	if bytes == 0 {
		bytes = tokenDetectionMaxBytes
	}
	return rows, bytes
}
func (s Settings) tokenDetectionRetentionSeconds() int64 {
	if s.TokenDetectionRetentionDays == 0 {
		return tokenDetectionRetention
	}
	return int64(max(4, s.TokenDetectionRetentionDays)) * 86400
}
func (s Settings) tokenDetectionExpiresAt(first, now int64) int64 {
	if s.TokenDetectionRetentionBasis == "first_evaluated" && first > 0 {
		return first + s.tokenDetectionRetentionSeconds()
	}
	return now + s.tokenDetectionRetentionSeconds()
}
func tokenDetectionJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

// The policy digest also invalidates heads produced by older evidence semantics.
func tokenDetectionPolicyHash(p tokenDetectionPolicy) string {
	return tokenDetectionHash([]any{"content-evidence-v5-independent-rules", tokenMeteringParserVersion, p})
}
