package monitor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"gorm.io/gorm"
	"math/big"
	"sort"
	"strings"

	"github.com/yl0711-coder/newapi-monitor/internal/metering"
)

type tokenDetectionInput struct {
	Fact  TokenMeteringFact   `json:"fact"`
	Usage tokenMeteringParsed `json:"usage"`
	Entry json.RawMessage     `json:"entry"`
	Refs  []TokenMeteringRef  `json:"hmac_refs"`
}
type tokenReferenceSample struct {
	EvidenceHash string              `json:"evidence_hash,omitempty"`
	Epoch        string              `json:"epoch"`
	LogID        int64               `json:"log_id"`
	CreatedAt    int64               `json:"created_at"`
	ChannelID    int64               `json:"channel_id"`
	Length       int64               `json:"request_length"`
	FactHash     string              `json:"fact_hash"`
	Input        int64               `json:"input"`
	ReadShare    string              `json:"read_share"`
	Intensity    string              `json:"intensity"`
	Usage        tokenMeteringParsed `json:"usage"`
}
type tokenReference struct {
	SampleCount       int                    `json:"sample_count"`
	ID                string                 `json:"id"`
	Bucket            string                 `json:"bucket"`
	ParentID          string                 `json:"parent_id"`
	Status            string                 `json:"status"`
	Reason            string                 `json:"reason"`
	From              int64                  `json:"from"`
	Through           int64                  `json:"through"`
	ExactSize         bool                   `json:"exact_size"`
	Truncated         bool                   `json:"truncated"`
	Excluded          int                    `json:"excluded"`
	ExclusionReasons  map[string]int         `json:"exclusion_reasons,omitempty"`
	MissingComparable []string               `json:"missing_comparable_fields,omitempty"`
	Search            []tokenReferenceSearch `json:"search,omitempty"`
	DriftExcluded     int                    `json:"drift_excluded"`
	Upper             *int64                 `json:"upper"`
	Samples           []tokenReferenceSample `json:"samples"`
	Gaps              []string               `json:"gaps"`
}

type tokenReferenceSearch struct {
	Tolerance int64 `json:"tolerance_bytes"`
	Read      int   `json:"read"`
	Eligible  int   `json:"eligible"`
	Channels  int   `json:"channels"`
}

func tokenDetectionHash(v any) string {
	h := sha256.Sum256([]byte(tokenDetectionJSON(v)))
	return hex.EncodeToString(h[:])
}
func tokenDetectionInputOf(f TokenMeteringFact) (tokenDetectionInput, error) {
	in := tokenDetectionInput{Fact: f, Refs: []TokenMeteringRef{}, Entry: json.RawMessage("[]")}
	if len(f.SourceFacts) > 64<<10 || json.Unmarshal([]byte(f.SourceFacts), &in.Usage) != nil {
		return in, errors.New("local_fact_invalid")
	}
	if tokenMeteringDropUnverifiedContract(&in.Usage) {
		// Reject the old guessed contract at every consumption boundary, for
		// both the current request and reference candidates. Do not wait for
		// the background rewrite to reach this retained fact.
		in.Usage.Version, in.Usage.Adapter = tokenMeteringParserVersion, "unknown"
		in.Usage.PricingSupport = "operands_only_no_calculation"
		normalizeTokenMetering(Settings{}, &in.Usage)
	}
	// The fact column is a source/evidence snapshot in its own right. Use it
	// when the bounded usage JSON predates path capture, while preserving the
	// original field state for all other values.
	if in.Fact.RequestPath != "" && !tokenMeteringFieldUsable(in.Usage.Fields["request_path"]) {
		in.Usage.Fields["request_path"] = metering.Field{State: "derived", Kind: "fact_snapshot", Value: in.Fact.RequestPath}
	}
	if f.EvidenceJSON != "" {
		if len(f.EvidenceJSON) > 128<<10 || !json.Valid([]byte(f.EvidenceJSON)) {
			return in, errors.New("local_evidence_invalid")
		}
		in.Entry = json.RawMessage(f.EvidenceJSON)
	}
	// Scheduling counters and last-seen timestamps are not new evidence.
	in.Fact.SourceFacts = ""
	in.Fact.EvidenceJSON = ""
	in.Fact.LastSeenAt = 0
	in.Fact.LinkCheckedAt = 0
	in.Fact.LinkNextAt = 0
	in.Fact.LinkAttempts = 0
	in.Fact.StoredBytes = 0
	return in, nil
}

var tokenReferenceScopeFields = []string{"model_price", "model_ratio", "effective_model_ratio", "completion_ratio", "effective_group_ratio", "group_ratio", "user_group_ratio", "cache_ratio", "cache_creation_ratio", "cache_creation_ratio_5m", "cache_creation_ratio_1h", "cache_creation_ratio_12h", "billing_mode", "billing_source", "currency", "quota_per_unit", "rounding_mode", "expr_b64", "request_rules", "upstream_model_name", "is_model_mapped", "request_path", "is_stream", "request_conversion", "is_system_prompt_overwritten"}

func tokenReferenceScopeOperands(u tokenMeteringParsed) map[string]any {
	price := map[string]any{}
	for _, k := range tokenReferenceScopeFields {
		f := tokenMeteringOperand(u, k)
		// Numeric string/number encodings and a source-derived operand with the
		// same verified value do not make otherwise comparable requests differ.
		state := f.State
		if tokenMeteringFieldUsable(f) {
			state = "usable"
		}
		price[k] = []string{state, f.Value, f.Raw}
		if state == "usable" {
			price[k] = []string{state, f.Value}
		}
	}
	return price
}

func tokenReferenceScopeDifferences(a, b tokenDetectionInput) []string {
	left, right := tokenReferenceScopeOperands(a.Usage), tokenReferenceScopeOperands(b.Usage)
	var different []string
	for _, k := range tokenReferenceScopeFields {
		if tokenDetectionJSON(left[k]) != tokenDetectionJSON(right[k]) {
			different = append(different, k)
		}
	}
	for k, values := range map[string][2]string{"usage_semantic": {a.Usage.Semantic, b.Usage.Semantic}, "adapter": {a.Usage.Adapter, b.Usage.Adapter}, "parser": {a.Usage.Version, b.Usage.Version}, "fact_path": {a.Fact.RequestPath, b.Fact.RequestPath}} {
		if values[0] != values[1] {
			different = append(different, k)
		}
	}
	ae, _ := tokenComparisonEntry(a)
	be, _ := tokenComparisonEntry(b)
	if ae.Route != be.Route {
		different = append(different, "entry_route")
	}
	if ae.Status != be.Status {
		different = append(different, "entry_status")
	}
	if len(different) == 0 {
		different = append(different, "identity_scope")
	}
	sort.Strings(different)
	return different
}

func tokenReferenceBucket(in tokenDetectionInput, p tokenDetectionPolicy) string {
	f, u := in.Fact, in.Usage
	price := tokenReferenceScopeOperands(u)
	entry, _ := tokenComparisonEntry(in)
	path := f.RequestPath
	if path == "" {
		path = u.Fields["request_path"].Value
	}
	return tokenDetectionHash([]any{"input-comparison-v4", f.SourceEpoch, f.UserID, f.Model, f.UserGroup, u.Semantic, u.Adapter, u.Version, price, path, entry.Route, entry.Status, p})
}

func tokenComparisonEntry(in tokenDetectionInput) (tokenMeteringCandidate, bool) {
	var entries []tokenMeteringCandidate
	if json.Unmarshal(in.Entry, &entries) != nil || len(entries) != 1 {
		return tokenMeteringCandidate{}, false
	}
	e := entries[0]
	return e, e.Status == 200 && e.Route != "" && e.LengthVersion == "request-length-v1"
}

func tokenComparable(in tokenDetectionInput) bool {
	return len(tokenComparableMissing(in)) == 0
}

func tokenComparableMissing(in tokenDetectionInput) []string {
	f, u := in.Fact, in.Usage
	stream := u.Fields["is_stream"].Int64()
	mapping := tokenMeteringOperand(u, "is_model_mapped")
	// Missing mapping metadata permits only a disclosed statistical comparison
	// of the recorded model. It is never converted to "not mapped", and the
	// bucket keeps unknown and verified mappings distinct. Malformed mappings
	// and explicitly mapped requests without a target remain ineligible.
	mappingUsable := mapping.State == "missing" || tokenMeteringFieldUsable(mapping) && (mapping.Value != "true" || u.Fields["upstream_model_name"].State == "valid")
	_, entryUsable := tokenComparisonEntry(in)
	pathUsable := f.RequestPath != "" || tokenMeteringFieldUsable(u.Fields["request_path"])
	var missing []string
	for _, v := range []struct {
		name string
		ok   bool
	}{
		{"customer", f.UserID > 0}, {"channel", f.ChannelID > 0}, {"model", f.Model != "" && f.Model != "[unsupported]"}, {"group", f.UserGroup != ""},
		{"exact_association", f.LinkState == "unique" && f.EvidenceLevel == "exact"}, {"request_length", f.LengthState == "valid" && f.RequestLength != nil && *f.RequestLength > 0},
		{"usage_semantic_and_total", u.TotalInput != nil}, {"request_path", pathUsable}, {"stream", stream != nil && (*stream == 0 || *stream == 1)}, {"model_mapping", mappingUsable}, {"entry_route_status_version", entryUsable},
	} {
		if !v.ok {
			missing = append(missing, v.name)
		}
	}
	return missing
}
func tokenCacheMetrics(u tokenMeteringParsed) (string, string) {
	if u.TotalInput == nil || *u.TotalInput <= 0 {
		return "", ""
	}
	a, e := tokenWeightedInput(u)
	if u.Adapter == "newapi-rc48" && u.Contract != nil && tokenMeteringOperand(u, "billing_mode").Value == "tiered_expr" {
		if tokenMeteringOperand(u, "request_rules").Value != "[]" && !tokenMeteringRecordedRulesUsable(u) {
			return "", ""
		}
		// Dynamic pricing may differ from the legacy model/cache ratios logged
		// next to it. Compare input-only cost with the same frozen expression,
		// tier length and total input, with output excluded on both sides.
		expr, err := metering.ParsePriceExpression(u.Fields["expr_b64"].Value)
		if err != nil {
			return "", ""
		}
		vars, err := tokenTierVariables(u, expr, big.NewRat(1, 1))
		if err != nil {
			return "", ""
		}
		vars["c"] = new(big.Rat)
		evaluate := func(values map[string]*big.Rat) (*big.Rat, string, error) {
			floats := make(map[string]float64, len(values))
			for k, v := range values {
				floats[k], _ = v.Float64()
			}
			amount, tier, err := expr.EvaluateFloat(floats)
			if err != nil {
				return nil, tier, err
			}
			return new(big.Rat).SetFloat64(amount), tier, nil
		}
		inputCost, tier, err := evaluate(vars)
		if err != nil {
			return "", ""
		}
		for _, key := range []string{"cr", "cc", "cc1h", "cc12h"} {
			vars[key] = new(big.Rat)
		}
		vars["p"] = big.NewRat(*u.TotalInput, 1)
		plainCost, plainTier, err := evaluate(vars)
		if err != nil || plainCost.Sign() <= 0 || tier != plainTier || tier != u.Fields["matched_tier"].Value {
			return "", ""
		}
		a = new(big.Rat).Mul(new(big.Rat).Quo(inputCost, plainCost), big.NewRat(*u.TotalInput, 1))
		e = nil
	}
	if e != nil {
		return "", ""
	}
	read, e := tokenRat(u, "cache_tokens")
	if e != nil {
		return "", ""
	}
	t := big.NewRat(*u.TotalInput, 1)
	return new(big.Rat).Quo(read, t).RatString(), new(big.Rat).Quo(a, t).RatString()
}
func tokenReferenceFrom(tx *gorm.DB, in tokenDetectionInput, p tokenDetectionPolicy, stage int, now int64) (tokenReference, error) {
	f := in.Fact
	until := f.CreatedAt
	switch stage {
	case 1:
		until += 300
	case 2:
		until += 600
	case 3:
		until += 3600
	}
	until = min(until, now+1)
	dimensionBucket := tokenReferenceBucket(in, p)
	ref := tokenReference{Bucket: tokenDetectionHash([]any{dimensionBucket, f.ChannelID, f.RequestLength}), Status: "insufficient", Reason: "comparable_fields_missing", From: f.CreatedAt - p.LookbackSeconds, Through: until, ExactSize: true, Samples: []tokenReferenceSample{}, ExclusionReasons: map[string]int{}, Gaps: []string{"supplier_independence_unverified", "same_size_does_not_prove_same_content", "encoding_and_conversion_unknown"}}
	if tokenMeteringOperand(in.Usage, "is_model_mapped").State == "missing" {
		ref.Gaps = append(ref.Gaps, "model_mapping_unverified")
	}
	if missing := tokenComparableMissing(in); len(missing) > 0 {
		ref.MissingComparable = missing
		return ref, nil
	}
	// Anchors are selected by event time. A later live reference cannot leak into
	// a first historical replay, even if it already exists in the database.
	var anchorRow TokenDetectionReference
	if err := tx.Where("bucket = ? AND through <= ?", ref.Bucket, f.CreatedAt).Order("through, id").Limit(1).Find(&anchorRow).Error; err != nil {
		return ref, err
	}
	var anchor tokenReference
	if anchorRow.ID != "" {
		ref.ParentID = anchorRow.ID
		if anchorRow.InvalidReason != "" {
			ref.Reason = anchorRow.InvalidReason
			return ref, nil
		}
		if anchorRow.SnapshotJSON == "" || anchorRow.ExpiresAt <= now {
			ref.Reason = "anchor_expired_requires_calibration"
			return ref, nil
		}
		if err := json.Unmarshal([]byte(anchorRow.SnapshotJSON), &anchor); err != nil {
			return ref, err
		}
		ref.ParentID = anchorRow.ID
		ids := make([]int64, 0, len(anchor.Samples))
		for _, sample := range anchor.Samples {
			ids = append(ids, sample.LogID)
		}
		var isolated int64
		if err := tx.Model(&TokenDetectionCurrent{}).Where("source_epoch = ? AND log_id IN ? AND quarantined = ?", f.SourceEpoch, ids, true).Count(&isolated).Error; err != nil {
			return ref, err
		}
		if isolated > 0 {
			ref.Reason = "anchor_candidate_isolated"
			return ref, nil
		}
		var live []TokenMeteringFact
		if err := tokenReferenceFactIdentities(tx, f.SourceEpoch, ids).
			Select("log_id,fact_hash,link_state,link_reason,evidence_level,length_state,evidence_json,request_length,request_length_version,request_length_meaning,request_length_observed_at,entry_event_id,entry_node,entry_route,entry_method,entry_status,entry_observed_at,request_path,request_method,missing_fields").Find(&live).Error; err != nil {
			return ref, err
		}
		if len(live) != len(ids) {
			ref.Reason = "anchor_source_missing"
			return ref, nil
		}
		for _, sample := range anchor.Samples {
			// Legacy snapshots did not freeze association identity. Never infer
			// equality from a length alone; original decision freezes stay intact.
			if sample.EvidenceHash == "" {
				ref.Reason = "anchor_evidence_version_unavailable"
				return ref, nil
			}
			for _, current := range live {
				if sample.LogID == current.LogID && (sample.FactHash != current.FactHash || sample.EvidenceHash != tokenMeteringEvidenceHash(current) || current.LinkState != "unique" || current.EvidenceLevel != "exact" || current.LengthState != "valid" || current.RequestLength == nil || sample.Length != *current.RequestLength) {
					ref.Reason = "anchor_evidence_changed_requires_review"
					return ref, nil
				}
			}
		}
		// A validated frozen anchor is the authoritative baseline. On a large
		// retained queue, expanding it for every later request rereads the same
		// JSON payloads and holds the single writer for seconds. Probe only the
		// bounded number of newly eligible identities: no additions can reuse
		// the anchor silently, while a large expansion is disclosed as capacity
		// and keeps the frozen operands unchanged.
		var additions []struct{ LogID int64 }
		probe := tokenReferenceCandidateQuery(tx, f, p, ref.From, until).
			Select("f.log_id").Where("f.created_at > ?", anchor.Through).Limit(65)
		if err := probe.Scan(&additions).Error; err != nil {
			return ref, err
		}
		if len(additions) == 0 {
			// The anchor itself is already the frozen reference. Reusing it must
			// keep its identity and provenance stable; adding its own ID as a new
			// parent would manufacture a different decision hash on every
			// generation-only recheck.
			return anchor, nil
		}
		if len(additions) >= 65 {
			return tokenFrozenReferenceAfterCapacity(anchor, anchorRow.ID), nil
		}
	}
	// Apply cheap, known comparison dimensions before the bounded page cap.
	// Same-channel traffic and unrelated sizes must not crowd all cross-channel
	// controls out of the page or let an upstream's own anomaly become its norm.
	exact, near := []tokenReferenceSample{}, []tokenReferenceSample{}
	widths := []int64{0}
	if p.SizeTolerance > 0 {
		widths = append(widths, min(int64(16), p.SizeTolerance))
	}
	if p.SizeTolerance > 16 {
		widths = append(widths, p.SizeTolerance)
	}
	seen := map[int64]bool{}
	exclude := func(reason string) { ref.Excluded++; ref.ExclusionReasons[reason]++ }
	for _, width := range widths {
		var identities []struct{ LogID int64 }
		scope := p
		scope.SizeTolerance = width
		err := tokenReferenceCandidateQuery(tx, f, scope, ref.From, until).Scan(&identities).Error
		if err != nil {
			return ref, err
		}
		if len(identities) > 200 {
			if anchor.Upper != nil {
				return tokenFrozenReferenceAfterCapacity(anchor, anchorRow.ID), nil
			}
			ref.Truncated = true
			ref.Reason = "reference_query_truncated"
			return ref, nil
		}
		type row struct {
			TokenMeteringFact
			Quarantined bool
		}
		var rows []row
		if len(identities) > 0 {
			ids := make([]int64, len(identities))
			for i, identity := range identities {
				ids[i] = identity.LogID
			}
			// Both reads share the worker transaction. The cap is applied to the
			// same ordered eligible population before JSON decoding, including the
			// rows that will later be excluded by pricing or quarantine evidence.
			if err := tokenReferenceFactIdentities(tx, f.SourceEpoch, ids).Select("f.*, COALESCE(c.quarantined,0) AS quarantined").
				Joins("LEFT JOIN token_detection_currents AS c ON c.source_epoch=f.source_epoch AND c.log_id=f.log_id").
				Order("f.created_at,f.log_id").Scan(&rows).Error; err != nil {
				return ref, err
			}
		}
		for _, r := range rows {
			if seen[r.LogID] {
				continue
			}
			seen[r.LogID] = true
			candidate, e := tokenInputForPolicy(r.TokenMeteringFact, p)
			if e != nil {
				return ref, e
			}
			if r.Quarantined {
				exclude("candidate_quarantined")
				continue
			}
			if missing := tokenComparableMissing(candidate); len(missing) > 0 {
				exclude("fields:" + strings.Join(missing, ","))
				continue
			}
			if tokenReferenceBucket(candidate, p) != dimensionBucket {
				exclude("incompatible:" + strings.Join(tokenReferenceScopeDifferences(in, candidate), ","))
				continue
			}
			format := tokenFormatRule(candidate.Usage)
			price := tokenPrice(candidate.Usage, p, big.NewRat(1, 1))
			quota := candidate.Usage.Fields["quota"].Int64()
			if format.Hit || !format.Eligible || (price.Supported && quota != nil && price.Quota != big.NewInt(*quota).String()) {
				exclude("logged_usage_or_formula_inconsistent")
				continue
			}
			diff := new(big.Int).Sub(big.NewInt(*f.RequestLength), big.NewInt(*candidate.Fact.RequestLength))
			diff.Abs(diff)
			if !diff.IsInt64() || diff.Int64() > p.SizeTolerance || new(big.Rat).SetFrac(diff, big.NewInt(*f.RequestLength)).Cmp(tokenRatio(p.SizeRelative)) > 0 {
				exclude("size_relative_tolerance")
				continue
			}
			share, intensity := tokenCacheMetrics(candidate.Usage)
			sample := tokenReferenceSample{EvidenceHash: tokenMeteringEvidenceHash(r.TokenMeteringFact), Epoch: f.SourceEpoch, LogID: r.LogID, CreatedAt: r.CreatedAt, ChannelID: r.ChannelID, Length: *r.RequestLength, FactHash: r.FactHash, Input: *candidate.Usage.TotalInput, ReadShare: share, Intensity: intensity, Usage: candidate.Usage}
			if diff.Sign() == 0 {
				exact = append(exact, sample)
			} else {
				near = append(near, sample)
			}
		}
		channels := map[int64]bool{}
		for _, s := range append(append([]tokenReferenceSample{}, exact...), near...) {
			channels[s.ChannelID] = true
		}
		ref.Search = append(ref.Search, tokenReferenceSearch{Tolerance: width, Read: len(identities), Eligible: len(exact) + len(near), Channels: len(channels)})
		if len(exact)+len(near) >= p.MinSamples && len(channels) >= p.MinChannels {
			break
		}
	}
	selected := exact
	exactChannels := map[int64]bool{}
	for _, s := range exact {
		exactChannels[s.ChannelID] = true
	}
	if len(exact) < p.MinSamples || len(exactChannels) < p.MinChannels {
		selected = append(selected, near...)
		ref.ExactSize = false
	}
	// Seed outliers are isolated against a cross-channel median, never treated
	// as healthy. With an anchor, any upward shift is excluded from learning.
	values := make([]int64, 0, len(selected))
	for _, s := range selected {
		values = append(values, s.Input)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	channels := map[int64]bool{}
	for _, s := range selected {
		drift := anchor.Upper != nil && s.Input > *anchor.Upper
		if anchor.Upper == nil && p.InputRatio != "" && len(values) >= p.MinSamples {
			median := values[(len(values)-1)/2]
			drift = new(big.Rat).SetInt64(s.Input).Cmp(new(big.Rat).Mul(big.NewRat(median, 1), tokenRatio(p.InputRatio))) > 0
		}
		if drift {
			ref.DriftExcluded++
			continue
		}
		ref.Samples = append(ref.Samples, s)
		channels[s.ChannelID] = true
	}
	if len(ref.Samples) < p.MinSamples || len(channels) < p.MinChannels {
		if anchor.Upper != nil {
			anchor.ParentID = anchorRow.ID
			anchor.Reason = "anchor_frozen_distribution_shift"
			anchor.DriftExcluded = ref.DriftExcluded
			anchor.ID = ""
			return anchor, nil
		}
		ref.SampleCount = len(ref.Samples)
		ref.Reason = "reference_samples_or_channels_insufficient"
		if len(ref.Samples) == 0 {
			ref.Reason = "no_comparable_samples_in_window"
		} else if len(ref.Samples) < p.MinSamples {
			ref.Reason = "reference_samples_insufficient"
		} else {
			ref.Reason = "reference_channels_insufficient"
		}
		return ref, nil
	}
	if len(ref.Samples) > 64 {
		if anchor.Upper != nil {
			return tokenFrozenReferenceAfterCapacity(anchor, anchorRow.ID), nil
		}
		ref.Truncated = true
		ref.Reason = "reference_snapshot_capacity"
		ref.Samples = nil
		return ref, nil
	}
	upper := int64(0)
	for _, s := range ref.Samples {
		upper = max(upper, s.Input)
	}
	if anchor.Upper != nil {
		upper = *anchor.Upper
	} // Stable upper bound never learns a common shift.
	if anchorRow.ID != "" && tokenDetectionHash(ref.Samples) == tokenDetectionHash(anchor.Samples) {
		ref.ParentID = anchor.ParentID
	}
	ref.Upper = &upper
	ref.Status = "statistical"
	ref.Reason = "unverified_statistical_reference"
	return ref, nil
}

func tokenReferenceCandidateQuery(tx *gorm.DB, f TokenMeteringFact, p tokenDetectionPolicy, from, until int64) *gorm.DB {
	width := p.SizeTolerance
	if relative := tokenRatio(p.SizeRelative); relative != nil {
		tolerance := new(big.Rat).Mul(big.NewRat(*f.RequestLength, 1), relative)
		n := new(big.Int).Quo(tolerance.Num(), tolerance.Denom())
		if n.IsInt64() {
			width = min(width, n.Int64())
		}
	}
	table := "token_metering_facts AS f"
	if tx.Dialector.Name() == "sqlite" && tx.Migrator().HasIndex(&TokenMeteringFact{}, tokenReferenceCandidateIndex) {
		table += " INDEXED BY " + tokenReferenceCandidateIndex
	} else if tx.Dialector.Name() == "sqlite" && tx.Migrator().HasIndex(&TokenMeteringFact{}, "idx_token_reference_input") {
		// SQLite otherwise favors the epoch/time index to avoid sorting equal
		// timestamps by log_id. That reads every user's large fact payload in
		// the interval while holding the writer. Bound the lookup by the existing
		// epoch/user/model/time index; retain the exact filters, order and cap.
		table += " INDEXED BY idx_token_reference_input"
	}
	return tx.Table(table).Select("f.log_id").
		Where("f.source_epoch=? AND f.user_id=? AND f.model=? AND f.user_group=? AND f.created_at>=? AND f.created_at<? AND f.channel_id<>?", f.SourceEpoch, f.UserID, f.Model, f.UserGroup, from, until, f.ChannelID).
		Where("f.link_state='unique' AND f.evidence_level='exact' AND f.length_state='valid' AND f.request_length>=? AND f.request_length<=?", max(int64(1), *f.RequestLength-width), *f.RequestLength+min(width, int64(^uint64(0)>>1)-*f.RequestLength)).
		Order("f.created_at,f.log_id").Limit(201)
}

// More newly available controls cannot erase an already frozen, revalidated
// baseline. Keep its original interval and operands; do not present the capped
// expansion as complete or learn from its partial prefix.
func tokenFrozenReferenceAfterCapacity(anchor tokenReference, parent string) tokenReference {
	anchor.ParentID, anchor.ID = parent, ""
	anchor.Reason = "anchor_frozen_reference_capacity"
	for _, gap := range anchor.Gaps {
		if gap == "reference_expansion_capacity" {
			return anchor
		}
	}
	anchor.Gaps = append(anchor.Gaps, "reference_expansion_capacity")
	return anchor
}

// Only validity metadata changes. Frozen reference operands and decision
// revisions remain immutable, including after live source retention.
func invalidateTokenReference(tx *gorm.DB, id, reason string, now int64) error {
	return tx.Model(&TokenDetectionReference{}).Where("id = ? AND (invalid_reason = ? OR invalid_reason IS NULL)", id, "").Updates(map[string]any{"invalid_reason": reason, "invalidated_at": now}).Error
}
