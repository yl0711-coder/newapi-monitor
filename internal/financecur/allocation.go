package financecur

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

const AllocationPPM = int64(1_000_000)

const maxAllocationIssueDetails = 1_000
const maxAllocationIssueBuckets = 100_000

const (
	ResourceAny    = "any"
	ResourceExact  = "exact"
	ResourcePrefix = "prefix"
	ResourceEmpty  = "empty"
)

// AllocationRule is an immutable policy decision supplied by an operator.
// NexusAPIPPM=1,000,000 includes all matched cost, 0 explicitly excludes it,
// and intermediate values allocate a documented shared-resource percentage.
type AllocationRule struct {
	ID                string `json:"id"`
	EffectiveFromUnix int64  `json:"effective_from_unix"`
	EffectiveToUnix   int64  `json:"effective_to_unix"`
	LineItemType      string `json:"line_item_type"`
	ProductCode       string `json:"product_code"`
	ServiceCode       string `json:"service_code"`
	UsageType         string `json:"usage_type"`
	Operation         string `json:"operation"`
	Description       string `json:"description"`
	ResourceMatch     string `json:"resource_match"`
	ResourceID        string `json:"resource_id"`
	NexusAPIPPM       int64  `json:"nexusapi_ppm"`
	Reason            string `json:"reason"`
}

type AllocationRuleResult struct {
	RuleID          string `json:"rule_id"`
	Entries         int64  `json:"entries"`
	Rows            int64  `json:"rows"`
	SourceNanoUSD   int64  `json:"source_nano_usd"`
	NexusAPINanoUSD int64  `json:"nexusapi_nano_usd"`
	ExcludedNanoUSD int64  `json:"excluded_nano_usd"`
}

type AllocationIssue struct {
	Kind        string    `json:"kind"`
	Entry       CostEntry `json:"entry"`
	RuleIDs     []string  `json:"rule_ids,omitempty"`
	Description string    `json:"description"`
}

type AllocationIssueBucket struct {
	Kind         string   `json:"kind"`
	LineItemType string   `json:"line_item_type"`
	ProductCode  string   `json:"product_code"`
	ServiceCode  string   `json:"service_code"`
	UsageType    string   `json:"usage_type"`
	Operation    string   `json:"operation"`
	Description  string   `json:"description"`
	ResourceID   string   `json:"resource_id"`
	RuleIDs      []string `json:"rule_ids,omitempty"`
	Entries      int64    `json:"entries"`
	Rows         int64    `json:"rows"`
	NanoUSD      int64    `json:"nano_usd"`
	AbsNanoUSD   uint64   `json:"abs_nano_usd"`
}

// EntryAllocation retains the exact, bounded cost-entry decision used to
// build an AllocationReport. It is intentionally omitted from JSON reports;
// callers use it to derive audited time buckets without re-running or
// re-implementing allocation rules.
type EntryAllocation struct {
	Entry              CostEntry `json:"-"`
	Status             string    `json:"-"`
	RuleIDs            []string  `json:"-"`
	NexusAPINanoUSD    int64     `json:"-"`
	ExcludedNanoUSD    int64     `json:"-"`
	UnallocatedNanoUSD int64     `json:"-"`
	ConflictNanoUSD    int64     `json:"-"`
}

type AllocationReport struct {
	Publishable         bool                    `json:"publishable"`
	Entries             int64                   `json:"entries"`
	Rows                int64                   `json:"rows"`
	ZeroCostEntries     int64                   `json:"zero_cost_entries"`
	ZeroCostRows        int64                   `json:"zero_cost_rows"`
	TotalNanoUSD        int64                   `json:"total_nano_usd"`
	NexusAPINanoUSD     int64                   `json:"nexusapi_nano_usd"`
	ExcludedNanoUSD     int64                   `json:"excluded_nano_usd"`
	UnallocatedNanoUSD  int64                   `json:"unallocated_nano_usd"`
	ConflictNanoUSD     int64                   `json:"conflict_nano_usd"`
	TotalAbsNanoUSD     uint64                  `json:"total_abs_nano_usd"`
	AllocatedAbsNanoUSD uint64                  `json:"allocated_abs_nano_usd"`
	IssueCount          int64                   `json:"issue_count"`
	Rules               []AllocationRuleResult  `json:"rules"`
	Issues              []AllocationIssue       `json:"issues"`
	IssueBuckets        []AllocationIssueBucket `json:"issue_buckets"`
	EntryAllocations    []EntryAllocation       `json:"-"`
}

type allocationIssueBucketKey struct {
	kind, lineType, product, service, usageType, operation, description, resource, ruleIDs string
}

// Allocate applies explicit resource policy without inference. Any uncovered,
// partially covered, or multiply matched entry keeps the report unpublished.
func Allocate(entries []CostEntry, rules []AllocationRule) (AllocationReport, error) {
	normalizedRules, err := normalizeAllocationRules(rules)
	if err != nil {
		return AllocationReport{}, err
	}
	report := AllocationReport{}
	issueBuckets := make(map[allocationIssueBucketKey]*AllocationIssueBucket)
	byRule := make(map[string]*AllocationRuleResult, len(normalizedRules))
	for _, rule := range normalizedRules {
		byRule[rule.ID] = &AllocationRuleResult{RuleID: rule.ID}
	}
	for _, entry := range entries {
		if err := validateCostEntry(entry); err != nil {
			return AllocationReport{}, err
		}
		report.Entries++
		if err := addInt64(&report.Rows, entry.Rows); err != nil {
			return AllocationReport{}, err
		}
		if err := addInt64(&report.TotalNanoUSD, entry.NanoUSD); err != nil {
			return AllocationReport{}, err
		}
		abs, err := absNanoUSD(entry.NanoUSD)
		if err != nil || report.TotalAbsNanoUSD > math.MaxUint64-abs {
			return AllocationReport{}, errors.New("CUR absolute cost exceeds integer safety range")
		}
		report.TotalAbsNanoUSD += abs
		if entry.NanoUSD == 0 {
			report.ZeroCostEntries++
			if err := addInt64(&report.ZeroCostRows, entry.Rows); err != nil {
				return AllocationReport{}, err
			}
			continue
		}

		matches, partial := allocationMatches(entry, normalizedRules)
		decision := EntryAllocation{Entry: entry}
		switch len(matches) {
		case 0:
			kind, description := "unallocated", "no allocation rule covers this cost entry"
			if len(partial) > 0 {
				kind, description = "partial_effective_period", "matching rule overlaps only part of the cost interval"
			}
			if err := addAllocationIssue(&report, issueBuckets, AllocationIssue{Kind: kind, Entry: entry, RuleIDs: partial, Description: description}); err != nil {
				return AllocationReport{}, err
			}
			if err := addInt64(&report.UnallocatedNanoUSD, entry.NanoUSD); err != nil {
				return AllocationReport{}, err
			}
			decision.Status = kind
			decision.RuleIDs = append([]string(nil), partial...)
			decision.UnallocatedNanoUSD = entry.NanoUSD
		case 1:
			rule := matches[0]
			nexus := proportionalNanoUSD(entry.NanoUSD, rule.NexusAPIPPM)
			excluded := entry.NanoUSD - nexus
			if err := addInt64(&report.NexusAPINanoUSD, nexus); err != nil {
				return AllocationReport{}, err
			}
			if err := addInt64(&report.ExcludedNanoUSD, excluded); err != nil {
				return AllocationReport{}, err
			}
			if report.AllocatedAbsNanoUSD > math.MaxUint64-abs {
				return AllocationReport{}, errors.New("CUR allocated absolute cost exceeds integer safety range")
			}
			report.AllocatedAbsNanoUSD += abs
			result := byRule[rule.ID]
			result.Entries++
			if err := addInt64(&result.Rows, entry.Rows); err != nil {
				return AllocationReport{}, err
			}
			if err := addInt64(&result.SourceNanoUSD, entry.NanoUSD); err != nil {
				return AllocationReport{}, err
			}
			if err := addInt64(&result.NexusAPINanoUSD, nexus); err != nil {
				return AllocationReport{}, err
			}
			if err := addInt64(&result.ExcludedNanoUSD, excluded); err != nil {
				return AllocationReport{}, err
			}
			decision.Status = "allocated"
			decision.RuleIDs = []string{rule.ID}
			decision.NexusAPINanoUSD = nexus
			decision.ExcludedNanoUSD = excluded
		default:
			ids := make([]string, 0, len(matches))
			for _, rule := range matches {
				ids = append(ids, rule.ID)
			}
			sort.Strings(ids)
			if err := addAllocationIssue(&report, issueBuckets, AllocationIssue{Kind: "conflict", Entry: entry, RuleIDs: ids, Description: "multiple allocation rules cover this cost entry"}); err != nil {
				return AllocationReport{}, err
			}
			if err := addInt64(&report.ConflictNanoUSD, entry.NanoUSD); err != nil {
				return AllocationReport{}, err
			}
			decision.Status = "conflict"
			decision.RuleIDs = ids
			decision.ConflictNanoUSD = entry.NanoUSD
		}
		report.EntryAllocations = append(report.EntryAllocations, decision)
	}
	for _, rule := range normalizedRules {
		report.Rules = append(report.Rules, *byRule[rule.ID])
	}
	for _, bucket := range issueBuckets {
		report.IssueBuckets = append(report.IssueBuckets, *bucket)
	}
	sort.Slice(report.Rules, func(i, j int) bool { return report.Rules[i].RuleID < report.Rules[j].RuleID })
	sort.Slice(report.IssueBuckets, func(i, j int) bool {
		left, right := report.IssueBuckets[i], report.IssueBuckets[j]
		if left.AbsNanoUSD != right.AbsNanoUSD {
			return left.AbsNanoUSD > right.AbsNanoUSD
		}
		if left.ProductCode != right.ProductCode {
			return left.ProductCode < right.ProductCode
		}
		if left.ResourceID != right.ResourceID {
			return left.ResourceID < right.ResourceID
		}
		return left.UsageType < right.UsageType
	})
	report.Publishable = report.IssueCount == 0 && report.Entries > 0 && report.AllocatedAbsNanoUSD == report.TotalAbsNanoUSD
	return report, nil
}

// AllocationPolicyHash returns an order-independent digest of normalized
// policy content. It is suitable for immutable statement provenance, not for
// authenticating an untrusted policy file.
func AllocationPolicyHash(rules []AllocationRule) (string, error) {
	normalized, err := normalizeAllocationRules(rules)
	if err != nil {
		return "", err
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].ID < normalized[j].ID })
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("encode allocation policy: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func normalizeAllocationRules(rules []AllocationRule) ([]AllocationRule, error) {
	normalized := append([]AllocationRule(nil), rules...)
	seen := make(map[string]struct{}, len(rules))
	for i := range normalized {
		rule := &normalized[i]
		rule.ID = strings.TrimSpace(rule.ID)
		rule.LineItemType = strings.TrimSpace(rule.LineItemType)
		rule.ProductCode = strings.TrimSpace(rule.ProductCode)
		rule.ServiceCode = strings.TrimSpace(rule.ServiceCode)
		rule.UsageType = strings.TrimSpace(rule.UsageType)
		rule.Operation = strings.TrimSpace(rule.Operation)
		rule.Description = strings.TrimSpace(rule.Description)
		rule.ResourceMatch = strings.TrimSpace(rule.ResourceMatch)
		rule.ResourceID = strings.TrimSpace(rule.ResourceID)
		rule.Reason = strings.TrimSpace(rule.Reason)
		if rule.ID == "" || rule.Reason == "" {
			return nil, fmt.Errorf("allocation rule %d requires id and reason", i)
		}
		if _, exists := seen[rule.ID]; exists {
			return nil, fmt.Errorf("duplicate allocation rule id %q", rule.ID)
		}
		seen[rule.ID] = struct{}{}
		if rule.EffectiveFromUnix < 0 || rule.EffectiveToUnix < 0 || rule.EffectiveToUnix != 0 && rule.EffectiveToUnix <= rule.EffectiveFromUnix {
			return nil, fmt.Errorf("allocation rule %q has invalid effective period", rule.ID)
		}
		if rule.NexusAPIPPM < 0 || rule.NexusAPIPPM > AllocationPPM {
			return nil, fmt.Errorf("allocation rule %q has invalid nexusapi_ppm", rule.ID)
		}
		switch rule.ResourceMatch {
		case ResourceAny:
			if rule.ResourceID != "" {
				return nil, fmt.Errorf("allocation rule %q cannot set resource_id with any match", rule.ID)
			}
		case ResourceEmpty:
			if rule.ResourceID != "" {
				return nil, fmt.Errorf("allocation rule %q cannot set resource_id with empty match", rule.ID)
			}
		case ResourceExact, ResourcePrefix:
			if rule.ResourceID == "" {
				return nil, fmt.Errorf("allocation rule %q requires resource_id", rule.ID)
			}
		default:
			return nil, fmt.Errorf("allocation rule %q has invalid resource_match", rule.ID)
		}
		if rule.LineItemType == "" && rule.ProductCode == "" && rule.ServiceCode == "" && rule.UsageType == "" && rule.Operation == "" && rule.Description == "" && rule.ResourceMatch == ResourceAny {
			return nil, fmt.Errorf("allocation rule %q is an unsafe catch-all", rule.ID)
		}
	}
	return normalized, nil
}

func addAllocationIssue(report *AllocationReport, buckets map[allocationIssueBucketKey]*AllocationIssueBucket, issue AllocationIssue) error {
	report.IssueCount++
	if len(report.Issues) < maxAllocationIssueDetails {
		report.Issues = append(report.Issues, issue)
	}
	ruleIDs := strings.Join(issue.RuleIDs, "\x00")
	key := allocationIssueBucketKey{
		kind: issue.Kind, lineType: issue.Entry.LineItemType, product: issue.Entry.ProductCode,
		service: issue.Entry.ServiceCode, usageType: issue.Entry.UsageType, operation: issue.Entry.Operation,
		description: issue.Entry.Description,
		resource:    issue.Entry.ResourceID, ruleIDs: ruleIDs,
	}
	bucket := buckets[key]
	if bucket == nil {
		if len(buckets) >= maxAllocationIssueBuckets {
			return fmt.Errorf("CUR allocation issue dimension exceeds safety limit %d", maxAllocationIssueBuckets)
		}
		bucket = &AllocationIssueBucket{
			Kind: issue.Kind, LineItemType: issue.Entry.LineItemType, ProductCode: issue.Entry.ProductCode,
			ServiceCode: issue.Entry.ServiceCode, UsageType: issue.Entry.UsageType, Operation: issue.Entry.Operation,
			Description: issue.Entry.Description,
			ResourceID:  issue.Entry.ResourceID, RuleIDs: append([]string(nil), issue.RuleIDs...),
		}
		buckets[key] = bucket
	}
	bucket.Entries++
	if err := addInt64(&bucket.Rows, issue.Entry.Rows); err != nil {
		return err
	}
	if err := addInt64(&bucket.NanoUSD, issue.Entry.NanoUSD); err != nil {
		return err
	}
	abs, err := absNanoUSD(issue.Entry.NanoUSD)
	if err != nil || bucket.AbsNanoUSD > math.MaxUint64-abs {
		return errors.New("CUR issue absolute cost exceeds integer safety range")
	}
	bucket.AbsNanoUSD += abs
	return nil
}

func validateCostEntry(entry CostEntry) error {
	if entry.Rows <= 0 || entry.FromUnix <= 0 || entry.ToUnix <= entry.FromUnix {
		return errors.New("invalid CUR cost entry")
	}
	return nil
}

func allocationMatches(entry CostEntry, rules []AllocationRule) (full []*AllocationRule, partial []string) {
	for i := range rules {
		rule := &rules[i]
		if !ruleMatchesDimensions(entry, *rule) {
			continue
		}
		from := rule.EffectiveFromUnix
		to := rule.EffectiveToUnix
		fullyCovered := (from == 0 || entry.FromUnix >= from) && (to == 0 || entry.ToUnix <= to)
		if fullyCovered {
			full = append(full, rule)
			continue
		}
		overlaps := (to == 0 || entry.FromUnix < to) && (from == 0 || entry.ToUnix > from)
		if overlaps {
			partial = append(partial, rule.ID)
		}
	}
	sort.Strings(partial)
	return full, partial
}

func ruleMatchesDimensions(entry CostEntry, rule AllocationRule) bool {
	if rule.LineItemType != "" && entry.LineItemType != rule.LineItemType ||
		rule.ProductCode != "" && entry.ProductCode != rule.ProductCode ||
		rule.ServiceCode != "" && entry.ServiceCode != rule.ServiceCode ||
		rule.UsageType != "" && entry.UsageType != rule.UsageType ||
		rule.Operation != "" && entry.Operation != rule.Operation ||
		rule.Description != "" && entry.Description != rule.Description {
		return false
	}
	switch rule.ResourceMatch {
	case ResourceAny:
		return true
	case ResourceEmpty:
		return entry.ResourceID == ""
	case ResourceExact:
		return entry.ResourceID == rule.ResourceID
	case ResourcePrefix:
		return strings.HasPrefix(entry.ResourceID, rule.ResourceID)
	default:
		return false
	}
}

func proportionalNanoUSD(value, ppm int64) int64 {
	quotient, remainder := value/AllocationPPM, value%AllocationPPM
	return quotient*ppm + remainder*ppm/AllocationPPM
}

func absNanoUSD(value int64) (uint64, error) {
	if value == math.MinInt64 {
		return 0, errors.New("CUR absolute cost exceeds integer safety range")
	}
	if value < 0 {
		value = -value
	}
	return uint64(value), nil
}
