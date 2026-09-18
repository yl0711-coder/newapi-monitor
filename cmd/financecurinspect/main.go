// Command financecurinspect performs a local, read-only CUR 2.0 coverage
// audit. It never connects to AWS and never decides business ownership.
package main

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

type output struct {
	Rows                 int64                            `json:"rows"`
	SourceSHA256         string                           `json:"source_sha256"`
	Currency             string                           `json:"currency"`
	BillingPeriodStart   int64                            `json:"billing_period_start_unix"`
	BillingPeriodEnd     int64                            `json:"billing_period_end_unix"`
	FirstUsageUnix       int64                            `json:"first_usage_unix"`
	LastUsageThroughUnix int64                            `json:"last_usage_through_unix"`
	UsageHours           int                              `json:"usage_hours"`
	TotalNanoUSD         int64                            `json:"total_nano_usd"`
	LineItemTypes        map[string]financecur.CostBucket `json:"line_item_types"`
	Services             []financecur.ServiceBucket       `json:"services"`
	ResourceDimensions   int                              `json:"resource_dimensions"`
	ResourceSample       []financecur.ResourceBucket      `json:"resource_sample,omitempty"`
	CostEntryDimensions  int                              `json:"cost_entry_dimensions"`
	Allocation           *allocationOutput                `json:"allocation,omitempty"`
}

type allocationOutput struct {
	Complete            bool                               `json:"complete"`
	PolicySHA256        string                             `json:"policy_sha256"`
	Entries             int64                              `json:"entries"`
	Rows                int64                              `json:"rows"`
	ZeroCostEntries     int64                              `json:"zero_cost_entries"`
	ZeroCostRows        int64                              `json:"zero_cost_rows"`
	TotalNanoUSD        int64                              `json:"total_nano_usd"`
	NexusAPINanoUSD     int64                              `json:"nexusapi_nano_usd"`
	ExcludedNanoUSD     int64                              `json:"excluded_nano_usd"`
	UnallocatedNanoUSD  int64                              `json:"unallocated_nano_usd"`
	ConflictNanoUSD     int64                              `json:"conflict_nano_usd"`
	TotalAbsNanoUSD     uint64                             `json:"total_abs_nano_usd"`
	AllocatedAbsNanoUSD uint64                             `json:"allocated_abs_nano_usd"`
	IssueCount          int                                `json:"issue_count"`
	IssueSample         []financecur.AllocationIssue       `json:"issue_sample"`
	IssueBuckets        []financecur.AllocationIssueBucket `json:"issue_buckets"`
	Rules               []financecur.AllocationRuleResult  `json:"rules"`
	Statement           financecur.Statement               `json:"statement"`
	Timeline            financecur.Timeline                `json:"timeline"`
}

func main() {
	path := flag.String("file", "", "local CUR 2.0 Parquet file")
	policyPath := flag.String("policy", "", "optional local allocation-policy JSON file")
	artifactPath := flag.String("artifact-out", "", "optional new local finance artifact JSON file (requires -policy; never overwrites)")
	billingFinalized := flag.Bool("billing-finalized", false, "mark the CUR billing period final after independent invoice verification")
	resourceLimit := flag.Int("resource-limit", 0, "include this many highest-cost resource dimensions (0-1000)")
	issueLimit := flag.Int("issue-limit", 100, "include this many allocation issue details and buckets (0-1000)")
	flag.Parse()
	if *path == "" {
		fail("-file is required")
	}
	if *resourceLimit < 0 || *resourceLimit > 1_000 {
		fail("-resource-limit must be between 0 and 1000")
	}
	if *issueLimit < 0 || *issueLimit > maxCLIAllocationIssues {
		fail("-issue-limit must be between 0 and %d", maxCLIAllocationIssues)
	}
	if *billingFinalized && *policyPath == "" {
		fail("-billing-finalized requires -policy")
	}
	if *artifactPath != "" && *policyPath == "" {
		fail("-artifact-out requires -policy")
	}
	file, err := os.Open(*path)
	if err != nil {
		fail("open CUR file: %v", err)
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		fail("hash CUR file: %v", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		fail("rewind CUR file: %v", err)
	}
	audit, err := financecur.ReadParquet(file, financecur.Limits{})
	if err != nil {
		fail("inspect CUR file: %v", err)
	}
	if *billingFinalized && audit.BillingPeriodEnd > time.Now().Unix() {
		fail("cannot finalize an open CUR billing period")
	}
	services := audit.Services
	if len(services) > 25 {
		services = services[:25]
	}
	result := output{
		Rows: audit.Rows, SourceSHA256: fmt.Sprintf("%x", digest.Sum(nil)), Currency: audit.Currency,
		BillingPeriodStart: audit.BillingPeriodStart, BillingPeriodEnd: audit.BillingPeriodEnd,
		FirstUsageUnix: audit.FirstUsageUnix, LastUsageThroughUnix: audit.LastUsageThroughUnix,
		UsageHours: audit.UsageHours, TotalNanoUSD: audit.TotalNanoUSD,
		LineItemTypes: audit.LineItemTypes, Services: services, ResourceDimensions: len(audit.Resources),
		CostEntryDimensions: len(audit.CostEntries),
	}
	if *resourceLimit > 0 {
		limit := *resourceLimit
		if limit > len(audit.Resources) {
			limit = len(audit.Resources)
		}
		result.ResourceSample = audit.Resources[:limit]
	}
	if *policyPath != "" {
		rules, err := readPolicy(*policyPath)
		if err != nil {
			fail("read allocation policy: %v", err)
		}
		report, err := financecur.Allocate(audit.CostEntries, rules)
		if err != nil {
			fail("apply allocation policy: %v", err)
		}
		policyHash, err := financecur.AllocationPolicyHash(rules)
		if err != nil {
			fail("hash allocation policy: %v", err)
		}
		statement, timeline, err := financecur.BuildStatement(audit, report, result.SourceSHA256, policyHash, "Asia/Shanghai", *billingFinalized)
		if err != nil {
			fail("build CUR statement: %v", err)
		}
		if *artifactPath != "" {
			artifact, err := financecur.NewArtifact(statement, timeline)
			if err != nil {
				fail("build CUR artifact: %v", err)
			}
			if err := writeArtifactExclusive(*artifactPath, artifact); err != nil {
				fail("write CUR artifact: %v", err)
			}
		}
		issues := limitAllocationOutput(report.Issues, *issueLimit)
		issueBuckets := limitAllocationOutput(report.IssueBuckets, *issueLimit)
		result.Allocation = &allocationOutput{
			Complete: report.Publishable, PolicySHA256: policyHash, Entries: report.Entries, Rows: report.Rows,
			ZeroCostEntries: report.ZeroCostEntries, ZeroCostRows: report.ZeroCostRows,
			TotalNanoUSD: report.TotalNanoUSD, NexusAPINanoUSD: report.NexusAPINanoUSD,
			ExcludedNanoUSD: report.ExcludedNanoUSD, UnallocatedNanoUSD: report.UnallocatedNanoUSD,
			ConflictNanoUSD: report.ConflictNanoUSD, TotalAbsNanoUSD: report.TotalAbsNanoUSD,
			AllocatedAbsNanoUSD: report.AllocatedAbsNanoUSD, IssueCount: int(report.IssueCount),
			IssueSample: issues, IssueBuckets: issueBuckets, Rules: report.Rules, Statement: statement, Timeline: timeline,
		}
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fail("encode result: %v", err)
	}
}

const maxCLIAllocationIssues = 1_000

func limitAllocationOutput[T any](items []T, limit int) []T {
	if limit <= 0 {
		return nil
	}
	if len(items) <= limit {
		return items
	}
	return items[:limit]
}

func writeArtifactExclusive(path string, artifact financecur.Artifact) (returnErr error) {
	if path == "" {
		return fmt.Errorf("artifact path is empty")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		if err := file.Close(); returnErr == nil && err != nil {
			returnErr = err
		}
		if !complete {
			_ = os.Remove(path)
		}
	}()
	if err := financecur.EncodeArtifact(file, artifact); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	complete = true
	return nil
}

func readPolicy(path string) ([]financecur.AllocationRule, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, 1<<20))
	decoder.DisallowUnknownFields()
	var rules []financecur.AllocationRule
	if err := decoder.Decode(&rules); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("policy contains trailing JSON")
		}
		return nil, err
	}
	return rules, nil
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
