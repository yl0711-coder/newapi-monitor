package monitor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

// This is a conditional accounting scenario, NOT an approval or import tool.
// It reuses retained source files, verifies every reviewed resource dimension,
// and leaves all unidentified entries and credits unresolved.
func TestFinanceCURResourceCandidateLocalRehearsal(t *testing.T) {
	dir := os.Getenv("MONITOR_CUR_LOCAL_EVIDENCE_DIR")
	if dir == "" {
		t.Skip("requires retained local CUR evidence")
	}
	type resource struct {
		Product string `json:"product_code"`
		ID      string `json:"resource_id"`
		Rows    int64  `json:"rows"`
		Nano    int64  `json:"nano_usd"`
		Status  string `json:"status"`
		Reason  string `json:"reason"`
	}
	var review struct {
		Months []struct {
			Period    string     `json:"period"`
			SHA       string     `json:"source_sha256"`
			Total     int64      `json:"total_nano_usd"`
			Resources []resource `json:"resources"`
		} `json:"months"`
	}
	raw, err := os.ReadFile(filepath.Join(dir, "resource-attribution-review-r4.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &review); err != nil {
		t.Fatal(err)
	}
	if len(review.Months) != 2 {
		t.Fatal("expected two retained source months")
	}
	reviewSHA := sha256.Sum256(raw)
	type result struct {
		Period            string `json:"period"`
		SourceSHA         string `json:"source_sha256"`
		Rows              int64  `json:"rows"`
		Dimensions        int    `json:"dimensions"`
		Rules             int    `json:"rules"`
		Source            int64  `json:"source_nano_usd"`
		CandidateNexus    int64  `json:"candidate_nexusapi_nano_usd"`
		CandidateExcluded int64  `json:"candidate_excluded_nano_usd"`
		Unresolved        int64  `json:"unresolved_nano_usd"`
		Issues            int64  `json:"unresolved_entries"`
	}
	var results []result
	var artifacts []financecur.Artifact
	var candidateRules []financecur.AllocationRule
	for _, month := range review.Months {
		if month.Period != "2026-09" && month.Period != "2026-10" {
			t.Fatal("unexpected period")
		}
		f, err := os.Open(filepath.Join(dir, month.Period+".parquet"))
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.New()
		_, err = io.Copy(h, f)
		if err != nil {
			f.Close()
			t.Fatal(err)
		}
		if hex.EncodeToString(h.Sum(nil)) != month.SHA {
			f.Close()
			t.Fatal("source SHA changed")
		}
		if _, err = f.Seek(0, 0); err != nil {
			f.Close()
			t.Fatal(err)
		}
		audit, err := financecur.ReadParquet(f, financecur.Limits{})
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		type key struct{ Product, ID string }
		type counts struct{ Nano, Rows int64 }
		actual := map[key]counts{}
		for _, e := range audit.CostEntries {
			k := key{e.ProductCode, e.ResourceID}
			v := actual[k]
			if err := addFinanceCURInt64(&v.Nano, e.NanoUSD); err != nil {
				t.Fatal(err)
			}
			if err := addFinanceCURInt64(&v.Rows, e.Rows); err != nil {
				t.Fatal(err)
			}
			actual[k] = v
		}
		if len(actual) != len(month.Resources) {
			t.Fatal("review omits source resource dimensions")
		}
		seen := map[key]bool{}
		var rules []financecur.AllocationRule
		var expectedNexus, expectedExcluded, expectedUnresolved int64
		for i, r := range month.Resources {
			k := key{r.Product, r.ID}
			if seen[k] || actual[k] != (counts{r.Nano, r.Rows}) {
				t.Fatal("duplicate, omitted or changed resource evidence", r.Product, r.ID)
			}
			seen[k] = true
			switch r.Status {
			case "nexusapi_identified", "nexusapi_confirmed", "other_identified":
				// Reviewed anonymous domain fees must not become a broad rule
				// for every fee of that service. Keep pending until narrowed by
				// exact bill description / line-item dimensions.
				if r.ID == "" && !(r.Product == "AWSCostExplorer" && r.Status == "nexusapi_confirmed") {
					expectedUnresolved += r.Nano
					continue
				}
				ppm := financecur.AllocationPPM
				if r.Status == "other_identified" {
					ppm = 0
					expectedExcluded += r.Nano
				} else {
					expectedNexus += r.Nano
				}
				match := financecur.ResourceExact
				if r.ID == "" {
					// Only the already-confirmed Cost Explorer management fee has
					// permission to use an empty-resource selector in this scenario.
					if r.Product != "AWSCostExplorer" || r.Status != "nexusapi_confirmed" {
						t.Fatal("candidate would broaden an anonymous resource rule")
					}
					match = financecur.ResourceEmpty
				}
				rules = append(rules, financecur.AllocationRule{ID: fmt.Sprintf("scenario-%s-%04d", month.Period, i), EffectiveFromUnix: audit.BillingPeriodStart, EffectiveToUnix: audit.BillingPeriodEnd, ProductCode: r.Product, ResourceMatch: match, ResourceID: r.ID, NexusAPIPPM: ppm, Reason: "CONDITIONAL LOCAL SCENARIO: " + r.Reason})
			case "unknown_resource", "adjustment_review":
				expectedUnresolved += r.Nano
			case "zero_cost":
				if r.Nano != 0 {
					t.Fatal("nonzero resource labelled zero")
				}
			default:
				t.Fatal("unknown review state", r.Status)
			}
		}
		before := append([]financecur.CostEntry(nil), audit.CostEntries...)
		allocated, err := financecur.Allocate(audit.CostEntries, rules)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, audit.CostEntries) {
			t.Fatal("allocation mutated input")
		}
		if allocated.TotalNanoUSD != month.Total || allocated.NexusAPINanoUSD != expectedNexus || allocated.ExcludedNanoUSD != expectedExcluded || allocated.UnallocatedNanoUSD != expectedUnresolved || allocated.ConflictNanoUSD != 0 || allocated.Publishable {
			t.Fatal("conditional allocation mismatch or unsafe publication", month.Period)
		}
		if allocated.TotalNanoUSD != allocated.NexusAPINanoUSD+allocated.ExcludedNanoUSD+allocated.UnallocatedNanoUSD {
			t.Fatal("money not conserved")
		}
		// Check every decision, not only net totals: offsetting errors must fail.
		statusByKey := map[key]string{}
		for _, r := range month.Resources {
			statusByKey[key{r.Product, r.ID}] = r.Status
			if r.ID == "" && r.Product != "AWSCostExplorer" {
				statusByKey[key{r.Product, r.ID}] = "unknown_resource"
			}
		}
		for _, d := range allocated.EntryAllocations {
			s := statusByKey[key{d.Entry.ProductCode, d.Entry.ResourceID}]
			switch s {
			case "nexusapi_identified", "nexusapi_confirmed":
				if d.NexusAPINanoUSD != d.Entry.NanoUSD || d.ExcludedNanoUSD != 0 || d.UnallocatedNanoUSD != 0 {
					t.Fatal("NexusAPI row misallocated")
				}
			case "other_identified":
				if d.ExcludedNanoUSD != d.Entry.NanoUSD || d.NexusAPINanoUSD != 0 || d.UnallocatedNanoUSD != 0 {
					t.Fatal("other-business cost leaked")
				}
			default:
				if d.UnallocatedNanoUSD != d.Entry.NanoUSD || d.Status != "unallocated" || d.NexusAPINanoUSD != 0 || d.ExcludedNanoUSD != 0 {
					t.Fatal("unknown cost/credit silently assigned")
				}
			}
		}
		replayed, err := financecur.Allocate(audit.CostEntries, rules)
		if err != nil || !reflect.DeepEqual(allocated, replayed) {
			t.Fatal("replay changed allocation")
		}
		policySHA, err := financecur.AllocationPolicyHash(rules)
		if err != nil {
			t.Fatal(err)
		}
		statement, timeline, err := financecur.BuildStatement(audit, allocated, month.SHA, policySHA, "Asia/Shanghai", false)
		if err != nil {
			t.Fatal(err)
		}
		a, err := financecur.NewArtifact(statement, timeline)
		if err != nil {
			t.Fatal(err)
		}
		artifacts = append(artifacts, a)
		candidateRules = append(candidateRules, rules...)
		results = append(results, result{month.Period, month.SHA, audit.Rows, len(actual), len(rules), allocated.TotalNanoUSD, allocated.NexusAPINanoUSD, allocated.ExcludedNanoUSD, allocated.UnallocatedNanoUSD, allocated.IssueCount})
	}
	set, err := financecur.NewSourceSet(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	p, err := financecur.NewSourceProjector(set)
	if err != nil {
		t.Fatal(err)
	}
	projected, err := p.Query(1777564800, 1791475200)
	if err != nil {
		t.Fatal(err)
	}
	if projected.RangeComplete || len(projected.MissingBillingPeriods) == 0 {
		t.Fatal("missing historical periods concealed")
	}
	var dayTotal, monthTotal, productTotal int64
	for _, d := range projected.Days {
		dayTotal += d.Bucket.NexusAPINanoUSD
	}
	for _, m := range projected.Months {
		monthTotal += m.Bucket.NexusAPINanoUSD
	}
	for _, p := range projected.Products {
		for _, d := range p.Days {
			productTotal += d.NexusAPINanoUSD
		}
	}
	if dayTotal != projected.Known.NexusAPINanoUSD || monthTotal != dayTotal || productTotal != dayTotal {
		t.Fatal("day/month/product reconciliation mismatch")
	}
	// The only optional output is an inert review envelope, not a Monitor
	// source-set file or production configuration. Refuse overwriting evidence.
	out := struct {
		Kind            string                      `json:"kind"`
		ReviewSHA       string                      `json:"review_sha256"`
		ProductionReady bool                        `json:"production_ready"`
		Months          []result                    `json:"months"`
		Query           financecur.SourceProjection `json:"query"`
		CandidateRules  []financecur.AllocationRule `json:"candidate_rules"`
	}{"conditional_resource_rehearsal_not_approved", hex.EncodeToString(reviewSHA[:]), false, results, projected, candidateRules}
	if path := os.Getenv("MONITOR_CUR_RESOURCE_REHEARSAL_OUTPUT"); path != "" {
		encoded, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.Write(append(encoded, '\n'))
		closeErr := f.Close()
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	for _, r := range results {
		t.Logf("%s source=%d candidate_nexus=%d other=%d unresolved=%d rules=%d dimensions=%d", r.Period, r.Source, r.CandidateNexus, r.CandidateExcluded, r.Unresolved, r.Rules, r.Dimensions)
	}
	t.Logf("fixed-range candidate known nano=%d; missing source intervals=%d; final=false; no production write", dayTotal, len(projected.MissingBillingPeriods))
}
