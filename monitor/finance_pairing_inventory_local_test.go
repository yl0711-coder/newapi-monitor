//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"
)

// Read-only diagnosis, not a binding/import plan. Current configuration is only
// a candidate hint: even 100% temporal overlap does not establish ownership.
func TestFinancePairingSealedSourceInventory(t *testing.T) {
	path := os.Getenv("MONITOR_FINANCE_GAP_AUDIT_DB")
	reportPath := os.Getenv("MONITOR_FINANCE_PAIRING_AUDIT_REPORT")
	if path == "" || reportPath == "" {
		t.Skip("requires explicit sealed local database and accepted report")
	}
	const expectedHash = "28e3460df585bbde44071013ec13e8fa696e0aa94d39c334e477d85ec723a20e"
	if giftLocalFileHash(t, path) != expectedHash {
		t.Fatal("unexpected database")
	}
	reportHash := giftLocalFileHash(t, reportPath)
	t.Cleanup(func() {
		if giftLocalFileHash(t, path) != expectedHash || giftLocalFileHash(t, reportPath) != reportHash {
			t.Error("input changed")
		}
	})
	backup, err := openGiftRelevantBackup(path)
	if err != nil {
		t.Fatal(err)
	}
	defer backup.close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	m := &Monitor{storeDB: backup.db}
	data, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var saved struct {
		Report financeOperatingReport `json:"after_rebuild"`
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	r := saved.Report
	if r.From != 1777564800 || r.To != 1791475200 || len(r.PairingAudit.Sources) != 24 {
		t.Fatal("unexpected report scope")
	}
	scope := stabilityScope{FromTs: r.From, ToTs: r.To}
	rawSources, truncated, err := m.loadFinancePairingSources(ctx, scope, 24)
	if err != nil || truncated || len(rawSources) != 24 {
		t.Fatal("source inventory changed", err)
	}
	byRef := map[string]financePairingSourceView{}
	for _, source := range rawSources {
		byRef[source.SourceRef] = source
	}
	type candidate struct {
		ChannelID                                                                                          int    `json:"channel_id"`
		Status                                                                                             int    `json:"preview_status"`
		Error                                                                                              string `json:"preview_error,omitempty"`
		Hours, ActiveHours, Requests, LocalRequests, LocalFailed, TestRequests, TestFailed, QueueableHours int64
		Billed, ActiveBilled, InactiveBilled                                                               channelEconomicsMoneyView
		Warnings                                                                                           []string
	}
	type sourceResult struct {
		Source                       financePairingSourceView `json:"source"`
		VerifiedHours                int64                    `json:"verified_hours"`
		Candidates                   []candidate              `json:"candidates"`
		HistoricalOwnershipConfirmed bool                     `json:"historical_ownership_confirmed"`
	}
	var results []sourceResult
	counts := map[string]int{}
	var billed, requests int64
	for _, source := range r.PairingAudit.Sources {
		original := source
		original.CandidateState = ""
		original.CurrentConfigCandidates = nil
		if !reflect.DeepEqual(original, byRef[source.SourceRef]) {
			t.Fatal("accepted report does not match original source evidence", source.SourceRef)
		}
		var epochs []string
		if err := backup.db.WithContext(ctx).Model(&ChannelUpstreamCostHourEvidence{}).Where("domain=? AND source_ref=? AND semantics_version=? AND hour_ts>=? AND hour_ts<?", source.Domain, source.SourceRef, channelCostEvidenceSemanticsVersion, r.From, r.To).Distinct().Pluck("account_epoch", &epochs).Error; err != nil || len(epochs) != 1 {
			t.Fatal("ambiguous source epoch", err)
		}
		entry := sourceResult{Source: source, Candidates: []candidate{}}
		if err := backup.db.WithContext(ctx).Raw(`SELECT COUNT(DISTINCT e.hour_ts) FROM channel_upstream_cost_hour_evidence e
		JOIN channel_upstream_cost_hour_states s ON s.domain=e.domain AND s.account_epoch=e.account_epoch AND s.hour_ts=e.hour_ts AND s.semantics_version=e.semantics_version
		WHERE e.domain=? AND e.account_epoch=? AND e.source_ref=? AND e.semantics_version=? AND e.hour_ts>=? AND e.hour_ts<? AND s.status='verified' AND s.reconcile_status='matched'`, source.Domain, epochs[0], source.SourceRef, channelCostEvidenceSemanticsVersion, r.From, r.To).Scan(&entry.VerifiedHours).Error; err != nil {
			t.Fatal(err)
		}
		for _, hint := range source.CurrentConfigCandidates {
			from, to := source.FirstHour, source.LastHour+3600
			plan, status, err := m.planChannelCostHistoricalBinding(ctx, channelCostHistoricalBindingInput{Domain: source.Domain, AccountEpoch: epochs[0], SourceRef: source.SourceRef, LocalChannelID: hint.ChannelID, ValidFrom: &from, ValidTo: &to, Reason: "READ ONLY CANDIDATE DIAGNOSIS; NO OWNERSHIP CONFIRMATION"}, "local-audit-only")
			c := candidate{ChannelID: hint.ChannelID, Status: status}
			if err != nil {
				c.Error = err.Error()
			} else {
				c.Hours, c.ActiveHours, c.Requests, c.LocalRequests, c.LocalFailed, c.TestRequests, c.TestFailed, c.QueueableHours = plan.EvidenceHours, plan.LocalActiveHours, plan.EvidenceRequests, plan.LocalRequests, plan.LocalFailedRequests, plan.LocalTestRequests, plan.LocalTestFailedRequests, plan.WillQueueHours
				c.Billed, c.ActiveBilled, c.InactiveBilled, c.Warnings = plan.EvidenceBilledCost, plan.ActiveBilledCost, plan.InactiveBilledCost, plan.RiskWarnings
				if !reflect.DeepEqual(c.Billed, source.BilledCost) || c.Hours != source.EvidenceHours || c.Requests != source.Requests {
					t.Fatalf("preview/source mismatch for %s: cost %s/%s hours %d/%d requests %d/%d", source.SourceRef, c.Billed.MicroUSD, source.BilledCost.MicroUSD, c.Hours, source.EvidenceHours, c.Requests, source.Requests)
				}
			}
			entry.Candidates = append(entry.Candidates, c)
		}
		counts[source.CandidateState]++
		if _, err := financeAddMoney(&billed, source.BilledCost); err != nil {
			t.Fatal(err)
		}
		if err := addEconomicsInt64(&requests, source.Requests); err != nil {
			t.Fatal(err)
		}
		results = append(results, entry)
	}
	if billed != 20_963_642_434 || requests != 190721 || !reflect.DeepEqual(counts, map[string]int{"configured_unique": 13, "configured_partial": 1, "configured_ambiguous": 1, "no_configured_match": 9}) {
		t.Fatal("control totals changed", billed, requests, counts)
	}
	var bindings int64
	if err := backup.db.Model(&ChannelCostSourceBinding{}).Count(&bindings).Error; err != nil || bindings != 0 {
		t.Fatal("unexpected binding or mutation", err)
	}
	if err := backup.unchanged(); err != nil {
		t.Fatal(err)
	}
	result := map[string]any{"mode": "read_only_source_diagnosis_not_binding_plan", "source_sha256": expectedHash, "report_sha256": reportHash, "from": r.From, "to_exclusive": r.To, "sources": results, "candidate_counts": counts, "billed_micro_usd": billed, "requests": requests, "production_accessed": false, "historical_bindings_written": 0, "amounts_changed": false, "warning": "Current configuration and concurrent activity do not prove historical ownership; never auto-apply these candidates."}
	if out := os.Getenv("MONITOR_FINANCE_PAIRING_AUDIT_RESULT"); out != "" {
		encoded, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := giftLocalWriteNew(out, encoded); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("PASS: %d sources, %d requests, %d microUSD; no binding or money changes", len(results), requests, billed)
}
