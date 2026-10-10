//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

// Opt-in acceptance of one real, unconfirmed candidate. No production client,
// Monitor workers, migrations, or external credential is constructed here.
const (
	attributionLocalDomain         = "4sapi.com"
	attributionLocalSource         = "bb2cbd335ad71b9c2149b6cdd3af476877d46ee8db0a0e9e5a0d3cde6a7cf1e2"
	attributionLocalSHA256         = "ed7255df0d569327b8867124c3ee4508ecfb6850233825ae3867971279e88bce"
	attributionLocalChannel        = 64
	financeLocalAttributionFailure = "local attribution manifest fault"
)

func TestChannelCostAttributionSealedLocalRehearsal(t *testing.T) {
	source := os.Getenv("MONITOR_ATTRIBUTION_ACCEPTANCE_DB")
	if source == "" {
		t.Skip("requires sealed local snapshot; never targets an existing or live database")
	}
	// Race instrumentation makes full-copy row hashing much slower than the
	// bounded publication itself. This is a test-only ceiling, not a runtime
	// database or worker timeout; the acceptance path has no external clients.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	db, output := openAttributionLocalCopy(t, source)
	db = db.WithContext(ctx)
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{attributionLocalDomain}}}
	var account ChannelUpstreamAccount
	if err := db.Select("domain", "provider", "base_url", "user_id", "account").First(&account, "domain=?", attributionLocalDomain).Error; err != nil {
		t.Fatal(err)
	}
	plan, status, err := m.planChannelCostHistoricalBinding(ctx, channelCostHistoricalBindingInput{
		Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), SourceRef: attributionLocalSource,
		LocalChannelID: attributionLocalChannel, Reason: "UNCONFIRMED LOCAL REHEARSAL ONLY; historical ownership not authorized",
	}, "local-acceptance-only")
	if err != nil || status != 200 || plan.EvidenceHours != 11 || plan.EvidenceRequests != 43 || plan.LocalActiveHours != 11 || plan.LocalRequests != 43 || plan.LocalTestRequests != 0 {
		t.Fatalf("candidate facts differ from inspected snapshot: status=%d err=%v", status, err)
	}
	hours := attributionEvidenceHours(t, db, account)
	beforePairing := attributionLocalPairingRows(ctx, t, db, hours)
	protected := attributionProtectedQueries(t, db, account, hours)
	beforeProtected := attributionQueryDigests(ctx, t, db, protected)
	beforeTotals := attributionHourTotals(t, db, account, hours)
	beforeTarget := attributionTargetRows(t, db, account, hours)
	t.Log("sealed baseline and scope digests captured")
	for _, row := range beforeTarget {
		if row.UpstreamChargeUnits != 0 || row.ProfitKnown || row.CoverageStatus != "upstream_cost_missing" {
			t.Fatal("candidate is not the expected unallocated baseline")
		}
	}
	checkAttributionLocalBindingRetry(ctx, t, m, plan.Binding, hours)
	bindingQuery := attributionLocalQuery{name: "created_candidate_binding", sql: "SELECT * FROM channel_cost_source_bindings ORDER BY rowid"}
	protected = append(protected, bindingQuery)
	beforeProtected[bindingQuery.name] = attributionQueryDigest(ctx, t, db, bindingQuery)
	checkAttributionLocalRollback(ctx, t, m, account, hours[0])
	t.Log("duplicate binding and injected manifest rollback verified")
	now := time.Now().Unix() + 120 // Pass only the synthetic failed job's retry deadline.
	for i := 0; i < 2; i++ {
		if err := m.publishDueChannelEconomicsBatch(ctx, account, now); err != nil {
			t.Fatal("bounded local queue failed", err)
		}
	}
	if got := attributionRowCount(t, db, "channel_economics_dirty_hours"); got != 0 {
		t.Fatal("candidate queue was not drained", got)
	}
	afterTotals := attributionHourTotals(t, db, account, hours)
	if !reflect.DeepEqual(beforeTotals, afterTotals) {
		t.Fatal("attribution changed total local revenue, original charges, or corrected cost")
	}
	afterTarget := verifyAttributionLocalTarget(t, db, account, hours, beforeTarget)
	afterPairing := attributionLocalPairingRows(ctx, t, db, hours)
	verifyAttributionLocalPairingChange(t, beforePairing, afterPairing)
	t.Log("bounded rebuild, money conservation and report progress verified")
	if got := attributionQueryDigests(ctx, t, db, protected); !reflect.DeepEqual(beforeProtected, got) {
		for name, before := range beforeProtected {
			if before != got[name] {
				t.Errorf("out-of-scope or immutable rows changed: %s", name)
			}
		}
		t.Fatal("local attribution crossed its allowed scope")
	}
	checkAttributionLocalPublicationRetry(ctx, t, m, account, hours, now)
	t.Log("protected rows and idempotent publication retry verified")
	var integrity string
	if err := db.Raw("PRAGMA quick_check").Scan(&integrity).Error; err != nil || integrity != "ok" {
		t.Fatal("rehearsal integrity check failed", err)
	}
	if giftLocalFileHash(t, source) != attributionLocalSHA256 {
		t.Fatal("original sealed snapshot changed")
	}
	receipt := map[string]any{
		"mode": "unconfirmed_local_attribution_rehearsal", "source_sha256": attributionLocalSHA256,
		"channel_id": attributionLocalChannel, "source_ref": attributionLocalSource,
		"hours": hours, "requests": 43, "before": beforeTarget, "after": afterTarget,
		"closed_channel_hours": 11, "closed_domain_hours": 1, "remaining_incomplete_domain_hours": 10,
		"totals_before": beforeTotals, "totals_after": afterTotals,
		"report_pairing_before": beforePairing, "report_pairing_after": afterPairing,
		"protected_queries": len(protected), "scope_unchanged": true,
		"old_publications_preserved": true, "duplicate_binding_rejected": true,
		"publication_retry_noop": true, "manifest_failure_rolled_back": true,
		"queue_drained": true, "original_snapshot_unchanged": true,
		"historical_ownership_confirmed": false, "production_applicable": false,
		"notes": []string{"Uses the earlier unconfirmed recharge-ratio rehearsal without changing its ratios.",
			"The single candidate is assumed only inside this new isolated copy; it is not authorized historical attribution.",
			"Other unallocated sources remain unknown; this does not publish full upstream or platform profit."},
	}
	encoded, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := giftLocalWriteNew(filepath.Join(output, "acceptance.json"), encoded); err != nil {
		t.Fatal(err)
	}
	t.Logf("PASS: 11 hours / 43 requests; conserved totals, retry/rollback, %d protected queries; output %s", len(protected), output)
}

func openAttributionLocalCopy(t *testing.T, source string) (*gorm.DB, string) {
	t.Helper()
	return openAttributionLocalCopyAt(t, source, os.Getenv("MONITOR_ATTRIBUTION_ACCEPTANCE_OUTPUT"))
}

func openAttributionLocalCopyAt(t *testing.T, source, output string) (*gorm.DB, string) {
	t.Helper()
	return openAttributionLocalCopyWithHashAt(t, source, output, attributionLocalSHA256)
}

func openAttributionLocalCopyWithHashAt(t *testing.T, source, output, expectedHash string) (*gorm.DB, string) {
	t.Helper()
	backup, err := openGiftRelevantBackup(source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(backup.close)
	if giftLocalFileHash(t, source) != expectedHash {
		t.Fatal("sealed rehearsal source hash mismatch")
	}
	if output == "" {
		private := t.TempDir()
		if err := os.Chmod(private, 0700); err != nil {
			t.Fatal(err)
		}
		output = filepath.Join(private, "unconfirmed-attribution")
	}
	if !filepath.IsAbs(output) {
		t.Fatal("output must be an absolute new private directory")
	}
	parent, err := os.Lstat(filepath.Dir(output))
	if err != nil || !parent.IsDir() || parent.Mode()&os.ModeSymlink != 0 || parent.Mode().Perm()&0077 != 0 {
		t.Fatal("output parent must be an existing private regular directory")
	}
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(output, "monitor-unconfirmed.db")
	if hash, err := giftLocalCopyBackup(source, destination); err != nil || hash != expectedHash {
		t.Fatal("copy failed", err)
	}
	if err := backup.unchanged(); err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := giftLocalDatabase(destination)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeDB)
	if err := db.Exec("PRAGMA journal_mode=DELETE").Error; err != nil {
		t.Fatal(err)
	}
	if attributionRowCount(t, db, "channel_cost_source_bindings") != 0 || attributionRowCount(t, db, "channel_economics_dirty_hours") != 0 {
		t.Fatal("rehearsal requires the inspected empty binding and queue baseline")
	}
	return db, output
}

func checkAttributionLocalBindingRetry(ctx context.Context, t *testing.T, m *Monitor, binding ChannelCostSourceBinding, hours []int64) {
	t.Helper()
	if err := m.saveCostSourceBinding(ctx, binding); err != nil {
		t.Fatal(err)
	}
	var queued []int64
	if err := m.storeDB.Model(&ChannelEconomicsDirtyHour{}).Order("hour_ts").Pluck("hour_ts", &queued).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(hours, queued) {
		t.Fatal("binding queued hours outside the exact observed source", queued)
	}
	var saved ChannelCostSourceBinding
	if err := m.storeDB.First(&saved).Error; err != nil {
		t.Fatal(err)
	}
	if saved != binding || saved.Status != "confirmed" {
		t.Fatal("saved rehearsal binding differs from its finite plan")
	}
	queries := []attributionLocalQuery{
		{name: "binding", sql: "SELECT * FROM channel_cost_source_bindings ORDER BY rowid"},
		{name: "queue", sql: "SELECT * FROM channel_economics_dirty_hours ORDER BY rowid"},
	}
	beforeRetry := attributionQueryDigests(ctx, t, m.storeDB, queries)
	if err := m.saveCostSourceBinding(ctx, binding); err == nil || !strings.Contains(err.Error(), "重叠") {
		t.Fatal("duplicate binding did not fail closed", err)
	}
	if !reflect.DeepEqual(beforeRetry, attributionQueryDigests(ctx, t, m.storeDB, queries)) {
		t.Fatal("rejected binding retry mutated the binding or queued work")
	}
	if attributionRowCount(t, m.storeDB, "channel_cost_source_bindings") != 1 || attributionRowCount(t, m.storeDB, "channel_economics_dirty_hours") != int64(len(hours)) {
		t.Fatal("duplicate binding changed durable state")
	}
}

func checkAttributionLocalRollback(ctx context.Context, t *testing.T, m *Monitor, account ChannelUpstreamAccount, hour int64) {
	t.Helper()
	queries := attributionMutablePublicationQueries()
	before := attributionQueryDigests(ctx, t, m.storeDB, queries)
	if err := m.storeDB.Exec(`CREATE TEMP TRIGGER fail_local_attribution_manifest BEFORE INSERT ON channel_economics_hour_manifest_publications
		BEGIN SELECT RAISE(ABORT,'local attribution manifest fault'); END`).Error; err != nil {
		t.Fatal(err)
	}
	err := m.publishOneDueChannelEconomicsHourOrdered(ctx, account, time.Now().Unix()+1, true)
	if err == nil || !strings.Contains(err.Error(), financeLocalAttributionFailure) {
		t.Fatal("injected manifest failure was not observed", err)
	}
	if err := m.storeDB.Exec("DROP TRIGGER fail_local_attribution_manifest").Error; err != nil {
		t.Fatal(err)
	}
	if after := attributionQueryDigests(ctx, t, m.storeDB, queries); !reflect.DeepEqual(before, after) {
		t.Fatal("failed publication left partial children or advanced pointers")
	}
	var dirty ChannelEconomicsDirtyHour
	if err := m.storeDB.First(&dirty, "domain=? AND hour_ts=?", account.Domain, hour).Error; err != nil || dirty.Attempts != 1 || dirty.LastError == "" {
		t.Fatal("failed work was lost instead of retained for retry", err)
	}
}

func checkAttributionLocalPublicationRetry(ctx context.Context, t *testing.T, m *Monitor, account ChannelUpstreamAccount, hours []int64, now int64) {
	t.Helper()
	queries := attributionMutablePublicationQueries()
	before := attributionQueryDigests(ctx, t, m.storeDB, queries)
	for _, hour := range hours {
		if err := m.publishChannelEconomicsHour(ctx, account, hour, "local_duplicate_retry", now+1); err != nil {
			t.Fatal(err)
		}
	}
	if after := attributionQueryDigests(ctx, t, m.storeDB, queries); !reflect.DeepEqual(before, after) {
		t.Fatal("identical publication retry changed the immutable ledger")
	}
}
