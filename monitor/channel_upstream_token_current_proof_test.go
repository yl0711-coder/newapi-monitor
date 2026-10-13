package monitor

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func tokenPublishedSnapshot(t *testing.T, f tokenReconcileFixture) string {
	t.Helper()
	var all [][]map[string]any
	for _, model := range []any{&ChannelEconomicsHourPublication{}, &ChannelEconomicsHourCurrent{},
		&ChannelEconomicsHourManifestPublication{}, &ChannelEconomicsHourManifestCurrent{},
		&ChannelEconomicsGlobalHourFact{}, &ChannelEconomicsDirtyHour{},
		&ChannelUpstreamCostHourEvidence{}, &ChannelUpstreamCostHourState{}} {
		var rows []map[string]any
		if err := f.m.storeDB.Model(model).Order("rowid").Find(&rows).Error; err != nil {
			t.Fatal(err)
		}
		all = append(all, rows)
	}
	b, err := json.Marshal(all)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func tokenEconomicsRescan(t *testing.T, f tokenReconcileFixture, items []sub2PricingUsageItem, now int64) ChannelUpstreamTokenScanCheckpoint {
	t.Helper()
	ctx := context.Background()
	before, err := readSub2TokenStoredCursor(f.m.storeDB, f.account, f.day)
	if err != nil {
		t.Fatal(err)
	}
	current, err := f.m.resetSub2TokenScanCheckpoint(ctx, f.account, f.day, before.Revision, now, f.key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, page := range []sub2PricingPage{sub2TokenPage(items[:2], 1), sub2TokenPage(items[2:], 2)} {
		current, err = f.m.saveSub2TokenScanPage(ctx, f.account, f.day, page, current.Revision, now, f.key, "v1")
		if err != nil {
			t.Fatal(err)
		}
	}
	current, err = f.m.recordSub2TokenObservation(ctx, f.account, f.day, current, sub2ObservationLegacy(t, f.account, items, f.day), now, f.key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	return current
}

func TestSub2TokenEconomicsCurrentProofInvalidKeepsAllPublishedState(t *testing.T) {
	for _, kind := range []string{"missing", "revoked", "restarted", "one-scan", "hash", "key", "future", "unclosed", "corrupt", "sql-error"} {
		for _, window := range []string{"nonempty", "empty"} {
			t.Run(kind+"/"+window, func(t *testing.T) {
				f, hour := sub2TokenEconomicsFixture(t, 1, 1)
				if window == "empty" {
					hour = f.day + 9*3600
				}
				ctx := context.Background()
				if err := f.m.publishSub2TokenEconomicsHour(ctx, f.account, hour, "before", f.day+86405); err != nil {
					t.Fatal(err)
				}
				before := tokenPublishedSnapshot(t, f)
				exec := func(query string, args ...any) {
					t.Helper()
					if err := f.m.storeDB.Exec(query, args...).Error; err != nil {
						t.Fatal(err)
					}
				}
				switch kind {
				case "missing":
					exec("DELETE FROM channel_upstream_token_scan_checkpoints")
				case "revoked":
					if _, err := f.m.invalidateSub2TokenObservation(ctx, f.account, f.day, f.cursor, f.day+86406); err != nil {
						t.Fatal(err)
					}
				case "restarted":
					if _, err := f.m.resetSub2TokenScanCheckpoint(ctx, f.account, f.day, f.cursor.Revision, f.day+86406, f.key, "v1"); err != nil {
						t.Fatal(err)
					}
				case "one-scan":
					exec("UPDATE channel_upstream_token_scan_checkpoints SET stable_scans=1")
				case "hash":
					exec("UPDATE channel_upstream_token_scan_checkpoints SET success_hash='wrong'")
				case "key":
					exec("UPDATE channel_upstream_token_scan_checkpoints SET success_key_id='another-key'")
				case "future":
					exec("UPDATE channel_upstream_token_scan_checkpoints SET success_at=?", f.day+86408)
				case "unclosed":
					exec("UPDATE channel_upstream_token_scan_checkpoints SET success_at=?", f.day+86399)
				case "corrupt":
					exec("UPDATE channel_upstream_token_scan_checkpoints SET envelope='corrupt'")
				case "sql-error":
					exec("DROP TABLE channel_upstream_token_scan_checkpoints")
				}
				if err := f.m.publishSub2TokenEconomicsHour(ctx, f.account, hour, "must block", f.day+86407); err == nil {
					t.Fatal("invalid current proof accepted")
				}
				if after := tokenPublishedSnapshot(t, f); after != before {
					t.Fatal("failed publication changed cost, reports, current pointers or queues")
				}
			})
		}
	}
}

func TestSub2TokenEconomicsCurrentProofRecoveryNoReportChurn(t *testing.T) {
	f, hour := sub2TokenEconomicsFixture(t, 2, 7)
	ctx := context.Background()
	if err := f.m.publishSub2TokenEconomicsHour(ctx, f.account, hour, "before", f.day+86405); err != nil {
		t.Fatal(err)
	}
	before := tokenPublishedSnapshot(t, f)
	if _, err := f.m.invalidateSub2TokenObservation(ctx, f.account, f.day, f.cursor, f.day+86406); err != nil {
		t.Fatal(err)
	}
	_, items, _, _ := sub2TokenFixture(t)
	for scan := 1; scan <= 2; scan++ {
		cursor := tokenEconomicsRescan(t, f, items, f.day+86406+int64(scan))
		if cursor.StableScans != scan {
			t.Fatal("incorrect recovered scan count", cursor.StableScans)
		}
		err := f.m.publishSub2TokenEconomicsHour(ctx, f.account, hour, "recovery", f.day+86409)
		if (scan == 1 && err == nil) || (scan == 2 && err != nil) {
			t.Fatal("recovery gate", scan, err)
		}
		if tokenPublishedSnapshot(t, f) != before {
			t.Fatal("identical proof recovery churned monetary state")
		}
	}
}

func TestSub2TokenEconomicsCurrentProofTokenMoveNeedsNewCosts(t *testing.T) {
	f, hour := sub2TokenEconomicsFixture(t, 1, 1)
	ctx := context.Background()
	if err := f.m.publishSub2TokenEconomicsHour(ctx, f.account, hour, "before", f.day+86405); err != nil {
		t.Fatal(err)
	}
	before := tokenPublishedSnapshot(t, f)
	_, items, _, _ := sub2TokenFixture(t)
	items[0].UpstreamTokenID = "574" // same hour/group/model/amount, different token
	var cursor ChannelUpstreamTokenScanCheckpoint
	for i := int64(0); i < 2; i++ {
		cursor = tokenEconomicsRescan(t, f, items, f.day+86406+i)
	}
	if cursor.StableScans != 2 || cursor.SuccessHash == f.cursor.SuccessHash {
		t.Fatal("changed-token fixture did not converge")
	}
	if err := f.m.publishSub2TokenEconomicsHour(ctx, f.account, hour, "stale costs", f.day+86408); err == nil {
		t.Fatal("same account total hid changed token attribution")
	}
	if tokenPublishedSnapshot(t, f) != before {
		t.Fatal("blocked changed-token proof altered old report")
	}
	// A different hour whose exact token content is unchanged remains usable.
	if err := f.m.publishSub2TokenEconomicsHour(ctx, f.account, f.day+11*3600, "unchanged hour", f.day+86408); err != nil {
		t.Fatal("unaffected hour incorrectly blocked", err)
	}
	in := sub2TokenCostCommitInput{Domain: f.account.Domain, ChannelID: 185, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), ConfigRevision: 1, DayTs: f.day, CursorRevision: cursor.Revision, SuccessHash: cursor.SuccessHash}
	result, err := f.m.commitSub2TokenCostCandidates(ctx, in, f.day+86409)
	if err != nil || result.UpdatedHours != 1 || result.UnchangedHours != 23 {
		t.Fatal("unexpected changed-hour cost rewrite", result, err)
	}
	if err := f.m.publishSub2TokenEconomicsHour(ctx, f.account, hour, "fresh costs", f.day+86410); err != nil {
		t.Fatal(err)
	}
	var current []ChannelEconomicsHourPublication
	if err := f.m.storeDB.Raw(`SELECT p.* FROM channel_economics_hour_publications p JOIN channel_economics_hour_current c ON p.publication_id=c.publication_id WHERE p.hour_ts=? ORDER BY p.local_channel_id`, hour).Scan(&current).Error; err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, row := range current {
		total += row.UpstreamChargeUnits
		if row.LocalChannelID == 185 && row.UpstreamChargeUnits != 0 {
			t.Fatal("old token cost survived in current channel report")
		}
	}
	if len(current) != 2 || total != 358023 || current[1].LocalChannelID != 186 || current[1].UpstreamChargeUnits != total {
		t.Fatal("token move changed account total or duplicated cost", current)
	}
}

func TestSub2TokenEconomicsCurrentProofConcurrentInvalidationRollsBack(t *testing.T) {
	f, hour := sub2TokenEconomicsFixture(t, 1, 1)
	path := filepath.Join(t.TempDir(), "concurrent-proof.db")
	if err := f.m.storeDB.Exec("VACUUM INTO ?", path).Error; err != nil {
		t.Fatal(err)
	}
	open := func() *gorm.DB {
		db, err := gorm.Open(sqlite.Open(path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(50)"), &gorm.Config{})
		if err != nil {
			t.Fatal(err)
		}
		sqlDB, err := db.DB()
		if err != nil {
			t.Fatal(err)
		}
		sqlDB.SetMaxOpenConns(1)
		t.Cleanup(func() { _ = sqlDB.Close() })
		return db
	}
	f.m = &Monitor{cfg: f.m.cfg, storeDB: open()}
	writer := &Monitor{cfg: f.m.cfg, storeDB: open()}
	before := tokenPublishedSnapshot(t, f)
	read, proceed := make(chan struct{}), make(chan struct{})
	var signal, release sync.Once
	defer release.Do(func() { close(proceed) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Pause after the publication transaction has read the old proof. A
	// separate SQLite connection then commits invalidation before any report
	// write. WAL must reject promotion of the stale read snapshot to a writer.
	if err := f.m.storeDB.Callback().Query().After("gorm:query").Register("test:pause_token_proof", func(tx *gorm.DB) {
		if tx.Statement.Table == "channel_upstream_token_scan_checkpoints" {
			signal.Do(func() {
				close(read)
				select {
				case <-proceed:
				case <-ctx.Done():
				}
			})
		}
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- f.m.publishSub2TokenEconomicsHour(ctx, f.account, hour, "concurrent", f.day+86407) }()
	select {
	case <-read:
	case err := <-done:
		t.Fatal("publisher did not read proof", err)
	case <-ctx.Done():
		t.Fatal("publisher did not reach proof barrier")
	}
	if _, err := writer.invalidateSub2TokenObservation(ctx, f.account, f.day, f.cursor, f.day+86406); err != nil {
		t.Fatal(err)
	}
	release.Do(func() { close(proceed) })
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("published from a stale SQLite snapshot after invalidation committed")
		}
	case <-ctx.Done():
		t.Fatal("publication did not finish")
	}
	if tokenPublishedSnapshot(t, f) != before {
		t.Fatal("concurrent invalidation left partial monetary writes")
	}
}
