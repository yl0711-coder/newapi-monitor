package monitor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"

	"fmt"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"strings"
	"testing"
	"time"
)

func TestTokenReviewReplayPartialIsIncomplete(t *testing.T) {
	m, now, path := tokenMeteringFixture(t)
	ctx := context.Background()
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	tokenMeteringRun(t, m, now)
	for i := 2; i <= 501; i++ {
		tokenMeteringSourceRow(t, m, int64(i), start+1, fmt.Sprint("late-", i))
	}
	if err := m.storeDB.Model(&TokenMeteringWindow{}).Where("source_epoch = ?", m.cfg.TokenFactsSourceEpoch).Update("next_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	lease, ok, err := m.claimTokenMetering(ctx, now+1)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	w, ok, err := m.nextTokenMeteringWindow(ctx, lease.SourceEpoch, lease.LeaseToken, now+1, true)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	rows, done, err := m.readTokenMeteringPage(ctx, w)
	if err != nil || done || len(rows) != tokenMeteringPageRows {
		t.Fatal(len(rows), done, err)
	}
	var facts []TokenMeteringFact
	var refs []TokenMeteringRef
	for _, r := range rows {
		f, rs := parseTokenMetering(m.cfg, r, now+1)
		facts = append(facts, f)
		refs = append(refs, rs...)
	}
	if err := m.commitTokenMeteringPage(ctx, lease.LeaseToken, w, rows, facts, refs, done, now+1); err != nil {
		t.Fatal(err)
	}
	var got TokenMeteringWindow
	if err := m.storeDB.First(&got, "source_epoch = ? AND start_at = ?", lease.SourceEpoch, start).Error; err != nil {
		t.Fatal(err)
	}
	state := tokenMeteringStateRead(t, m)
	if got.Done || got.SourceComplete || !got.Replay || state.CompleteThrough > start {
		t.Fatalf("partial replay claimed complete: window=%+v state=%+v", got, state)
	}
	replacement := tokenReviewReopen(t, m, path)
	next, ok, err := replacement.claimTokenMetering(ctx, now+31)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if err := m.commitTokenMeteringPage(ctx, lease.LeaseToken, w, rows, facts, refs, done, now+31); !errors.Is(err, errTokenMeteringLease) {
		t.Fatalf("old worker: %v", err)
	}
	resumed, ok, err := replacement.nextTokenMeteringWindow(ctx, next.SourceEpoch, next.LeaseToken, now+31, true)
	if err != nil || !ok || resumed.CursorID != tokenMeteringPageRows {
		t.Fatal(resumed, ok, err)
	}
	h, err := replacement.tokenMeteringHealth(ctx, "", now+31)
	if err != nil || h.State.CompleteThrough != start || h.PendingWindows != 1 {
		t.Fatal(h, err)
	}
	if err := replacement.storeDB.Model(&TokenMeteringState{}).Where("source_epoch = ?", next.SourceEpoch).Update("lease_until", 0).Error; err != nil {
		t.Fatal(err)
	}
	tokenMeteringRun(t, replacement, now+32)
	state = tokenMeteringStateRead(t, replacement)
	if state.CompleteThrough != start+300 || state.FactCount != 501 {
		t.Fatal(state)
	}
	if err := replacement.storeDB.First(&got).Error; err != nil || !got.Done || !got.SourceComplete || got.Replay {
		t.Fatal(got, err)
	}
}

func tokenReviewReopen(t *testing.T, m *Monitor, path string) *Monitor {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	return &Monitor{cfg: m.cfg, storeDB: db, prodDB: m.prodDB, nginxEvidenceDB: m.nginxEvidenceDB}
}

func TestTokenReviewRetentionMustNotResolveConflict(t *testing.T) {
	m, now, path := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	tokenMeteringRun(t, m, now)
	tokenMeteringEvidence(t, m, "older", start+1, 100, "one")
	tokenMeteringEvidence(t, m, "newer", start+2, 10000, "one")
	tokenMeteringLinkDue(t, m, now)
	before := tokenMeteringFactRead(t, m, 1)
	if before.LinkState != "conflict" {
		t.Fatal(before)
	}
	if err := m.nginxEvidenceDB.AutoMigrate(&NginxEvidenceIngestBatch{}); err != nil {
		t.Fatal(err)
	}
	m.cfg.NginxEvidenceRetentionHours = 24
	if err := m.pruneNginxEvidenceOnce(context.Background(), time.Unix(start+2+86400, 0)); err != nil {
		t.Fatal(err)
	}
	m = tokenReviewReopen(t, m, path)
	tokenMeteringLinkDue(t, m, now+60)
	after := tokenMeteringFactRead(t, m, 1)
	if after.LinkState != "conflict" || after.EvidenceLevel != "unavailable" || after.RequestLength != nil || after.EvidenceJSON != before.EvidenceJSON {
		t.Fatalf("retention erased known conflict: %+v", after)
	}
	if _, err := m.prodDB.Exec("UPDATE logs SET prompt_tokens=101 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	tokenMeteringReplay(t, m, now+61)
	tokenMeteringLinkDue(t, m, now+tokenMeteringRepairSeconds)
	after = tokenMeteringFactRead(t, m, 1)
	if after.LinkState != "conflict" || after.LinkNextAt != 0 || after.RequestLength != nil || after.EvidenceJSON != before.EvidenceJSON || after.EvidenceSnapshotAt != before.EvidenceSnapshotAt || after.Revision != 2 {
		t.Fatal(after)
	}
	var bytes int64
	if err := m.storeDB.Model(&TokenMeteringFact{}).Select("SUM(stored_bytes)").Scan(&bytes).Error; err != nil || bytes != tokenMeteringStateRead(t, m).FactBytes {
		t.Fatal(bytes, err)
	}
}

func TestTokenReviewExpressionCommentsCannotRetainText(t *testing.T) {
	for _, expr := range []string{"prompt_tokens /* PRIVATE_CANARY */ * 0.1", "prompt_tokens // PRIVATE_CANARY", "/* PRIVATE_CANARY */ prompt_tokens", "prompt_tokens /* 私密文本 */ + 1"} {
		t.Run(fmt.Sprintf("case-%x", len(expr)), func(t *testing.T) {
			raw, _ := json.Marshal(base64.StdEncoding.EncodeToString([]byte(expr)))
			got := tokenMeteringExpression(raw)
			if got.State != "invalid" || got.Value != "" || strings.Contains(got.Value, "PRIVATE_CANARY") {
				t.Fatalf("comment payload retained: %+v", got)
			}
		})
	}
}

func TestTokenReviewReplayFailureAndAtomicCompleteness(t *testing.T) {
	for _, failure := range []string{"first_page_source", "later_page_source", "last_page_commit", "failure_record"} {
		t.Run(failure, func(t *testing.T) {
			m, now, path := tokenMeteringFixture(t)
			ctx := context.Background()
			start := tokenMeteringStateRead(t, m).StartAt
			tokenMeteringSourceRow(t, m, 1, start+1, "one")
			tokenMeteringRun(t, m, now)
			for i := 2; i <= 501; i++ {
				tokenMeteringSourceRow(t, m, int64(i), start+1, fmt.Sprint("late-", i))
			}
			if err := m.storeDB.Model(&TokenMeteringWindow{}).Where("source_epoch = ?", m.cfg.TokenFactsSourceEpoch).Update("next_at", 0).Error; err != nil {
				t.Fatal(err)
			}
			lease, ok, err := m.claimTokenMetering(ctx, now+1)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			w, ok, err := m.nextTokenMeteringWindow(ctx, lease.SourceEpoch, lease.LeaseToken, now+1, true)
			if err != nil || !ok {
				t.Fatal(ok, err)
			}
			if state := tokenMeteringStateRead(t, m); state.CompleteThrough != start || state.ScanThrough != start+300 || w.Done || w.SourceComplete {
				t.Fatal(state, w)
			}
			if failure == "later_page_source" || failure == "last_page_commit" {
				rows, done, err := m.readTokenMeteringPage(ctx, w)
				if err != nil || done {
					t.Fatal(err)
				}
				var facts []TokenMeteringFact
				var refs []TokenMeteringRef
				for _, r := range rows {
					f, rs := parseTokenMetering(m.cfg, r, now+1)
					facts = append(facts, f)
					refs = append(refs, rs...)
				}
				if err := m.commitTokenMeteringPage(ctx, lease.LeaseToken, w, rows, facts, refs, done, now+1); err != nil {
					t.Fatal(err)
				}
			}
			if err := m.storeDB.Model(&TokenMeteringState{}).Where("source_epoch = ?", lease.SourceEpoch).Update("lease_until", 0).Error; err != nil {
				t.Fatal(err)
			}
			if failure == "last_page_commit" {
				if err := m.storeDB.Exec("CREATE TRIGGER fail_final BEFORE UPDATE ON token_metering_states WHEN NEW.complete_through > OLD.complete_through BEGIN SELECT RAISE(FAIL,'injected'); END").Error; err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := m.prodDB.Exec("ALTER TABLE logs RENAME TO unavailable"); err != nil {
					t.Fatal(err)
				}
				if failure == "failure_record" {
					if err := m.storeDB.Exec("CREATE TRIGGER fail_record BEFORE UPDATE ON token_metering_windows WHEN NEW.attempts > 0 BEGIN SELECT RAISE(FAIL,'injected'); END").Error; err != nil {
						t.Fatal(err)
					}
				}
			}
			if _, err := m.runTokenMeteringRound(ctx, time.Unix(now+2, 0)); err == nil {
				t.Fatal("failure swallowed")
			}
			m = tokenReviewReopen(t, m, path)
			state := tokenMeteringStateRead(t, m)
			if state.CompleteThrough != start || state.ScanThrough != start+300 {
				t.Fatal(state)
			}
			if err := m.storeDB.First(&w).Error; err != nil || w.Done || w.SourceComplete || !w.Replay {
				t.Fatal(w, err)
			}
			wantCount := int64(1)
			if failure == "later_page_source" || failure == "last_page_commit" {
				wantCount = tokenMeteringPageRows
			}
			if failure == "last_page_commit" {
				// Prefixes commit before the final transaction's completeness
				// publication is rejected. Its final rows and watermark must
				// both roll back; the earlier prefixes survive.
				wantCount = w.CursorID
				var actual, maxID int64
				countErr := m.storeDB.Raw("SELECT COUNT(*),MAX(log_id) FROM token_metering_facts WHERE source_epoch=?", state.SourceEpoch).Row().Scan(&actual, &maxID)
				if countErr != nil || wantCount <= tokenMeteringPageRows || wantCount >= 501 || actual != wantCount || maxID != wantCount {
					t.Fatal("failed final commit lost durable prefix", w.CursorID)
				}
			}
			if state.FactCount != wantCount {
				t.Fatal("partial transaction", state)
			}
			h, err := m.tokenMeteringHealth(ctx, "", now+3)
			if err != nil || h.State.CompleteThrough != start || h.PendingWindows != 1 {
				t.Fatal(h, err)
			}
			if failure == "last_page_commit" {
				if err := m.storeDB.Exec("DROP TRIGGER fail_final").Error; err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := m.prodDB.Exec("ALTER TABLE unavailable RENAME TO logs"); err != nil {
					t.Fatal(err)
				}
				if failure == "failure_record" {
					if err := m.storeDB.Exec("DROP TRIGGER fail_record").Error; err != nil {
						t.Fatal(err)
					}
				}
			}
			resumedAt := max(now+33, w.NextAt+1)
			tokenMeteringRun(t, m, resumedAt)
			if wantCount == 1 {
				// Repair has one reserved page per round. Its first recovered
				// page must remain incomplete until the next round reaches EOF.
				partial := tokenMeteringStateRead(t, m)
				if partial.FactCount != tokenMeteringPageRows || partial.CompleteThrough != start || partial.ScanThrough != start+300 {
					t.Fatal("first recovered replay page claimed complete", partial)
				}
				tokenMeteringRun(t, m, resumedAt+2)
			}
			state = tokenMeteringStateRead(t, m)
			if state.CompleteThrough != start+300 || state.FactCount != 501 {
				t.Fatal(state)
			}
		})
	}
}

func TestTokenReviewLegacyReplayHealthAndClaim(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringRun(t, m, now)
	if err := m.storeDB.Model(&TokenMeteringWindow{}).Where("source_epoch = ?", m.cfg.TokenFactsSourceEpoch).Update("replay", true).Error; err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenFactsEnabled = false
	h, err := m.tokenMeteringHealth(context.Background(), "", now)
	var windows int64
	if err := m.tokenStore().Model(&TokenMeteringWindow{}).Count(&windows).Error; err != nil {
		t.Fatal(err)
	}
	if err != nil || h.State.CompleteThrough != start || h.PendingWindows != windows {
		t.Fatal(h, err)
	}
	// A read-only health query must not persist normalization.
	if tokenMeteringStateRead(t, m).CompleteThrough != start+300 {
		t.Fatal("health wrote checkpoint")
	}
	state, ok, err := m.claimTokenMetering(context.Background(), now+1)
	if err != nil || !ok || state.CompleteThrough != start {
		t.Fatal(state, err)
	}
	if tokenMeteringStateRead(t, m).CompleteThrough != start {
		t.Fatal("claim did not reconcile")
	}
}

func TestTokenReviewConflictSnapshotWitnessAndStorageLimit(t *testing.T) {
	for _, limited := range []bool{false, true} {
		t.Run(fmt.Sprint(limited), func(t *testing.T) {
			m, now, _ := tokenMeteringFixture(t)
			start := tokenMeteringStateRead(t, m).StartAt
			tokenMeteringSourceRow(t, m, 1, start+1, "one")
			tokenMeteringRun(t, m, now)
			for i := 0; i < 25; i++ {
				size := int64(100)
				if i == 24 {
					size = 10000
				}
				tokenMeteringEvidence(t, m, fmt.Sprintf("event-%02d", i), start+1, size, "one")
			}
			baseline := tokenMeteringStateRead(t, m).FactBytes
			if limited {
				m.cfg.TokenFactsMaxBytes = baseline
			}
			tokenMeteringLinkDue(t, m, now)
			f := tokenMeteringFactRead(t, m, 1)
			if f.LinkState != "conflict" || f.RequestLength != nil {
				t.Fatal(f)
			}
			if limited {
				if !strings.Contains(f.LinkReason, "snapshot_storage_limit") || tokenMeteringStateRead(t, m).FactBytes != baseline {
					t.Fatal(f)
				}
			} else {
				var witnesses []tokenMeteringCandidate
				if err := json.Unmarshal([]byte(f.EvidenceJSON), &witnesses); err != nil || len(witnesses) > 20 {
					t.Fatal(witnesses, err)
				}
				if !strings.Contains(f.EvidenceJSON, "event-24") {
					t.Fatal("lost disagreement witness", f.EvidenceJSON)
				}
			}
			if err := m.nginxEvidenceDB.Where("event_id <> ?", "event-24").Delete(&NginxRequestEvidence{}).Error; err != nil {
				t.Fatal(err)
			}
			m.cfg.TokenFactsMaxBytes = baseline + 10000
			tokenMeteringLinkDue(t, m, now+60)
			if after := tokenMeteringFactRead(t, m, 1); after.LinkState != "conflict" || after.RequestLength != nil || after.EvidenceJSON != f.EvidenceJSON {
				t.Fatal(after)
			}
		})
	}
}

func TestTokenReviewExpressionPrivacyThroughPersistence(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	start := tokenMeteringStateRead(t, m).StartAt
	tokenMeteringSourceRow(t, m, 1, start+1, "one")
	for i, expr := range []string{"prompt_tokens /* PRIVATE_CANARY */ * 0.1", "prompt_tokens // PRIVATE_CANARY", "prompt_tokens + 1 /* PRIVATE_CANARY */"} {
		encoded := base64.StdEncoding.EncodeToString([]byte(expr))
		other := fmt.Sprintf(`{"billing_mode":"tiered_expr","request_rules":[],"expr_b64":%q}`, encoded)
		if _, err := m.prodDB.Exec("UPDATE logs SET other=? WHERE id=1", other); err != nil {
			t.Fatal(err)
		}
		tokenMeteringReplay(t, m, now+int64(i))
		f := tokenMeteringFactRead(t, m, 1)
		p := tokenMeteringDecode(t, f)
		if strings.Contains(f.SourceFacts, "PRIVATE_CANARY") || strings.Contains(f.SourceFacts, encoded) || p.Fields["expr_b64"].State != "invalid" || p.PricingSupport != "tiered_unsupported" {
			t.Fatal(f.SourceFacts)
		}
	}
	raw, _ := json.Marshal(base64.StdEncoding.EncodeToString([]byte("(prompt_tokens + completion_tokens) / 2")))
	if tokenMeteringExpression(raw).State != "valid" {
		t.Fatal("valid arithmetic rejected")
	}
}
