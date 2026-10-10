package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func newCloudWatchNginxRecoveryTestMonitor(t *testing.T) (*Monitor, time.Time) {
	t.Helper()
	m := newTestMonitor(t)
	t.Cleanup(m.Close)
	m.cfg.CloudWatchNginxEnabled = true
	m.cfg.CloudWatchNginxLookbackHours = 168
	m.cfg.NginxEvidenceMode = "verified"
	m.cfg.NginxEvidenceRetentionHours = 168
	m.nginxEvidenceDB = newCloudWatchNginxRecoveryTestDB(t)
	return m, time.Now().UTC().Truncate(time.Minute)
}

func newCloudWatchNginxRecoveryTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "evidence.db")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&NginxRequestEvidence{}, &NginxEvidenceIngestBatch{}, &CloudWatchNginxEvidenceCheckpoint{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}

func completeEmptyCloudWatchNginxEvidence(t *testing.T, m *Monitor, now time.Time) CloudWatchNginxCursor {
	t.Helper()
	state, err := m.loadCloudWatchNginxCursor(now)
	if err != nil {
		t.Fatal(err)
	}
	from, target := cloudWatchNginxEvidenceRange(now, m.cfg.NginxEvidenceRetentionHours)
	if err := m.persistCloudWatchNginxRequestEvidence(context.Background(), nil, from, target, "empty-retention-window"); err != nil {
		t.Fatal(err)
	}
	if err := m.publishCloudWatchNginxEvidenceCursor(context.Background(), &state, from, target, target); err != nil {
		t.Fatal(err)
	}
	return state
}

func TestCloudWatchNginxEvidenceRestoredMainWithEmptyVolumeReplaysRetention(t *testing.T) {
	m, now := newCloudWatchNginxRecoveryTestMonitor(t)
	old := completeEmptyCloudWatchNginxEvidence(t, m, now)
	old.NextTs, old.ThroughTs, old.Status = old.TargetThroughTs, old.TargetThroughTs, "caught_up"
	if err := m.storeDB.Save(&old).Error; err != nil {
		t.Fatal(err)
	}
	// The restored main DB still advertises complete evidence, but the newly
	// attached volume contains neither events nor the original commit proof.
	m.nginxEvidenceDB = newCloudWatchNginxRecoveryTestDB(t)
	m.cloudWatchNginxEvidenceRecoveryCache.Store(nil)
	m.cloudWatchNginxEvidenceFrom.Store(old.EvidenceCoverageFromTs)
	m.cloudWatchNginxEvidenceThrough.Store(old.EvidenceThroughTs)
	ready, _ := m.readyStatus(now)
	if !ready.CloudWatchNginx.EvidenceRecovery.Incomplete || ready.CloudWatchNginx.EvidenceThroughTs != 0 ||
		ready.Collectors["nginx_evidence"].ThroughTs != 0 {
		t.Fatalf("startup trusted stale main-store evidence watermark: %+v", ready.CloudWatchNginx)
	}
	got, err := m.loadCloudWatchNginxCursor(now)
	if err != nil {
		t.Fatal(err)
	}
	from, target := cloudWatchNginxEvidenceRange(now, m.cfg.NginxEvidenceRetentionHours)
	if got.EvidenceStoreID == old.EvidenceStoreID || got.EvidenceNextTs != from || got.EvidenceThroughTs != from ||
		got.EvidenceStatus != "recovering" || got.EvidenceGapFromTs != from || got.EvidenceGapToTs != target ||
		got.EvidenceGapReason != "evidence_store_replaced" || got.NextTs != old.NextTs || got.ThroughTs != old.ThroughTs {
		t.Fatalf("replacement volume did not reset only evidence coverage: %+v", got)
	}
	if got := m.cloudWatchNginxEvidenceRecovery(now); !got.Verified || !got.Incomplete || got.DetectedAt != now.Unix() {
		t.Fatalf("recovery gap not explicitly exposed: %+v", got)
	}
	got = completeEmptyCloudWatchNginxEvidence(t, m, now)
	status := m.cloudWatchNginxEvidenceRecovery(now)
	if status.Incomplete || status.DetectedAt != now.Unix() || status.Reason != "evidence_store_replaced" || got.EvidenceStatus != "caught_up" {
		t.Fatalf("recovery must close gap but retain audit history: state=%+v recovery=%+v", got, status)
	}
}

func TestCloudWatchNginxEvidenceCompleteEmptyWindowSurvivesRestart(t *testing.T) {
	m, now := newCloudWatchNginxRecoveryTestMonitor(t)
	old := completeEmptyCloudWatchNginxEvidence(t, m, now)
	var count int64
	if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("test must cover legitimate empty evidence window: count=%d err=%v", count, err)
	}
	got, err := m.loadCloudWatchNginxCursor(now)
	if err != nil {
		t.Fatal(err)
	}
	if got.EvidenceThroughTs != old.EvidenceThroughTs || got.EvidenceStoreID != old.EvidenceStoreID ||
		got.EvidenceStatus != "caught_up" || got.EvidenceGapDetectedAt != 0 {
		t.Fatalf("valid empty-window proof was lost: %+v", got)
	}
}

func TestCloudWatchNginxEvidenceCannotPublishBeforeDurableProof(t *testing.T) {
	m, now := newCloudWatchNginxRecoveryTestMonitor(t)
	state, err := m.loadCloudWatchNginxCursor(now)
	if err != nil {
		t.Fatal(err)
	}
	from, target := cloudWatchNginxEvidenceRange(now, m.cfg.NginxEvidenceRetentionHours)
	if err := m.publishCloudWatchNginxEvidenceCursor(context.Background(), &state, from, target, target); err == nil {
		t.Fatal("main evidence watermark advanced without durable complete-window proof")
	}
	var stored CloudWatchNginxCursor
	if err := m.storeDB.First(&stored, "id = ?", cloudWatchNginxCursorID).Error; err != nil {
		t.Fatal(err)
	}
	if state.EvidenceThroughTs != from || stored.EvidenceThroughTs != from {
		t.Fatalf("failed publication changed evidence watermark: state=%+v stored=%+v", state, stored)
	}
}

func TestCloudWatchNginxEvidenceLegacyWatermarkWithoutProofReplays(t *testing.T) {
	m, now := newCloudWatchNginxRecoveryTestMonitor(t)
	from, target := cloudWatchNginxEvidenceRange(now, m.cfg.NginxEvidenceRetentionHours)
	legacy := CloudWatchNginxCursor{ID: cloudWatchNginxCursorID, SemanticsVersion: cloudWatchNginxVersion,
		CoverageFromTs: from, NextTs: target, ThroughTs: target, Status: "caught_up",
		EvidenceCoverageFromTs: from, EvidenceThroughTs: target, EvidenceNextTs: target, EvidenceStatus: "caught_up"}
	if err := m.storeDB.Save(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	// One surviving row is not proof that all retained windows survived.
	if err := m.nginxEvidenceDB.Create(&NginxRequestEvidence{EventID: "legacy", Node: cloudWatchNginxNode, EventMS: from * 1000}).Error; err != nil {
		t.Fatal(err)
	}
	got, err := m.loadCloudWatchNginxCursor(now)
	if err != nil {
		t.Fatal(err)
	}
	if got.EvidenceNextTs != from || got.EvidenceStatus != "recovering" || got.EvidenceGapReason != "evidence_coverage_unverified" {
		t.Fatalf("legacy unverified watermark was trusted: %+v", got)
	}
}

func TestCloudWatchNginxEvidenceOlderSnapshotAndInterruptedReplaceAreIncomplete(t *testing.T) {
	for _, test := range []string{"older_evidence_snapshot", "interrupted_replace"} {
		t.Run(test, func(t *testing.T) {
			m, now := newCloudWatchNginxRecoveryTestMonitor(t)
			state := completeEmptyCloudWatchNginxEvidence(t, m, now)
			from := state.EvidenceThroughTs - 3600
			if test == "older_evidence_snapshot" {
				if err := m.nginxEvidenceDB.Model(&CloudWatchNginxEvidenceCheckpoint{}).Where("id = ?", cloudWatchNginxCursorID).
					Update("through_ts", from).Error; err != nil {
					t.Fatal(err)
				}
			} else if err := m.nginxEvidenceDB.Transaction(func(tx *gorm.DB) error {
				return beginCloudWatchNginxEvidenceReplace(tx, from, state.EvidenceThroughTs)
			}); err != nil {
				t.Fatal(err)
			}
			m.refreshCloudWatchNginxEvidenceRecovery(now)
			if got := m.cloudWatchNginxEvidenceRecovery(now); !got.Incomplete || got.Verified {
				t.Fatalf("incomplete evidence DB accepted before restart: %+v", got)
			}
			var got CloudWatchNginxCursor
			var err error
			if test == "interrupted_replace" {
				// Recovery must happen in the running worker too, not require a
				// restart after an interrupted multi-transaction replacement.
				got = state
				err = m.refreshCloudWatchNginxEvidenceCursor(context.Background(), &got, now)
			} else {
				got, err = m.loadCloudWatchNginxCursor(now)
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.EvidenceThroughTs != state.EvidenceCoverageFromTs || got.EvidenceStatus != "recovering" {
				t.Fatalf("incomplete evidence DB not scheduled for recovery: %+v", got)
			}
		})
	}
}

func TestReadyEvidenceCoverageUsesAtomicProofWithoutDatabaseQueries(t *testing.T) {
	m, now := newCloudWatchNginxRecoveryTestMonitor(t)
	state := completeEmptyCloudWatchNginxEvidence(t, m, now)
	m.cloudWatchNginxEvidenceFrom.Store(state.EvidenceCoverageFromTs)
	m.cloudWatchNginxEvidenceThrough.Store(state.EvidenceThroughTs)
	var proofQueries atomic.Int64
	guard := func(tx *gorm.DB) {
		switch tx.Statement.Dest.(type) {
		case *CloudWatchNginxCursor, *CloudWatchNginxEvidenceCheckpoint:
			proofQueries.Add(1)
			_ = tx.AddError(errors.New("HTTP readiness must not read evidence proof from SQLite"))
		}
	}
	for _, db := range []*gorm.DB{m.storeDB, m.nginxEvidenceDB} {
		if err := db.Callback().Query().Before("gorm:query").Register("test:no_http_evidence_probe", guard); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 10; i++ {
		ready, _ := m.readyStatus(now)
		if ready.CloudWatchNginx.EvidenceRecovery.Incomplete || ready.CloudWatchNginx.EvidenceThroughTs != state.EvidenceThroughTs {
			t.Fatalf("readiness did not reuse checked atomic proof: %+v", ready.CloudWatchNginx)
		}
	}
	if got := proofQueries.Load(); got != 0 {
		t.Fatalf("/ready read evidence SQLite %d times", got)
	}
}

func TestCloudWatchNginxEvidenceMinuteBoundaryExpiresProofButPreservesRecoveryHistory(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	m := &Monitor{cfg: Settings{CloudWatchNginxEnabled: true, NginxEvidenceMode: "verified", NginxEvidenceRetentionHours: 168}}
	from, target := cloudWatchNginxEvidenceRange(now, m.cfg.NginxEvidenceRetentionHours)
	cached := cloudWatchNginxEvidenceRecoveryStatus{Verified: true, ThroughTs: from, Incomplete: true,
		Reason: "evidence_store_replaced", FromTs: from, ToTs: target, DetectedAt: now.Unix()}
	original := cached
	m.cloudWatchNginxEvidenceRecoveryCache.Store(&cached)
	m.cloudWatchNginxEvidenceFrom.Store(from)
	m.cloudWatchNginxEvidenceThrough.Store(from)
	if atBoundary := m.cloudWatchNginxEvidenceRecovery(now); !atBoundary.Verified {
		t.Fatalf("proof valid at exact minute boundary was rejected: %+v", atBoundary)
	}
	// Retention is rounded up to a retained whole minute. One millisecond
	// later the cached cursor is behind that boundary, without any DB change.
	later := now.Add(time.Millisecond)
	retentionFrom, _ := cloudWatchNginxEvidenceRange(later, m.cfg.NginxEvidenceRetentionHours)
	got := m.cloudWatchNginxEvidenceRecovery(later)
	if retentionFrom != from+60 || got.Verified || got.ThroughTs != 0 || !got.Incomplete ||
		got.DetectedAt != cached.DetectedAt || got.Reason != cached.Reason || got.FromTs != cached.FromTs || got.ToTs != cached.ToTs {
		t.Fatalf("expired proof discarded detected recovery history: cached=%+v result=%+v", cached, got)
	}
	if *m.cloudWatchNginxEvidenceRecoveryCache.Load() != original {
		t.Fatal("HTTP projection mutated the immutable cached audit history")
	}
	coverage := m.logChainNginxEvidenceCoverage(later)
	if coverage["from_ts"] != retentionFrom || coverage["through_ts"] != int64(0) {
		t.Fatalf("expired proof still advertised coverage: %+v", coverage)
	}
	text := strings.Join(m.logChainCurrentBlindSpots(later), "\n")
	if !strings.Contains(text, "正在补扫") || !strings.Contains(text, "不代表请求没有发生") ||
		!strings.Contains(text, time.Unix(target, 0).In(cstLocation).Format("2006-01-02 15:04:05")) {
		t.Fatalf("minute rollover hid the known recovery gap: %s", text)
	}
}

func TestLogChainRequestsExposeEvidenceRecoveryBlindSpot(t *testing.T) {
	m, now := newCloudWatchNginxRecoveryTestMonitor(t)
	state := completeEmptyCloudWatchNginxEvidence(t, m, now)
	// Match the store-opening lifecycle: replacing the DB invalidates its
	// cached proof before any HTTP response can advertise coverage.
	m.nginxEvidenceDB = newCloudWatchNginxRecoveryTestDB(t)
	m.cloudWatchNginxEvidenceRecoveryCache.Store(nil)
	if _, err := m.loadCloudWatchNginxCursor(now); err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/logchain/requests?days=1&domain=missing-evidence-test.example", nil)
	m.serveLogChainRequests(c)
	if recorder.Code != http.StatusOK {
		t.Fatalf("logchain response=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		BlindSpots []string `json:"blind_spots"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	text := strings.Join(response.BlindSpots, "\n")
	if !strings.Contains(text, "正在补扫") || !strings.Contains(text, "不代表请求没有发生") ||
		!strings.Contains(text, time.Unix(state.EvidenceThroughTs, 0).In(cstLocation).Format("2006-01-02 15:04:05")) {
		t.Fatalf("recovery gap not explicitly returned to troubleshooting UI: %s", text)
	}
	completeEmptyCloudWatchNginxEvidence(t, m, now)
	if strings.Contains(strings.Join(m.logChainCurrentBlindSpots(now), "\n"), "正在补扫") {
		t.Fatal("completed recovery still shown as an active evidence gap")
	}
}

func TestCloudWatchNginxEvidencePruningNarrowsDurableCoverage(t *testing.T) {
	m, now := newCloudWatchNginxRecoveryTestMonitor(t)
	state := completeEmptyCloudWatchNginxEvidence(t, m, now)
	m.cloudWatchNginxEvidenceFrom.Store(state.EvidenceCoverageFromTs)
	m.cloudWatchNginxEvidenceThrough.Store(state.EvidenceThroughTs)
	if err := m.nginxEvidenceDB.Create(&NginxRequestEvidence{EventID: "ttl-expired", Node: cloudWatchNginxNode,
		EventMS: (state.EvidenceCoverageFromTs + 30) * 1000}).Error; err != nil {
		t.Fatal(err)
	}
	later := now.Add(time.Hour + 15*time.Second)
	if err := m.pruneNginxEvidenceOnce(context.Background(), later); err != nil {
		t.Fatal(err)
	}
	var proof CloudWatchNginxEvidenceCheckpoint
	if err := m.nginxEvidenceDB.First(&proof, "id = ?", cloudWatchNginxCursorID).Error; err != nil {
		t.Fatal(err)
	}
	wantFrom, _ := cloudWatchNginxEvidenceRange(later, m.cfg.NginxEvidenceRetentionHours)
	if proof.CoverageFromTs != wantFrom || proof.ThroughTs != state.EvidenceThroughTs {
		t.Fatalf("pruned data still included in durable coverage: %+v want from=%d", proof, wantFrom)
	}
	var expiredRows int64
	if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Where("event_id = ?", "ttl-expired").Count(&expiredRows).Error; err != nil || expiredRows != 0 {
		t.Fatalf("TTL fixture was not deleted: count=%d err=%v", expiredRows, err)
	}
	// Do not refresh the collector cursor/cache: the HTTP projection itself
	// must exclude the interval that the independent TTL maintenance deleted.
	coverage := m.logChainNginxEvidenceCoverage(later)
	ready, _ := m.readyStatus(later)
	if m.cloudWatchNginxEvidenceFrom.Load() != state.EvidenceCoverageFromTs ||
		coverage["from_ts"] != wantFrom || coverage["through_ts"] != state.EvidenceThroughTs ||
		ready.CloudWatchNginx.EvidenceCoverageFrom != wantFrom {
		t.Fatalf("logchain/ready advertised expired coverage before collector refresh: coverage=%+v ready=%+v", coverage, ready.CloudWatchNginx)
	}
	got, err := m.loadCloudWatchNginxCursor(later)
	if err != nil {
		t.Fatal(err)
	}
	if got.EvidenceGapDetectedAt != 0 || got.EvidenceCoverageFromTs != wantFrom || got.EvidenceThroughTs != state.EvidenceThroughTs {
		t.Fatalf("normal retention pruning must not trigger a full reset: %+v", got)
	}
}
