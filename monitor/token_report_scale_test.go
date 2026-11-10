package monitor

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type tokenReportTimingLogger struct {
	logger.Interface
	t *testing.T
}

func TestTokenDetectionHealthProcessedPrefixScale(t *testing.T) {
	snapshot := os.Getenv("TOKEN_BACKLOG_SNAPSHOT")
	if snapshot == "" {
		t.Skip("set TOKEN_BACKLOG_SNAPSHOT to a stopped-writer snapshot")
	}
	m, _ := tokenIsolatedRuntimeFixture(t, snapshot)
	var state TokenMeteringState
	if err := m.tokenStore().First(&state).Error; err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenFactsSourceEpoch = state.SourceEpoch
	policy := detectionPolicy()
	m.cfg.TokenDetectionPolicy = tokenDetectionJSON(policy)
	hash := tokenDetectionPolicyHash(policy)
	db := m.tokenStore()
	// Keep the real fact payloads and indexes. On this disposable copy only,
	// make a long, fully current prefix so LIMIT cannot hide its scan cost.
	const prefix = 100000
	var cutoff int64
	if err := db.Model(&TokenMeteringFact{}).Select("log_id").Where("source_epoch=?", state.SourceEpoch).Order("log_id").Offset(prefix - 1).Limit(1).Scan(&cutoff).Error; err != nil || cutoff == 0 {
		t.Fatal("snapshot needs at least 100000 real facts", cutoff, err)
	}
	fill := func(after, through int64) {
		t.Helper()
		tokenReportScaleFixtureChange(t, m, func(db *gorm.DB) error {
			if after == 0 {
				for _, table := range []string{"token_detection_currents", "token_detection_retries"} {
					if err := db.Exec("DELETE FROM "+table+" WHERE source_epoch=?", state.SourceEpoch).Error; err != nil {
						return err
					}
				}
			}
			return db.Exec(`INSERT INTO token_detection_currents
			(source_epoch,log_id,created_at,user_id,channel_id,model,user_group,fact_revision,link_revision,policy_hash,reference_generation,reference_bucket,source_complete,candidate,next_at,result_json)
			SELECT f.source_epoch,f.log_id,f.created_at,f.user_id,f.channel_id,f.model,f.user_group,f.revision,f.link_revision,?,0,'',
			CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END,0,0,'{"rules":[]}'
			FROM token_metering_facts f LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300
			WHERE f.source_epoch=? AND f.log_id>? AND f.log_id<=?`, hash, state.SourceEpoch, after, through).Error
		})
	}
	fill(0, cutoff)
	check := func(label string, want int) {
		t.Helper()
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/anomalies/token/detection/health", nil)
		started := time.Now()
		m.serveTokenDetectionHealth(c)
		t.Logf("real facts=%d synthetic current prefix=%s health status=%d elapsed=%s", state.FactCount, label, w.Code, time.Since(started))
		if w.Code != 200 {
			t.Fatalf("health with processed prefix: %s", w.Body.String())
		}
		var body struct {
			PendingSampleCount int `json:"pending_sample_count"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.PendingSampleCount != want {
			t.Fatal("health sample changed", body.PendingSampleCount, want, err)
		}
	}
	check("100000", 50)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	rows, err := tokenDetectionPendingSample(m.tokenReadStore().WithContext(ctx), state.SourceEpoch, hash, time.Now().Unix(), 50)
	cancel()
	if err != nil || len(rows) != 50 {
		t.Fatal("pending suffix", len(rows), err)
	}
	for _, row := range rows {
		if row.LogID <= cutoff {
			t.Fatal("processed prefix incorrectly reported as pending")
		}
	}
	fill(cutoff, 1<<62)
	check("all", 0)
}

func (l tokenReportTimingLogger) Trace(_ context.Context, begin time.Time, query func() (string, int64), err error) {
	elapsed := time.Since(begin)
	if elapsed < 50*time.Millisecond && err == nil {
		return
	}
	sql, rows := query()
	fields := strings.Fields(sql)
	// Only the statement shape is logged; never emit source operands/payloads.
	l.t.Logf("report query shape=%q rows=%d elapsed=%s error=%v", strings.Join(fields[:min(4, len(fields))], " "), rows, elapsed, err)
}

func TestTokenReportCurrentSnapshotScale(t *testing.T) {
	snapshot := os.Getenv("TOKEN_BACKLOG_SNAPSHOT")
	if snapshot == "" {
		t.Skip("set TOKEN_BACKLOG_SNAPSHOT to a stopped-writer snapshot")
	}
	m, _ := tokenIsolatedRuntimeFixture(t, snapshot)
	var state TokenMeteringState
	if err := m.tokenStore().First(&state).Error; err != nil {
		t.Fatal(err)
	}
	var revision TokenDetectionRevision
	if err := m.tokenStore().First(&revision).Error; err != nil {
		t.Fatal(err)
	}
	var frozen tokenDetectionFrozen
	if err := json.Unmarshal([]byte(revision.FrozenJSON), &frozen); err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenFactsSourceEpoch = state.SourceEpoch
	m.cfg.TokenDetectionPolicy = tokenDetectionJSON(frozen.Policy)
	m.tokenReadStore().Config.Logger = tokenReportTimingLogger{Interface: logger.Default.LogMode(logger.Silent), t: t}
	query := tokenDetectionQuery{SourceEpoch: state.SourceEpoch, From: state.StartAt, To: state.ScanThrough, Limit: 25}
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	report, err := m.tokenDetectionReport(ctx, query)
	t.Logf("actual retained-range facts=%d range_seconds=%d elapsed=%s error=%v", state.FactCount, query.To-query.From, time.Since(started), err)
	if err != nil {
		t.Fatal(err)
	}
	if len(report["items"].([]map[string]any)) != 25 {
		t.Fatal("retained-range list did not return a full first page")
	}
	var independent int64
	if err := m.tokenReadStore().Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND created_at>=? AND created_at<?", query.SourceEpoch, query.From, query.To).
		Where(`candidate=0 AND (source_complete=0 OR EXISTS (SELECT 1 FROM json_each(result_json,'$.rules') WHERE json_extract(value,'$.eligible')=0))`).Count(&independent).Error; err != nil {
		t.Fatal(err)
	}
	if report["summary"].(tokenDetectionSummary).Insufficient != independent {
		t.Fatal("derived coverage count changed the original full-population predicate")
	}
	t.Logf("independent original-JSON coverage count=%d matched", independent)
	var pending int64
	if err := m.tokenReadStore().Table("token_metering_facts AS f").Joins(tokenDetectionHeadJoin(m.tokenReadStore())).
		Joins("LEFT JOIN token_detection_buckets b ON b.id=c.reference_bucket").
		Joins("LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300").
		Where("f.source_epoch=? AND f.created_at>=? AND f.created_at<?", query.SourceEpoch, query.From, query.To).
		Where(`c.log_id IS NULL OR c.fact_revision!=f.revision OR c.link_revision!=f.link_revision OR c.policy_hash!=? OR c.reference_generation!=COALESCE(b.generation,0) OR c.source_complete!=CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END`, tokenDetectionPolicyHash(frozen.Policy)).
		Count(&pending).Error; err != nil {
		t.Fatal(err)
	}
	if report["summary"].(tokenDetectionSummary).Unprocessed != pending {
		t.Fatal("optimized pending count changed the original full-population predicate")
	}
	t.Logf("independent original pending count=%d matched", pending)
	for _, status := range []string{"candidate", "insufficient", "not_hit"} {
		q := query
		q.Status = status
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		started := time.Now()
		got, err := m.tokenDetectionReport(ctx, q)
		cancel()
		t.Logf("retained-range status=%s elapsed=%s error=%v", status, time.Since(started), err)
		if err != nil || got["summary"].(tokenDetectionSummary).Insufficient != independent || got["summary"].(tokenDetectionSummary).Unprocessed != pending {
			t.Fatal("status filter became unavailable or hid population gaps", status, err)
		}
	}
	// A second, explicitly synthetic phase protects the report as the real
	// detector catches up. Retain real identities/payload sizes and existing
	// first-page evidence; add only disposable heads on this test database.
	var boundary TokenDetectionCurrent
	if err := m.tokenStore().Select("created_at,log_id").Order("created_at,log_id").Offset(5000).First(&boundary).Error; err != nil {
		t.Fatal(err)
	}
	var heads int64
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Count(&heads).Error; err != nil {
		t.Fatal(err)
	}
	targetHeads := state.FactCount
	if heads < targetHeads {
		tokenReportScaleFixtureChange(t, m, func(db *gorm.DB) error {
			return db.Exec(`INSERT INTO token_detection_currents
			(source_epoch,log_id,created_at,user_id,channel_id,model,user_group,fact_revision,link_revision,policy_hash,reference_generation,reference_bucket,source_complete,candidate,result_json)
			SELECT f.source_epoch,f.log_id,f.created_at,f.user_id,f.channel_id,f.model,f.user_group,f.revision,f.link_revision,?,0,'',
			CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END,0,'{"rules":[]}'
			FROM token_metering_facts f LEFT JOIN token_detection_currents c ON c.source_epoch=f.source_epoch AND c.log_id=f.log_id
			LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300
			WHERE f.source_epoch=? AND f.created_at>=? AND f.created_at<? AND c.log_id IS NULL
			AND (f.created_at>? OR (f.created_at=? AND f.log_id>?)) ORDER BY f.created_at,f.log_id LIMIT ?`,
				tokenDetectionPolicyHash(frozen.Policy), query.SourceEpoch, query.From, query.To, boundary.CreatedAt, boundary.CreatedAt, boundary.LogID, targetHeads-heads).Error
		})
	}
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Count(&heads).Error; err != nil {
		t.Fatal(err)
	}
	// Compare the original pending predicate on a covered fact-range scan.
	// The near-complete case must not become slower as heads accumulate.
	var coveredPending int64
	// This independent legacy scan is an offline correctness oracle. It does
	// not serve the API; keep its bounded allowance separate from the actual
	// report's unchanged three-second deadline below.
	comparisonCtx, comparisonStop := context.WithTimeout(context.Background(), 30*time.Second)
	comparisonStart := time.Now()
	comparisonErr := m.tokenReadStore().WithContext(comparisonCtx).Table("token_metering_facts AS f INDEXED BY idx_token_detection_work_fact").
		Joins(tokenDetectionHeadJoin(m.tokenReadStore())).
		Joins("LEFT JOIN token_detection_buckets b ON b.id=c.reference_bucket").
		Joins("LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300").
		Where("f.source_epoch=? AND f.created_at>=? AND f.created_at<?", query.SourceEpoch, query.From, query.To).
		Where(`c.log_id IS NULL OR c.fact_revision!=f.revision OR c.link_revision!=f.link_revision OR c.policy_hash!=? OR c.reference_generation!=COALESCE(b.generation,0) OR c.source_complete!=CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END`, tokenDetectionPolicyHash(frozen.Policy)).
		Count(&coveredPending).Error
	comparisonStop()
	t.Logf("covered fact-range pending count=%d elapsed=%s error=%v", coveredPending, time.Since(comparisonStart), comparisonErr)
	if comparisonErr != nil {
		t.Fatal(comparisonErr)
	}
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	started = time.Now()
	grown, err := m.tokenDetectionReport(ctx, query)
	stop()
	t.Logf("synthetic result-head growth to=%d report_elapsed=%s error=%v (real first 5001 results preserved; not a four-day validation)", heads, time.Since(started), err)
	if err != nil || len(grown["items"].([]map[string]any)) != 25 {
		t.Fatal("report became unavailable as detection heads accumulated", err)
	}
	if grown["summary"].(tokenDetectionSummary).Unprocessed != coveredPending {
		t.Fatal("grown head count changed pending semantics")
	}
	// Reuse this large disposable fixture for health after the processed prefix
	// grows to the entire retained population. Only the remaining old heads need
	// synthetic validity alignment; the snapshot and frozen artifacts stay intact.
	tokenReportScaleFixtureChange(t, m, func(db *gorm.DB) error {
		if err := db.Exec(`INSERT INTO token_detection_currents
		(source_epoch,log_id,created_at,fact_revision,link_revision,policy_hash,reference_generation,reference_bucket,source_complete,next_at,result_json)
		SELECT f.source_epoch,f.log_id,f.created_at,f.revision,f.link_revision,?,0,'',
		CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END,0,'{"rules":[]}'
		FROM token_metering_facts f LEFT JOIN token_detection_currents c ON c.source_epoch=f.source_epoch AND c.log_id=f.log_id
		LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300
		WHERE f.source_epoch=? AND c.log_id IS NULL`, tokenDetectionPolicyHash(frozen.Policy), state.SourceEpoch).Error; err != nil {
			return err
		}
		if err := db.Exec(`UPDATE token_detection_currents AS c SET
		(fact_revision,link_revision,source_complete,policy_hash,next_at,reference_bucket,reference_generation)=
		(SELECT f.revision,f.link_revision,CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END,?,0,'',0
		 FROM token_metering_facts f LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300 WHERE f.source_epoch=c.source_epoch AND f.log_id=c.log_id)
		WHERE c.source_epoch=? AND (c.next_at>0 OR c.reference_bucket<>'' OR c.reference_generation<>0 OR EXISTS (
		 SELECT 1 FROM token_metering_facts f LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300
		 WHERE f.source_epoch=c.source_epoch AND f.log_id=c.log_id AND (c.fact_revision!=f.revision OR c.link_revision!=f.link_revision OR c.policy_hash!=? OR c.source_complete!=CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END)))`,
			tokenDetectionPolicyHash(frozen.Policy), state.SourceEpoch, tokenDetectionPolicyHash(frozen.Policy)).Error; err != nil {
			return err
		}
		return db.Where("source_epoch=?", state.SourceEpoch).Delete(&TokenDetectionRetry{}).Error
	})
	health := func(want int) {
		t.Helper()
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/anomalies/token/detection/health", nil)
		started := time.Now()
		m.serveTokenDetectionHealth(c)
		var body struct {
			PendingSampleCount int `json:"pending_sample_count"`
		}
		t.Logf("grown health real_facts=%d expected_pending=%d status=%d elapsed=%s", state.FactCount, want, w.Code, time.Since(started))
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || w.Code != 200 || body.PendingSampleCount != want {
			t.Fatal("grown health", w.Body.String(), err)
		}
	}
	health(0)
	if err := m.tokenStore().Exec(`DELETE FROM token_detection_currents WHERE source_epoch=? AND log_id IN
		(SELECT log_id FROM token_metering_facts WHERE source_epoch=? ORDER BY log_id DESC LIMIT 50)`, state.SourceEpoch, state.SourceEpoch).Error; err != nil {
		t.Fatal(err)
	}
	health(50)
}
