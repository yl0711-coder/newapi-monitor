package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// Opt-in to avoid writing a large SQLite fixture on every routine CI run.
// MONITOR_REPORT_LOAD_TEST=1 go test -run '^TestModelReportMixedSevenDayWorkload$' -v ./monitor
func TestModelReportMixedSevenDayWorkload(t *testing.T) {
	if os.Getenv("MONITOR_REPORT_LOAD_TEST") != "1" {
		t.Skip("enable the seven-day SQLite load fixture with MONITOR_REPORT_LOAD_TEST=1")
	}
	const facts = 96000
	m := seedModelReportMixedWorkload(t, facts)
	r := gin.New()
	const stats, frt = "/model-statistics/report", "/model-statistics/frt"
	r.GET(stats, m.serveModelStatisticsReport)
	r.GET(frt, m.serveModelFRTReport)
	ctx, cancel := context.WithCancel(context.Background())
	writes := make(chan int, 1)
	go simulateModelReportWriter(ctx, m, writes)
	stopped := false
	defer func() {
		if !stopped {
			cancel()
			<-writes
		}
	}()
	start := make(chan struct{})
	backupFinished := make(chan mixedReportBackupOutcome, 1)
	backupCtx, cancelBackup := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancelBackup()
	go runMixedReportBackup(backupCtx, m, start, backupFinished)
	type outcome struct {
		path    string
		reply   *httptest.ResponseRecorder
		started time.Time
		ended   time.Time
	}
	replies := make(chan outcome, 16)
	for i := range cap(replies) {
		path := stats
		if i%2 == 0 {
			path = frt
		}
		go func() {
			<-start
			begin := time.Now()
			replies <- outcome{path, mixedReportRequest(r, path), begin, time.Now()}
		}()
	}
	close(start)
	var passed, busy int
	var slowest time.Duration
	var successes []outcome
	for range cap(replies) {
		got := <-replies
		if got.reply.Code == http.StatusServiceUnavailable && got.reply.Header().Get("Retry-After") == "1" {
			busy++
			continue
		}
		if err := checkMixedReportPayload(got.path, got.reply, facts); err != nil {
			t.Error(err)
			continue
		}
		passed++
		successes = append(successes, got)
		slowest = max(slowest, got.ended.Sub(got.started))
	}
	cancel()
	commits := <-writes
	stopped = true
	backup := <-backupFinished
	if backup.err != nil || !m.storeBackupSetVerified.Load() {
		t.Fatalf("real daily backup path did not publish a verified main+facts pair: %v", backup.err)
	}
	var overlapped bool
	for _, got := range successes {
		overlapped = overlapped || (got.started.Before(backup.ended) && backup.started.Before(got.ended))
	}
	if passed == 0 || busy == 0 || commits == 0 {
		t.Fatalf("burst didn't cover successful reads, contention and concurrent writes: ok=%d busy=%d writes=%d", passed, busy, commits)
	}
	if !overlapped {
		t.Fatal("backup execution did not overlap a successful report")
	}
	for _, path := range []string{stats, frt} {
		if err := checkMixedReportPayload(path, mixedReportRequest(r, path), facts); err != nil {
			t.Errorf("normal request did not recover after burst: %v", err)
		}
	}
	t.Logf("facts=%d burst_requests=%d successful=%d explicit_busy=%d slowest_success=%v concurrent_write_commits=%d verified_backup_elapsed=%v backup_overlapped_report=%v", facts, passed+busy, passed, busy, slowest, commits, backup.ended.Sub(backup.started), overlapped)
}

type mixedReportBackupOutcome struct {
	started, ended time.Time
	err            error
}

func runMixedReportBackup(ctx context.Context, m *Monitor, start <-chan struct{}, done chan<- mixedReportBackupOutcome) {
	select {
	case <-ctx.Done():
		done <- mixedReportBackupOutcome{err: ctx.Err()}
		return
	case <-start:
	}
	begin := time.Now()
	_, err := m.createStoreBackupSet(ctx, begin, true, true)
	done <- mixedReportBackupOutcome{started: begin, ended: time.Now(), err: err}
}

func seedModelReportMixedWorkload(t *testing.T, facts int) *Monitor {
	t.Helper()
	now := time.Now()
	end := customerHealthSourceFinalizedThrough(now)
	m := modelStatisticsWindowFixture(t, true, now, end, end, end)
	// The small report fixtures normally share one DB; the production backup
	// path requires distinct on-disk databases, as in the deployed topology.
	if err := m.openUsageFactsStore(filepath.Join(filepath.Dir(m.cfg.StorePath), "facts.db"), false); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelSnap{ID: 1, Name: "fixture-channel"}).Error; err != nil {
		t.Fatal(err)
	}
	const seed = `WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i+1 < ?)
	INSERT INTO capacity_user_minute_samples
	(bucket_ts,user_id,channel_id,grp,model_name,username,traffic_class_version,
	 success,anomaly,failed,ttft_500,ttft_1k,ttft_2k,ttft_5k,ttft_10k,ttft_inf,ttft_observed,
	 ttft_over_3s,ttft_max_ms,ttft_semantics_version,frt_sum_ms,frt_sum_observed,frt_sum_version)
	SELECT ?-60*(i%10080+1),i%383+1,i/10080+1,printf('g%d',i%4),printf('m%d',i/101%12),printf('u%d',i%383+1),?,
	 1,0,0,1,0,0,0,0,0,1,0,400,?,400,1,? FROM n`
	res := m.storeDB.Exec(seed, facts, end, stabilityTrafficClassificationVersion, ttftCoverageSemanticsVersion, frtSumSemanticsVersion)
	if res.Error != nil || res.RowsAffected != int64(facts) {
		t.Fatalf("fixture: got %d rows, error %v", res.RowsAffected, res.Error)
	}
	return m
}

func simulateModelReportWriter(ctx context.Context, m *Monitor, done chan<- int) {
	writes := 0
	defer func() { done <- writes }()
	ticker := time.NewTicker(6 * time.Millisecond)
	defer ticker.Stop()
	for i := range 40 {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			res := m.storeDB.WithContext(ctx).Model(&ChannelSnap{}).Where("id = ?", 1).UpdateColumn("name", fmt.Sprintf("fixture-channel-%d", i))
			if res.Error == nil && res.RowsAffected == 1 {
				writes++
			}
		}
	}
}

func mixedReportRequest(r *gin.Engine, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path+"?window=7d", nil))
	return w
}

func checkMixedReportPayload(path string, reply *httptest.ResponseRecorder, facts int64) error {
	if reply.Code != http.StatusOK {
		return fmt.Errorf("%s returned %d: %.300s", path, reply.Code, reply.Body.String())
	}
	var total int64
	if path == "/model-statistics/report" {
		var r ModelStatisticsReport
		if err := json.Unmarshal(reply.Body.Bytes(), &r); err != nil {
			return err
		}
		for _, model := range r.Models {
			total += model.Requests
		}
	} else {
		var r ModelFRTReport
		if err := json.Unmarshal(reply.Body.Bytes(), &r); err != nil {
			return err
		}
		for _, ch := range r.Channels {
			if ch.FRT.Observed == nil || ch.FRT.MeanMs == nil || *ch.FRT.MeanMs != 400 {
				return fmt.Errorf("FRT channel %d lost known sample counts or mean", ch.ChannelID)
			}
			total += *ch.FRT.Observed
		}
	}
	if total != facts {
		return fmt.Errorf("%s counted %d facts; want %d", path, total, facts)
	}
	return nil
}
