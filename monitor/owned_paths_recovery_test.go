package monitor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestChannelSyncRetryBudget(t *testing.T) {
	for i, want := range []time.Duration{10 * time.Minute, 15 * time.Second, 30 * time.Second, 60 * time.Second, 120 * time.Second, 120 * time.Second} {
		if got := channelSyncRetryDelay(10*time.Minute, i); got != want {
			t.Fatalf("attempt=%d got=%s want=%s", i, got, want)
		}
	}
}

func TestGroupConfigChannelFailureRecoveryWake(t *testing.T) {
	m := newTestMonitor(t)
	prod, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	m.prodDB = prod
	m.cfg.GroupGovernanceEnabled = true
	m.cfg.LogChainOnlySource = true
	m.cfg.BackgroundSourceMinStartIntervalMS = -1
	m.cfg.GroupGovernanceSyncMinutes = 10
	if _, err = prod.Exec(`CREATE TABLE options (key TEXT,value TEXT); INSERT INTO options VALUES ('GroupRatio','{"g":1}');`); err != nil {
		t.Fatal(err)
	}
	if _, err = prod.Exec(`CREATE TABLE channels(id INTEGER,name TEXT,type INTEGER,status INTEGER,"group" TEXT,models TEXT,base_url TEXT); INSERT INTO channels VALUES(1,'a',1,1,'g','m','https://example.test');`); err != nil {
		t.Fatal(err)
	}
	prod.SetMaxOpenConns(1)
	held, err := prod.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Exercise the actual 10-second source budget, not a synthetic failure
	// state. The caller context remains alive, so the failure stays visible.
	err = m.refreshChannelsContext(context.Background())
	held.Close()
	if !errors.Is(err, context.DeadlineExceeded) || !m.channelSnapshotSyncFailed.Load() {
		t.Fatalf("source timeout was not retained: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); m.runGroupGovernanceLoop(ctx) }()
	lifecycleEventually(t, 2*time.Second, m.groupGovernanceWaitingChannels.Load, "dependency retry was not armed")
	if err = m.refreshChannelsContext(ctx); err != nil {
		t.Fatal(err)
	}
	lifecycleEventually(t, 3*time.Second, func() bool {
		var s GroupGovernanceState
		return m.storeDB.Where("id=1").Limit(1).Find(&s).Error == nil && s.LastSuccessAt > 0 && s.LastError == ""
	}, "channel recovery did not wake governance before minute backoff")
	// Successful routine refreshes do not schedule a new users/tokens audit.
	for {
		select {
		case <-m.groupGovernanceConfigWake():
		default:
			goto drained
		}
	}
drained:
	m.notifyGroupGovernanceChannelsReady()
	select {
	case <-m.groupGovernanceConfigWake():
		t.Fatal("routine channel refresh woke heavy audit")
	default:
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loop did not cancel")
	}
}

func TestGGRecentProofDoesNotMoveFinalizationOrBridgeGap(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.CapacityEnabled = true
	now := time.Now().Unix() / 60 * 60
	write := func(from, to int64) {
		t.Helper()
		if err := m.storeDB.Transaction(func(tx *gorm.DB) error { return publishGGMinuteCoverage(tx, from, to, now) }); err != nil {
			t.Fatal(err)
		}
	}
	write(now-60, now-1)
	if _, end, _, err := m.groupGovernanceWindow(context.Background(), time.Unix(now, 0)); err != nil || end != 0 {
		t.Fatalf("a partial minute certified coverage: end=%d err=%v", end, err)
	}
	write(now-900, now+60)
	target, end, from, err := m.groupGovernanceWindow(context.Background(), time.Unix(now+20, 0))
	if err != nil || target != now || end != now || from != now-900 {
		t.Fatalf("%d %d %d %v", target, end, from, err)
	}
	if metricFinalizeTarget(now) != now-3600 {
		t.Fatal("shared finalization changed")
	}
	// Old disjoint finalization must not hide the current committed window.
	write(now-7200, now-6000)
	_, end, from, err = m.groupGovernanceWindow(context.Background(), time.Unix(now, 0))
	if err != nil || end != now || from != now-900 {
		t.Fatal("old replay replaced tail")
	}
	if err = m.storeDB.Transaction(func(tx *gorm.DB) error { return publishGGMinuteCoverage(tx, now+600, now+900, now+900) }); err != nil {
		t.Fatal(err)
	}
	_, end, from, err = m.groupGovernanceWindow(context.Background(), time.Unix(now+900, 0))
	if err != nil || end != now+900 || from != now+600 {
		t.Fatal("disjoint source success bridged gap")
	}
}

func TestGGFinalizedErrorReplayAndPublicationRetry(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.GroupGovernanceEnabled = true
	m.prodDB = frtRecomputeProdDB(t)
	from := time.Now().Unix()/60*60 - 7200
	add := func(id int) {
		t.Helper()
		if _, err := m.prodDB.Exec("INSERT INTO logs(id,created_at,user_id,channel_id,model_name,`group`,type,content,other,request_id) VALUES(?,?,?,?,?,?,5,?,?,?)", id, from+int64(id), 1, 1, "m", "g", "openai_error", `{"error_code":"upstream_http2_stream_error"}`, fmt.Sprintf("request-%d", id)); err != nil {
			t.Fatal(err)
		}
	}
	read := func() {
		t.Helper()
		if _, err := m.sampleCustomerHealthRangeLow(context.Background(), from, from+60); err != nil {
			t.Fatal(err)
		}
	}
	check := func(count int64, version int) {
		t.Helper()
		var row GroupGovernanceErrorMinute
		if err := m.storeDB.First(&row).Error; err != nil {
			t.Fatal(err)
		}
		if row.Failed != count || row.FailedRead != count || row.Version != version {
			t.Fatalf("projection %+v", row)
		}
	}
	add(1)
	read()
	check(1, groupGovernanceErrorVersion)
	// A late row in an already finalized old slice is reclassified, not skipped
	// by the wall-clock one-hour gate. Repeated replay is idempotent.
	add(2)
	read()
	read()
	check(2, groupGovernanceErrorVersion)
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("fail-gg-publish", func(tx *gorm.DB) {
		if tx.Statement.Table == "group_governance_error_minutes" {
			_ = tx.AddError(errors.New("injected publication failure"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	add(3)
	read()
	check(2, 0)
	var fact CapacityUserMinuteSample
	if err := m.storeDB.First(&fact).Error; err != nil || fact.Failed != 3 {
		t.Fatal("optional error publish blocked shared facts", fact, err)
	}
	if err := m.storeDB.Callback().Create().Remove("fail-gg-publish"); err != nil {
		t.Fatal(err)
	}
	read()
	check(3, groupGovernanceErrorVersion)
}

func TestCustomerHealthOnlineSpendIndependentOfUsage(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.CapacityEnabled = true
	m.cfg.CustomerHealthSourceEnabled = false
	m.cfg.UsageFactsEnabled = true
	m.cfg.UsageFactsReadEnabled = true
	day, target := customerHealthSourceRange(chTestNow())
	if err := m.saveCustomerHealthSourceCursor(day, target); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&CapacityUserMinuteSample{BucketTs: day + 60, UserID: 101, ChannelID: 1, Grp: "g", ModelName: "m", Success: 2, Quota: 750000, RefundQuota: 250000, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	rows := []CustomerHealthRow{{Members: []CustomerHealthMember{{UserID: 101}}}}
	m.attachCustomerHealthSpendBatch(context.Background(), rows, day, day+86400, chTestNow())
	if rows[0].TodaySpendUSD == nil || *rows[0].TodaySpendUSD != 1 || rows[0].SpendThroughTs != target || rows[0].SpendFromTs != day {
		t.Fatalf("independent amount/coverage: %+v", rows[0])
	}
	// Neither a stale process-local cursor nor an enabled Usage projection can
	// certify a missing independent day prefix.
	if err := m.storeDB.Model(&CustomerHealthSourceCursor{}).Where("id=1").Update("through_ts", day).Error; err != nil {
		t.Fatal(err)
	}
	rows[0].TodaySpendUSD = nil
	m.attachCustomerHealthSpendBatch(context.Background(), rows, day, day+86400, chTestNow())
	if rows[0].TodaySpendUSD != nil {
		t.Fatal("unknown amount became known")
	}
}
