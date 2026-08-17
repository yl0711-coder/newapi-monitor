package monitor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func newUsageHistoryTestMonitor(t *testing.T) *Monitor {
	t.Helper()
	m := newTestMonitor(t)
	// Production history queries observe the shared low-lane duty cycle. Unit
	// tests use fake SQLite and verify semantics without wall-clock sleeps.
	m.cfg.StabilityBackfillDelayMS = -1
	m.cfg.StabilityBackfillSourceDutyPercent = 100
	m.cfg.UsageFactsEnabled = true
	m.cfg.UsageFactsFullHistoryEnabled = true
	m.cfg.UsageFactsHistoryDelayMS = -1
	m.cfg.UsageFactsHistoryDutyPercent = 100
	m.cfg.UsageFactsHistorySourceMode = "complete"
	m.cfg.UsageFactsHistorySourceEpoch = "test-source-v1"
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = "CAST((created_at + 28800) / 86400 AS INTEGER)"
	if _, err := m.prodDB.Exec("ALTER TABLE users ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0"); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestFullHistorySnapshotReadinessUsesDurableCheckpointWithoutWorkerOrWrites(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 81, 1)
	now := time.Now()
	through := m.usageFactFinalizedHour(now)
	floor := usageFactDayStart(through - 2*usageFactDaySeconds)
	verifiedThrough := usageFactDayStart(through)
	epoch := "restore-source-v1"
	nowUnix := now.Unix()
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", 81).Updates(map[string]any{
		"active": true, "tracked_revision": int64(1), "source_floor_hour": floor,
		"source_first_log_hour": nil, "source_ceiling_hour": nil,
		"coverage_through_hour": verifiedThrough, "tail_through_hour": through,
		"verify_next_hour": verifiedThrough, "verified_through_hour": verifiedThrough,
		"verification_status": "complete", "verified_at": nowUnix,
		"source_floor_checked_at": nowUnix, "source_history_status": "no_history", "coverage_status": "ready",
		"classification_version":  userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion, "source_epoch": epoch,
		"updated_at": nowUnix,
	}).Error; err != nil {
		t.Fatal(err)
	}
	userID := int64(81)
	job := UsageFactJob{
		ID: usageFactHistoryJobID(userID, 1), Kind: usageFactHistoryKindVerify, Priority: 50,
		UserID: &userID, TrackedRevision: 1, SourceEpoch: epoch, FromTs: floor, ThroughTs: through,
		NextHour: through, VerifyNextHour: verifiedThrough,
		TotalHours: (through - floor) / usageFactHourSeconds, CompletedHours: (through - floor) / usageFactHourSeconds,
		VerifiedHours: (verifiedThrough - floor) / usageFactHourSeconds,
		Status:        usageFactHistoryJobComplete, CreatedAt: nowUnix, UpdatedAt: nowUnix, CompletedAt: nowUnix,
	}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	published := UsageFactPublishedMember{
		UserID: userID, TrackedRevision: 1, SourceEpoch: epoch,
		ClassificationVersion: userTrafficClassificationVersion,
		QuerySemanticsVersion: usageFactQuerySemanticsVersion,
		SourceFloorHour:       floor, VerifiedThroughHour: verifiedThrough, PublishedAt: nowUnix,
	}
	if err := m.usageFactsStore().Create(&published).Error; err != nil {
		t.Fatal(err)
	}
	fingerprint := portalMemberFingerprintFromIDs([]int64{userID})
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"traffic_class_version": userTrafficClassificationVersion,
		"published_fingerprint": fingerprint, "published_window_days": int((through - floor) / usageFactDaySeconds),
		"published_range_start": floor, "published_through": through, "published_at": nowUnix,
	}).Error; err != nil {
		t.Fatal(err)
	}

	var beforeGlobal UsageFactSyncState
	if err := m.usageFactsStore().First(&beforeGlobal, 1).Error; err != nil {
		t.Fatal(err)
	}
	m.cfg = Settings{
		LocalSnapshotOnly: true, UsageFactsLocalReadOnly: true, UsageFactsReadEnabled: true,
		UsageFactsFullHistoryEnabled: true, UsageFactsHistorySourceMode: "complete", UsageFactsHistorySourceEpoch: epoch,
	}
	m.prodDB = nil
	m.refreshUsageFactsReadiness(context.Background(), now)
	if !m.usageFactsReadReady.Load() || m.usageFactsReadyFrom.Load() != floor || m.usageFactsReadyThrough.Load() != through {
		t.Fatalf("restored full-history checkpoint did not open read gate: ready=%v from=%d through=%d",
			m.usageFactsReadReady.Load(), m.usageFactsReadyFrom.Load(), m.usageFactsReadyThrough.Load())
	}
	if m.usageFactsFullHistoryEnabled() || !m.usageFactsFullHistoryMode() || m.prodDB != nil {
		t.Fatal("restored snapshot enabled a source/mutation worker")
	}
	progress, err := m.usageFactHistoryProgress(context.Background(), now)
	if err != nil || !progress.Enabled || progress.PublishedMembers != 1 {
		t.Fatalf("restored full-history progress unavailable: progress=%+v err=%v", progress, err)
	}
	var afterGlobal UsageFactSyncState
	if err := m.usageFactsStore().First(&afterGlobal, 1).Error; err != nil {
		t.Fatal(err)
	}
	if beforeGlobal != afterGlobal {
		t.Fatalf("read-only readiness mutated the restored facts state: before=%+v after=%+v", beforeGlobal, afterGlobal)
	}
}

func TestUsageFactHistoryDurableJobRunsDiscoveryDaysAndZeroDayToReady(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 1, 1)
	day1 := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	registered := day1 + 9*usageFactHourSeconds
	if _, err := m.prodDB.Exec("INSERT INTO users(id,username,email,created_at) VALUES(1,'u1','u1@x',?)", registered); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec(`INSERT INTO logs
(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`+"`group`"+`,token_id,token_name,other)
VALUES(1,1,10,?,2,'m',1000,10,20,'g',5,'t','')`, day1+10*usageFactHourSeconds); err != nil {
		t.Fatal(err)
	}
	// Lag=10m makes the finalized target exactly day3 00:00.
	now := time.Unix(day1+2*usageFactDaySeconds+20*60, 0)
	if err := m.reconcileUsageFactHistoryJobs(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	owner := "test-worker"
	tuning := defaultUsageFactHistoryTuning()
	for i := 0; i < 4; i++ { // discovery + two daily chunks; final pass observes idle
		worked, err := m.syncNextUsageFactHistoryWork(context.Background(), owner, &tuning, now)
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if i < 3 && !worked {
			t.Fatalf("step %d unexpectedly idle", i)
		}
	}
	var job UsageFactJob
	if err := m.usageFactsStore().First(&job, "id = ?", usageFactHistoryJobID(1, 1)).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != usageFactHistoryJobComplete || job.FromTs != day1 || job.ThroughTs != day1+2*usageFactDaySeconds ||
		job.NextHour != job.ThroughTs || job.TotalHours != 48 || job.CompletedHours != 48 {
		t.Fatalf("durable job did not close exactly: %+v", job)
	}
	var state UsageFactMemberState
	if err := m.usageFactsStore().First(&state, "user_id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.CoverageStatus != "ready" || state.SourceFloorHour == nil || *state.SourceFloorHour != day1 ||
		state.CoverageThroughHour == nil || *state.CoverageThroughHour != job.ThroughTs ||
		state.TailThroughHour == nil || *state.TailThroughHour != job.ThroughTs {
		t.Fatalf("member state not ready: %+v", state)
	}
	var proofs []UsageFactMemberDayState
	if err := m.usageFactsStore().Where("user_id = ?", 1).Order("date_ts").Find(&proofs).Error; err != nil {
		t.Fatal(err)
	}
	if len(proofs) != 2 || !usageFactMemberDayHistoryReady(proofs[0]) || !usageFactMemberDayHistoryReady(proofs[1]) || proofs[1].SourceRows != 0 {
		t.Fatalf("source-controlled real+zero day proofs missing: %+v", proofs)
	}
}

func TestUsageFactHistoryNoLogsCompletesFromBoundaryWithoutEmptyDayQueries(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 2, 1)
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	if _, err := m.prodDB.Exec("INSERT INTO users(id,username,email,created_at) VALUES(2,'u2','u2@x',?)", day+3600); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(day+5*usageFactDaySeconds+20*60, 0)
	if err := m.reconcileUsageFactHistoryJobs(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	tuning := defaultUsageFactHistoryTuning()
	worked, err := m.syncNextUsageFactHistoryWork(context.Background(), "no-history", &tuning, now)
	if err != nil || !worked {
		t.Fatalf("discovery failed: worked=%v err=%v", worked, err)
	}
	// Even a successful no-row source boundary must pass the durable local
	// empty-range audit before publication. This prevents stale legacy facts
	// from being exposed after an epoch/revision change.
	worked, err = m.syncNextUsageFactHistoryWork(context.Background(), "no-history", &tuning, now)
	if err != nil || !worked {
		t.Fatalf("empty-range verification failed: worked=%v err=%v", worked, err)
	}
	var job UsageFactJob
	if err := m.usageFactsStore().First(&job, "id = ?", usageFactHistoryJobID(2, 1)).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != usageFactHistoryJobComplete || job.CompletedHours != job.TotalHours {
		t.Fatalf("no-history job should complete after boundary plus empty-range proof: %+v", job)
	}
	var proofCount int64
	if err := m.usageFactsStore().Model(&UsageFactMemberDayState{}).Where("user_id = ?", 2).Count(&proofCount).Error; err != nil {
		t.Fatal(err)
	}
	if proofCount != 0 {
		t.Fatalf("no-history member manufactured %d empty day rows", proofCount)
	}
}

func TestUsageFactHistoryReconcileExtendsOneJobInsteadOfDuplicating(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 3, 1)
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	if _, err := m.prodDB.Exec("INSERT INTO users(id,username,email,created_at) VALUES(3,'u3','u3@x',?)", day); err != nil {
		t.Fatal(err)
	}
	first := time.Unix(day+2*usageFactDaySeconds+20*60, 0)
	if err := m.reconcileUsageFactHistoryJobs(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := first.Add(time.Hour)
	if err := m.reconcileUsageFactHistoryJobs(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	var jobs []UsageFactJob
	if err := m.usageFactsStore().Where("user_id = ? AND tracked_revision = ?", 3, 1).Find(&jobs).Error; err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ThroughTs != m.usageFactFinalizedHour(second) {
		t.Fatalf("target movement duplicated instead of extending one job: %+v", jobs)
	}
}

func TestDiscoverUsageFactSourceBoundariesUsesRegistrationAndRealLogs(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	registered := time.Date(2026, 5, 10, 9, 30, 0, 0, usageCST).Unix()
	earlierLog := time.Date(2026, 5, 11, 23, 30, 0, 0, usageCST).Unix()
	laterLog := time.Date(2026, 5, 13, 12, 0, 0, 0, usageCST).Unix()
	if _, err := m.prodDB.Exec("INSERT INTO users(id,username,email,created_at) VALUES(1,'a','a@x',?),(2,'b','b@x',?)", registered, registered); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec(`INSERT INTO logs(id,user_id,created_at,type,model_name,quota,other)
VALUES(1,1,?,2,'m',100,''),(2,1,?,6,'m',20,''),(3,1,?,5,'ignored',1,'')`, earlierLog, laterLog, earlierLog-3600); err != nil {
		t.Fatal(err)
	}

	got, err := m.discoverUsageFactSourceBoundaries(context.Background(), []int64{2, 1, 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].UserID != 1 || got[1].UserID != 2 {
		t.Fatalf("boundary order/dedupe wrong: %+v", got)
	}
	if got[0].RegisteredAt != registered || got[0].FirstLogAt != earlierLog || got[0].LastLogAt != laterLog {
		t.Fatalf("member 1 boundary wrong: %+v", got[0])
	}
	floor, ok := got[0].sourceFloorHour()
	if !ok || floor != usageFactDayStart(registered) {
		t.Fatalf("logical floor must include the proven-empty registration gap: floor=%d ok=%v", floor, ok)
	}
	historyStart, ok := got[0].historyStartHour()
	if !ok || historyStart != usageFactDayStart(earlierLog) {
		t.Fatalf("history queries must skip the proven-empty registration gap: start=%d ok=%v", historyStart, ok)
	}
	ceiling, ok := got[0].sourceCeilingHour()
	if !ok || ceiling != time.Unix(laterLog, 0).Add(time.Hour).Truncate(time.Hour).Unix() {
		t.Fatalf("source ceiling wrong: %d %v", ceiling, ok)
	}
	if got[1].FirstLogAt != 0 {
		t.Fatalf("no-history member must remain explicit: %+v", got[1])
	}
	floor, ok = got[1].sourceFloorHour()
	if !ok || floor != usageFactDayStart(registered) {
		t.Fatalf("no-history member must use registration day: floor=%d ok=%v", floor, ok)
	}
	if _, ok := got[1].historyStartHour(); ok {
		t.Fatal("no-history member must not schedule empty daily source queries")
	}
}

func TestUsageFactHistoryDiscoveryFallbackHasBoundedTurnAndNoPenaltySiblings(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	countingDB, counts := newCountingFakeProdDB(t)
	m.prodDB = countingDB
	m.usageDayExpr = usageDayExprSQLite
	if _, err := m.prodDB.Exec("ALTER TABLE users ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0"); err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	now := time.Unix(day+2*usageFactDaySeconds, 0)
	owner := "bounded-discovery"
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	jobs := make([]UsageFactJob, 0, 3)
	for _, id := range []int64{101, 102, 103} {
		prepareUsageHistoryCommitMember(t, m, id, 1)
		if _, err := m.prodDB.Exec("INSERT INTO users(id,username,email,created_at) VALUES(?,?,?,?)",
			id, fmt.Sprintf("u%d", id), fmt.Sprintf("u%d@x", id), day); err != nil {
			t.Fatal(err)
		}
		if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", id).Updates(map[string]any{
			"source_epoch": epoch, "classification_version": userTrafficClassificationVersion,
			"query_semantics_version": usageFactQuerySemanticsVersion, "source_history_status": "discovering",
		}).Error; err != nil {
			t.Fatal(err)
		}
		uid := id
		job := UsageFactJob{
			ID: usageFactHistoryJobID(uid, 1), Kind: usageFactHistoryKindDiscover, UserID: &uid,
			TrackedRevision: 1, SourceEpoch: epoch, ThroughTs: day + usageFactDaySeconds,
			Status: usageFactHistoryJobRunning, LeaseOwner: owner, LeaseUntil: now.Add(time.Minute).Unix(),
			CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
		}
		if err := m.usageFactsStore().Create(&job).Error; err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, job)
	}
	counts.reset()
	counts.failUsersAboveArgs.Store(1)
	claim := usageFactHistoryClaim{Jobs: jobs, LeaseOwner: owner}
	if err := m.executeUsageFactHistoryDiscovery(context.Background(), claim, now); err != nil {
		t.Fatal(err)
	}
	if got := counts.users.Load(); got != 2 {
		t.Fatalf("discovery turn exceeded group+one users-query budget: got=%d want=2", got)
	}
	if got := counts.logs.Load(); got != 1 {
		t.Fatalf("only the isolated member should reach boundary log query: got=%d want=1", got)
	}
	for index, id := range []int64{101, 102, 103} {
		var job UsageFactJob
		if err := m.usageFactsStore().First(&job, "id = ?", usageFactHistoryJobID(id, 1)).Error; err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			if job.Status == usageFactHistoryJobRunning || job.Kind != usageFactHistoryKindVerify {
				t.Fatalf("attempted first member did not advance: %+v", job)
			}
			continue
		}
		if job.Status != usageFactHistoryJobQueued || job.Kind != usageFactHistoryKindDiscover ||
			job.Attempts != 0 || job.NextRetryAt != 0 || job.LeaseOwner != "" {
			t.Fatalf("unattempted sibling consumed failure/backoff: %+v", job)
		}
	}
}

func TestUsageFactSourceBoundaryUsesMigratedLogBeforeRegistration(t *testing.T) {
	registered := time.Date(2026, 5, 11, 9, 0, 0, 0, usageCST).Unix()
	migrated := time.Date(2026, 5, 9, 23, 0, 0, 0, usageCST).Unix()
	boundary := usageFactSourceBoundary{RegisteredAt: registered, FirstLogAt: migrated}
	floor, ok := boundary.sourceFloorHour()
	if !ok || floor != usageFactDayStart(migrated) {
		t.Fatalf("migrated log before registration must extend the logical floor: floor=%d ok=%v", floor, ok)
	}
}

func TestFetchUsageFactHistoryRangeMatchesIndependentControlAndMaterializesZeroDays(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	if _, err := m.prodDB.Exec(`INSERT INTO logs
(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`+"`group`"+`,token_id,token_name,other)
VALUES
(1,1,10,?,2,'m-a',1000,100,20,'g-a',5,'token-a',''),
(2,1,10,?,2,'m-a',2000,200,30,'g-a',5,'token-a',''),
(3,1,11,?,6,'m-b',300,0,0,'g-b',6,'token-b','')`, day+60, day+3600, day+7200); err != nil {
		t.Fatal(err)
	}

	result, err := m.fetchUsageFactHistoryRange(context.Background(), day, day+2*usageFactDaySeconds, []int64{2, 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.SourceQueries != 2 || result.Rows != 2 || len(result.Controls) != 4 {
		t.Fatalf("unexpected range metadata: queries=%d rows=%d controls=%d", result.SourceQueries, result.Rows, len(result.Controls))
	}
	key := usageFactMemberDayKey{userID: 1, dayTs: day}
	control := result.Controls[key]
	if control.SourceRows != 3 || control.Requests != 2 || control.RefundRecords != 1 ||
		control.PromptTokens != 300 || control.CompletionTokens != 50 || control.ConsumeQuota != 3000 || control.RefundQuota != 300 {
		t.Fatalf("control totals wrong: %+v", control)
	}
	if rows := result.Facts[key]; len(rows) != 2 || rows[0].ChannelID != 10 || rows[1].ChannelID != 11 {
		t.Fatalf("dimensional facts wrong: %+v", rows)
	}
	for _, zeroKey := range []usageFactMemberDayKey{
		{userID: 2, dayTs: day},
		{userID: 1, dayTs: day + usageFactDaySeconds},
		{userID: 2, dayTs: day + usageFactDaySeconds},
	} {
		if zero := result.Controls[zeroKey]; zero.SourceRows != 0 || zero.Requests != 0 {
			t.Fatalf("zero member-day not materialized: key=%+v control=%+v", zeroKey, zero)
		}
		if len(result.Facts[zeroKey]) != 0 {
			t.Fatalf("zero member-day unexpectedly has facts: key=%+v", zeroKey)
		}
	}
}

func TestValidateUsageFactHistoryResultRejectsControlMismatch(t *testing.T) {
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	key := usageFactMemberDayKey{userID: 1, dayTs: day}
	result := usageFactHistoryRange{
		FromTs: day, ThroughTs: day + usageFactDaySeconds, UserIDs: []int64{1},
		Facts: map[usageFactMemberDayKey][]UsageDailyFact{
			key: {{DateTs: day, UserID: 1, ChannelID: 1, Requests: 1, ConsumeQuota: 100}},
		},
		Controls: map[usageFactMemberDayKey]usageFactHistoryControl{
			key: {UserID: 1, DateTs: day, SourceRows: 1, Requests: 1, ConsumeQuota: 101},
		},
	}
	if err := validateUsageFactHistoryResult(&result); !errors.Is(err, errUsageFactHistoryControl) {
		t.Fatalf("control mismatch must fail closed: %v", err)
	}
	result.Controls[key] = usageFactHistoryControl{UserID: 1, DateTs: day, SourceRows: 2, Requests: 1, ConsumeQuota: 100}
	if err := validateUsageFactHistoryResult(&result); !errors.Is(err, errUsageFactHistoryControl) {
		t.Fatalf("source row mismatch must fail closed: %v", err)
	}
}

func TestUsageFactHistoryGuardsRangeBatchAndInteractivePriority(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	if _, err := validateUsageFactHistoryRange(day+1, day+usageFactDaySeconds, []int64{1}); err == nil {
		t.Fatal("unaligned range must be rejected")
	}
	if _, err := validateUsageFactHistoryRange(day, day+8*usageFactDaySeconds, []int64{1}); err == nil {
		t.Fatal("range larger than seven days must be rejected")
	}
	ids := make([]int64, usageFactHistoryMaxMembers+1)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	if _, err := validateUsageFactHistoryRange(day, day+usageFactDaySeconds, ids); err == nil {
		t.Fatal("member batch larger than limit must be rejected")
	}
	m.usageInteractiveWaiters.Store(1)
	_, err := m.fetchUsageFactHistoryRange(context.Background(), day, day+usageFactDaySeconds, []int64{1})
	if !errors.Is(err, errUsageFactSourceBusy) {
		t.Fatalf("interactive waiter must preempt cold history: %v", err)
	}
}

func TestUsageFactHistoryDayIndexRoundTripAcrossCSTBoundary(t *testing.T) {
	for _, day := range []time.Time{
		time.Date(2024, 2, 29, 0, 0, 0, 0, usageCST),
		time.Date(2026, 12, 31, 0, 0, 0, 0, usageCST),
	} {
		index := (day.Unix() + usageTZOffsetSec) / usageFactDaySeconds
		if got := usageFactHistoryDayIndexToTs(index); got != day.Unix() {
			t.Fatalf("day roundtrip failed: %s got=%d want=%d", day, got, day.Unix())
		}
	}
}

func TestUsageFactSourceAuditAdaptiveBudgetIsolatesWithoutPenalizingUnattemptedMembers(t *testing.T) {
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	ids := []int64{1, 2, 3, 4}
	budget := usageFactSourceAuditFetchBudget
	success := make(map[int64]usageFactHistoryRange)
	failed := make(map[int64]error)
	queries := 0
	fetch := func(_ context.Context, from, through int64, batch []int64) (usageFactHistoryRange, error) {
		queries++
		containsBad := false
		for _, id := range batch {
			containsBad = containsBad || id == 1
		}
		if containsBad {
			return usageFactHistoryRange{}, errUsageFactHistoryControl
		}
		return usageFactHistoryRange{FromTs: from, ThroughTs: through, UserIDs: append([]int64(nil), batch...)}, nil
	}
	fetchUsageFactSourceAuditAdaptiveWith(context.Background(), day, day+usageFactDaySeconds,
		ids, &budget, success, failed, fetch)
	if queries > usageFactSourceAuditFetchBudget {
		t.Fatalf("adaptive source audit exceeded per-turn budget: %d", queries)
	}
	if !errors.Is(failed[1], errUsageFactHistoryControl) || len(success) != 0 {
		t.Fatalf("lowest bad member was not isolated within the bounded turn: success=%v failed=%v", success, failed)
	}
	if !errors.Is(failed[2], errUsageFactAdaptiveBudget) || !errors.Is(failed[3], errUsageFactAdaptiveBudget) ||
		!errors.Is(failed[4], errUsageFactAdaptiveBudget) ||
		!usageFactHistoryFailureIsSourceGlobal(errUsageFactAdaptiveBudget) {
		t.Fatalf("unattempted members must be released without attempts: failed=%v", failed)
	}

	// Once completed/isolated jobs leave this cursor, the next bounded turn
	// reaches the previously unattempted right side instead of restarting an
	// unbounded split tree.
	nextBudget := usageFactSourceAuditFetchBudget
	nextSuccess := make(map[int64]usageFactHistoryRange)
	nextFailed := make(map[int64]error)
	fetchUsageFactSourceAuditAdaptiveWith(context.Background(), day, day+usageFactDaySeconds,
		[]int64{3, 4}, &nextBudget, nextSuccess, nextFailed, fetch)
	if len(nextSuccess) != 2 || len(nextFailed) != 0 {
		t.Fatalf("deferred healthy members did not progress next turn: success=%v failed=%v", nextSuccess, nextFailed)
	}
}

func TestUsageFactSourceAuditWorkloadFallbackKeepsSignedMemberVisible(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 41, 1)
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	now := time.Unix(day+3*usageFactDaySeconds, 0)
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", 41).Updates(map[string]any{
		"source_epoch": epoch, "classification_version": userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageFactPublishedMember{
		UserID: 41, TrackedRevision: 1, SourceEpoch: epoch,
		ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion,
		SourceFloorHour: day, VerifiedThroughHour: day + 2*usageFactDaySeconds, PublishedAt: now.Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	uid := int64(41)
	job := UsageFactJob{
		ID: usageFactSourceAuditJobID(uid, 1), Kind: usageFactHistoryKindSourceAudit, Priority: 5,
		UserID: &uid, TrackedRevision: 1, SourceEpoch: epoch, FromTs: day,
		ThroughTs: day + 2*usageFactDaySeconds, NextHour: day,
		TotalHours: 48, Status: usageFactHistoryJobRunning, LeaseOwner: "source-audit",
		LeaseUntil: now.Add(time.Minute).Unix(), CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
	}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	claim := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: job.LeaseOwner,
		From: day, Through: day + usageFactDaySeconds}
	if err := m.downgradeUsageFactSourceAuditToHourly(context.Background(), claim,
		errUsageFactHistoryRangeTooLarge, now); err != nil {
		t.Fatal(err)
	}
	var got UsageFactJob
	if err := m.usageFactsStore().First(&got, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if got.Kind != usageFactHistoryKindAuditHour || got.Status != usageFactHistoryJobQueued ||
		got.NextHour != day || got.VerifyNextHour != day+usageFactDaySeconds || got.Attempts != 0 {
		t.Fatalf("source audit fallback was not durably downgraded: %+v", got)
	}
	var published int64
	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Where("user_id = ?", uid).Count(&published).Error; err != nil {
		t.Fatal(err)
	}
	activeRepairs, err := loadUsageFactActiveRepairUsers(m.usageFactsStore())
	if err != nil {
		t.Fatal(err)
	}
	if published != 1 || activeRepairs[uid] {
		t.Fatalf("an incomplete audit revoked the signed member: published=%d repairs=%v", published, activeRepairs)
	}
	// The regular reconciliation pass must not overwrite the durable hourly
	// cursor back to a daily audit before its 24 hours finish.
	desired := job
	desired.Status = usageFactHistoryJobQueued
	if err := m.usageFactsStore().Transaction(func(tx *gorm.DB) error {
		return upsertUsageFactAuditJob(tx, desired, usageFactHistorySourceAuditCycle, now.Add(time.Hour))
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&got, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if got.Kind != usageFactHistoryKindAuditHour || got.VerifyNextHour != day+usageFactDaySeconds {
		t.Fatalf("audit reconciliation erased hourly fallback: %+v", got)
	}
}

func TestUsageFactSourceAuditHourlyVerificationAtomicallyRepairsAndReturnsToCycle(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 42, 1)
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	through := day + usageFactDaySeconds
	hour23 := through - usageFactHourSeconds
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	now := time.Unix(through+usageFactHourSeconds, 0)
	if _, err := m.prodDB.Exec("INSERT INTO users(id,username,email,created_at) VALUES(42,'u42','u42@x',?)", day); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec(`INSERT INTO logs
(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`+"`group`"+`,token_id,token_name,other)
VALUES(1,42,10,?,2,'m',1000,10,20,'g',5,'t','')`, hour23+60); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", 42).Updates(map[string]any{
		"source_epoch": epoch, "source_history_status": "complete_hot", "coverage_status": "ready",
		"classification_version":  userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	old := UsageDailyFact{DateTs: day, UserID: 42, ChannelID: 10, Grp: "g", ModelName: "m",
		TokenID: 5, TokenName: "t", Requests: 1, PromptTokens: 1, CompletionTokens: 1, ConsumeQuota: 1}
	if err := m.usageFactsStore().Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	oldHash := usageDailyFactContentHash([]UsageDailyFact{old})
	if err := m.usageFactsStore().Create(&UsageFactMemberDayState{
		UserID: 42, DateTs: day, Status: "complete", Rows: 1, SourceRows: 1, Requests: 1,
		PromptTokens: 1, CompletionTokens: 1, Tokens: 2, ConsumeQuota: 1,
		ContentHash: oldHash, SourceResultHash: "old-source", FactContentHash: oldHash,
		ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion,
		SourceEpoch: epoch, SourceCheckedAt: now.Unix(), CompletedAt: now.Unix(), UpdatedAt: now.Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	emptyHash := usageFactContentHash(nil)
	proofs := make([]UsageFactMemberHourState, 0, 23)
	for hour := day; hour < hour23; hour += usageFactHourSeconds {
		proofs = append(proofs, UsageFactMemberHourState{UserID: 42, HourTs: hour, Status: "complete",
			ContentHash: emptyHash, SourceEpoch: epoch, CompletedAt: now.Unix(), UpdatedAt: now.Unix()})
	}
	if err := m.usageFactsStore().CreateInBatches(proofs, 100).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageFactPublishedMember{
		UserID: 42, TrackedRevision: 1, SourceEpoch: epoch,
		ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion,
		SourceFloorHour: day, VerifiedThroughHour: through, PublishedAt: now.Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"traffic_class_version": userTrafficClassificationVersion,
		"published_fingerprint": portalMemberFingerprintFromIDs([]int64{42}),
		"published_window_days": 1, "published_range_start": day, "published_through": through,
		"published_at": now.Unix(), "generation": int64(10), "serving_generation": int64(10),
	}).Error; err != nil {
		t.Fatal(err)
	}
	m.publishUsageFactGenerations(10, 10)
	uid := int64(42)
	job := UsageFactJob{
		ID: usageFactSourceAuditJobID(uid, 1), Kind: usageFactHistoryKindAuditHour, Priority: 5,
		UserID: &uid, TrackedRevision: 1, SourceEpoch: epoch, FromTs: day, ThroughTs: through,
		NextHour: hour23, VerifyNextHour: through, TotalHours: 24, CompletedHours: 23,
		Status: usageFactHistoryJobRunning, LeaseOwner: "hour-audit", LeaseUntil: now.Add(time.Minute).Unix(),
		CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
	}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	claim := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: job.LeaseOwner,
		From: hour23, Through: through}
	if err := m.executeUsageFactHistorySourceAuditHour(context.Background(), claim, now); err != nil {
		t.Fatal(err)
	}
	var got UsageFactJob
	if err := m.usageFactsStore().First(&got, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if got.Kind != usageFactHistoryKindSourceAudit || got.Status != usageFactHistoryJobComplete ||
		got.NextHour != through || got.VerifyNextHour != 0 || got.CompletedHours != 24 {
		t.Fatalf("hourly audit did not return to its durable source cycle: %+v", got)
	}
	var daily []UsageDailyFact
	if err := m.usageFactsStore().Where("user_id = ? AND date_ts = ?", 42, day).Find(&daily).Error; err != nil {
		t.Fatal(err)
	}
	if len(daily) != 1 || daily[0].ConsumeQuota != 1000 || daily[0].PromptTokens != 10 || daily[0].CompletionTokens != 20 {
		t.Fatalf("hourly audit did not atomically replace the changed daily fact: %+v", daily)
	}
	var stagedHours, published int64
	if err := m.usageFactsStore().Model(&UsageFactMemberHourState{}).Where("user_id = ? AND hour_ts >= ? AND hour_ts < ?", 42, day, through).Count(&stagedHours).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Where("user_id = ?", 42).Count(&published).Error; err != nil {
		t.Fatal(err)
	}
	if stagedHours != 0 || published != 1 {
		t.Fatalf("hourly audit leaked staging or revoked the signed member: staged=%d published=%d", stagedHours, published)
	}
}

func TestUsageFactPruneKeepsActiveHourlyAuditStagingUntilFinalized(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 43, 1)
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, usageCST)
	day := usageFactDayStart(now.AddDate(0, 0, -10).Unix())
	hour := day + 7*usageFactHourSeconds
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	if err := m.usageFactsStore().Create(&UsageFactMemberDayState{
		UserID: 43, DateTs: day, Status: "complete", ContentHash: "signed-day",
		SourceResultHash: "signed-source", FactContentHash: "signed-day",
		ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion,
		SourceEpoch: epoch, SourceCheckedAt: now.Unix(), CompletedAt: now.Unix(), UpdatedAt: now.Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	row := UsageHourFact{HourTs: hour, DayTs: day, UserID: 43, ChannelID: 1, ModelName: "staging",
		TokenID: 1, Requests: 1, ConsumeQuota: 10}
	if err := m.usageFactsStore().Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageFactMemberHourState{
		UserID: 43, HourTs: hour, Status: "complete", Rows: 1, Requests: 1,
		ContentHash: usageFactContentHash([]UsageHourFact{row}), SourceEpoch: epoch,
		CompletedAt: now.Unix(), UpdatedAt: now.Unix(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	uid := int64(43)
	job := UsageFactJob{
		ID: usageFactSourceAuditJobID(uid, 1), Kind: usageFactHistoryKindAuditHour, UserID: &uid,
		TrackedRevision: 1, SourceEpoch: epoch, FromTs: day, ThroughTs: day + 30*usageFactDaySeconds,
		NextHour: hour + usageFactHourSeconds, VerifyNextHour: day + usageFactDaySeconds,
		Status: usageFactHistoryJobQueued, CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
	}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Update("last_pruned_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.pruneUsageFacts(now); err != nil {
		t.Fatal(err)
	}
	countStaging := func() (int64, int64) {
		t.Helper()
		var facts, proofs int64
		if err := m.usageFactsStore().Model(&UsageHourFact{}).Where("user_id = ? AND hour_ts = ?", uid, hour).Count(&facts).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.usageFactsStore().Model(&UsageFactMemberHourState{}).Where("user_id = ? AND hour_ts = ?", uid, hour).Count(&proofs).Error; err != nil {
			t.Fatal(err)
		}
		return facts, proofs
	}
	if facts, proofs := countStaging(); facts != 1 || proofs != 1 {
		t.Fatalf("active hourly audit staging was pruned: facts=%d proofs=%d", facts, proofs)
	}
	if err := m.usageFactsStore().Model(&UsageFactJob{}).Where("id = ?", job.ID).
		Updates(map[string]any{"status": usageFactHistoryJobComplete, "completed_at": now.Unix()}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Update("last_pruned_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.pruneUsageFacts(now.Add(25 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	if facts, proofs := countStaging(); facts != 0 || proofs != 0 {
		t.Fatalf("terminal hourly audit staging was not pruned: facts=%d proofs=%d", facts, proofs)
	}
}

func TestUsageFactHistoryTailConsumesCurrentEpochHourWithoutSourceQuery(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 9, 1)
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	hour := day + 10*usageFactHourSeconds
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", 9).Updates(map[string]any{
		"source_epoch": epoch, "classification_version": userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	rows := []UsageHourFact{{HourTs: hour, DayTs: day, UserID: 9, ChannelID: 1, ModelName: "m",
		TokenID: 1, Requests: 1, PromptTokens: 2, CompletionTokens: 1, ConsumeQuota: 100}}
	if err := m.usageFactsStore().Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	metrics := factsMetrics(rows)
	if err := m.usageFactsStore().Create(&UsageFactMemberHourState{
		UserID: 9, HourTs: hour, Status: "complete", Rows: int(metrics.Rows), Requests: metrics.Requests,
		Tokens: metrics.tokens(), ContentHash: usageFactContentHash(rows), SourceEpoch: epoch,
	}).Error; err != nil {
		t.Fatal(err)
	}
	userID := int64(9)
	job := UsageFactJob{ID: usageFactHistoryJobID(userID, 1), Kind: usageFactHistoryKindTail,
		UserID: &userID, TrackedRevision: 1, SourceEpoch: epoch, FromTs: day, ThroughTs: hour + usageFactHourSeconds,
		NextHour: hour, Status: usageFactHistoryJobRunning, LeaseOwner: "tail-owner", LeaseUntil: time.Now().Add(time.Minute).Unix(),
		CreatedAt: time.Now().Unix(), UpdatedAt: time.Now().Unix()}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	// A source access would panic/fail; the current-epoch local proof must be the
	// sole input used to advance this durable cursor.
	m.prodDB = nil
	claim := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: job.LeaseOwner,
		From: hour, Through: hour + usageFactHourSeconds}
	if err := m.executeUsageFactHistoryTail(context.Background(), claim, time.Unix(hour+2*usageFactHourSeconds, 0)); err != nil {
		t.Fatal(err)
	}
	var got UsageFactJob
	if err := m.usageFactsStore().First(&got, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if got.NextHour != job.ThroughTs || got.Attempts != 0 || got.Status == usageFactHistoryJobPaused {
		t.Fatalf("local proof did not advance history tail losslessly: %+v", got)
	}
}

func TestUsageFactHistoryMidnightIsolatesLocalBadMemberWithOneControlQuery(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	countingDB, counts := newCountingFakeProdDB(t)
	m.prodDB = countingDB
	prepareUsageHistoryCommitMember(t, m, 91, 1)
	prepareUsageHistoryCommitMember(t, m, 92, 1)

	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	hour23 := day + 23*usageFactHourSeconds
	through := day + usageFactDaySeconds
	now := time.Unix(through+usageFactHourSeconds, 0)
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	emptyHash := usageFactContentHash(nil)
	proofs := make([]UsageFactMemberHourState, 0, 48)
	for _, id := range []int64{91, 92} {
		for hour := day; hour < through; hour += usageFactHourSeconds {
			// User 92 has one corrupt/missing local hour. User 91 must still be
			// finalized by the one batched independent source control query.
			if id == 92 && hour == day+7*usageFactHourSeconds {
				continue
			}
			proofs = append(proofs, UsageFactMemberHourState{
				UserID: id, HourTs: hour, Status: "complete", ContentHash: emptyHash,
				SourceEpoch: epoch, CompletedAt: now.Unix(), UpdatedAt: now.Unix(),
			})
		}
		if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", id).Updates(map[string]any{
			"source_epoch": epoch, "source_history_status": "complete_hot", "coverage_status": "tailing",
			"classification_version":  userTrafficClassificationVersion,
			"query_semantics_version": usageFactQuerySemanticsVersion,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.usageFactsStore().CreateInBatches(proofs, 100).Error; err != nil {
		t.Fatal(err)
	}

	owner := "midnight-isolation"
	jobs := make([]UsageFactJob, 0, 2)
	for _, id := range []int64{91, 92} {
		uid := id
		job := UsageFactJob{
			ID: usageFactHistoryJobID(uid, 1), Kind: usageFactHistoryKindTail, UserID: &uid,
			TrackedRevision: 1, SourceEpoch: epoch, FromTs: day, ThroughTs: through, NextHour: hour23,
			TotalHours: 24, CompletedHours: 23, Status: usageFactHistoryJobRunning,
			LeaseOwner: owner, LeaseUntil: now.Add(time.Minute).Unix(), CreatedAt: now.Unix(), UpdatedAt: now.Unix(),
		}
		if err := m.usageFactsStore().Create(&job).Error; err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, job)
	}
	counts.reset()
	claim := usageFactHistoryClaim{Jobs: jobs, LeaseOwner: owner, From: hour23, Through: through}
	if err := m.executeUsageFactHistoryTail(context.Background(), claim, now); err == nil {
		t.Fatal("missing local hour must remain a member-scoped failure")
	}
	if got := counts.logs.Load(); got != 1 {
		t.Fatalf("one local bad member amplified source day-control queries: got=%d want=1", got)
	}

	var healthy, bad UsageFactJob
	if err := m.usageFactsStore().First(&healthy, "id = ?", usageFactHistoryJobID(91, 1)).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&bad, "id = ?", usageFactHistoryJobID(92, 1)).Error; err != nil {
		t.Fatal(err)
	}
	if healthy.NextHour != through || healthy.Attempts != 0 || healthy.Kind != usageFactHistoryKindVerify {
		t.Fatalf("healthy member did not finalize/advance independently: %+v", healthy)
	}
	if bad.NextHour != hour23 || bad.Attempts != 1 || bad.Status == usageFactHistoryJobComplete {
		t.Fatalf("bad member was not isolated at its own cursor: %+v", bad)
	}
	var healthyProof UsageFactMemberDayState
	if err := m.usageFactsStore().First(&healthyProof, "user_id = ? AND date_ts = ?", 91, day).Error; err != nil {
		t.Fatal(err)
	}
	if !usageFactMemberDayHistoryReady(healthyProof) || healthyProof.SourceEpoch != epoch {
		t.Fatalf("healthy member lacks strict day proof: %+v", healthyProof)
	}
	var badProofs int64
	if err := m.usageFactsStore().Model(&UsageFactMemberDayState{}).
		Where("user_id = ? AND date_ts = ?", 92, day).Count(&badProofs).Error; err != nil {
		t.Fatal(err)
	}
	if badProofs != 0 {
		t.Fatalf("bad member received a day proof: %d", badProofs)
	}
}

func TestUsageFactHistoryManualDayRepairIsRootOnlyClosedAndDurablyIdempotent(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	m.RegisterRoutes(router)
	userID := int64(111)
	prepareUsageHistoryCommitMember(t, m, userID, 1)
	now := time.Now()
	today := usageFactDayStart(now.Unix())
	day := today - 2*usageFactDaySeconds
	floor := today - 10*usageFactDaySeconds
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	nowUnix := now.Unix()
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
		"source_floor_hour": floor, "source_floor_checked_at": nowUnix, "source_history_status": "complete_hot",
		"source_epoch": epoch, "coverage_status": "ready", "coverage_through_hour": today,
		"tail_through_hour": today, "verification_status": "complete", "verify_next_hour": today,
		"verified_through_hour": today, "verified_at": nowUnix,
		"classification_version":  userTrafficClassificationVersion,
		"query_semantics_version": usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&UsageFactPublishedMember{
		UserID: userID, TrackedRevision: 1, SourceEpoch: epoch,
		ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion,
		SourceFloorHour: floor, VerifiedThroughHour: today, PublishedAt: nowUnix,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"traffic_class_version": userTrafficClassificationVersion,
		"published_fingerprint": portalMemberFingerprintFromIDs([]int64{userID}),
		"published_range_start": floor, "published_through": today,
		"published_window_days": 10, "published_at": nowUnix,
		"generation": 3, "serving_generation": 3,
	}).Error; err != nil {
		t.Fatal(err)
	}

	dayText := time.Unix(day, 0).In(usageCST).Format("2006-01-02")
	requestID := "manual-fh-day-repair-111"
	body := fmt.Sprintf(`{"user_id":%d,"day":%q,"reason":"verified mismatch","request_id":%q,"confirm":"REPAIR_FULL_HISTORY_DAY"}`,
		userID, dayText, requestID)
	adminCookie := &http.Cookie{Name: sessionCookie, Value: m.signSession("admin", roleAdmin, nowUnix)}
	if w := lifecycleAdminDo(router, http.MethodPost, "/usage/facts-history/repair", body, requestID, adminCookie); w.Code != http.StatusForbidden {
		t.Fatalf("non-root exact repair status=%d body=%s", w.Code, w.Body.String())
	}
	rootCookie := &http.Cookie{Name: sessionCookie, Value: m.signSession("root", roleRoot, nowUnix)}
	if w := lifecycleAdminDo(router, http.MethodPost, "/usage/facts-history/repair", body, requestID, rootCookie); w.Code != http.StatusAccepted {
		t.Fatalf("root exact repair status=%d body=%s", w.Code, w.Body.String())
	}
	jobID := usageFactRepairJobID(userID, 1, day)
	var job UsageFactJob
	if err := m.usageFactsStore().First(&job, "id = ?", jobID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != usageFactHistoryJobQueued || job.FromTs != day || job.ThroughTs != day+usageFactDaySeconds ||
		job.RequestedBy != "root" {
		t.Fatalf("manual exact repair job is not narrowly scoped: %+v", job)
	}
	var published int64
	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Where("user_id = ?", userID).Count(&published).Error; err != nil {
		t.Fatal(err)
	}
	if published != 0 {
		t.Fatal("repair target remained published")
	}

	// Simulate completion, then retry the exact same HTTP request. The durable
	// request receipt must return that terminal job without reopening it.
	if err := m.usageFactsStore().Model(&UsageFactJob{}).Where("id = ?", jobID).Updates(map[string]any{
		"status": usageFactHistoryJobComplete, "attempts": 4, "completed_at": nowUnix,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if w := lifecycleAdminDo(router, http.MethodPost, "/usage/facts-history/repair", body, requestID, rootCookie); w.Code != http.StatusAccepted {
		t.Fatalf("idempotent replay status=%d body=%s", w.Code, w.Body.String())
	}
	if err := m.usageFactsStore().First(&job, "id = ?", jobID).Error; err != nil {
		t.Fatal(err)
	}
	if job.Status != usageFactHistoryJobComplete || job.Attempts != 4 {
		t.Fatalf("same request_id reopened completed repair: %+v", job)
	}
	var receipts, jobs int64
	if err := m.usageFactsStore().Model(&UsageFactRepairRequest{}).Count(&receipts).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactJob{}).Where("id = ?", jobID).Count(&jobs).Error; err != nil {
		t.Fatal(err)
	}
	if receipts != 1 || jobs != 1 {
		t.Fatalf("HTTP replay duplicated durable work: receipts=%d jobs=%d", receipts, jobs)
	}

	otherDayBody := fmt.Sprintf(`{"user_id":%d,"day":%q,"reason":"verified mismatch","request_id":%q,"confirm":"REPAIR_FULL_HISTORY_DAY"}`,
		userID, time.Unix(day-usageFactDaySeconds, 0).In(usageCST).Format("2006-01-02"), requestID)
	if w := lifecycleAdminDo(router, http.MethodPost, "/usage/facts-history/repair", otherDayBody, requestID, rootCookie); w.Code != http.StatusConflict {
		t.Fatalf("reused request_id for another day status=%d body=%s", w.Code, w.Body.String())
	}
	currentDayBody := fmt.Sprintf(`{"user_id":%d,"day":%q,"reason":"bad current day","request_id":"manual-current-day","confirm":"REPAIR_FULL_HISTORY_DAY"}`,
		userID, time.Unix(today, 0).In(usageCST).Format("2006-01-02"))
	if w := lifecycleAdminDo(router, http.MethodPost, "/usage/facts-history/repair", currentDayBody, "manual-current-day", rootCookie); w.Code != http.StatusBadRequest {
		t.Fatalf("unclosed day repair status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestUsageFactHistoryLeaseBusyNeverConsumesDurableAttempts(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	userID := int64(11)
	now := time.Now()
	job := UsageFactJob{ID: "fh-lease-busy", Kind: usageFactHistoryKindTail, UserID: &userID,
		TrackedRevision: 1, SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch, Status: usageFactHistoryJobRunning,
		LeaseOwner: "owner", LeaseUntil: now.Add(time.Minute).Unix(), Attempts: 4, CreatedAt: now.Unix(), UpdatedAt: now.Unix()}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	claim := usageFactHistoryClaim{Jobs: []UsageFactJob{job}, LeaseOwner: job.LeaseOwner}
	if !usageFactHistoryFailureIsSourceGlobal(errUsageFactLeaseBusy) {
		t.Fatal("local hour lease collision must be a no-penalty coordination result")
	}
	if err := m.releaseUsageFactHistoryClaim(context.Background(), claim, errUsageFactLeaseBusy, now, true); err != nil {
		t.Fatal(err)
	}
	var got UsageFactJob
	if err := m.usageFactsStore().First(&got, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	if got.Status != usageFactHistoryJobQueued || got.Attempts != 4 || got.NextRetryAt != 0 {
		t.Fatalf("lease collision consumed an attempt/backoff: %+v", got)
	}
}

func TestUsageFactSemanticResetCancelsAncillaryMaintenanceJobs(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 19, 1)
	now := time.Date(2026, 5, 20, 12, 0, 0, 0, usageCST)
	nowUnix := now.Unix()
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", 19).Updates(map[string]any{
		"classification_version": 0, "query_semantics_version": 0, "source_epoch": epoch,
		"coverage_status": "ready", "verification_status": "complete", "updated_at": nowUnix,
	}).Error; err != nil {
		t.Fatal(err)
	}
	userID := int64(19)
	primary := UsageFactJob{
		ID: usageFactHistoryJobID(userID, 1), Kind: usageFactHistoryKindVerify, UserID: &userID,
		TrackedRevision: 1, SourceEpoch: epoch, Status: usageFactHistoryJobComplete,
		CreatedAt: nowUnix, UpdatedAt: nowUnix, CompletedAt: nowUnix,
	}
	ancillary := []UsageFactJob{
		{ID: "fhr-semantic-old", Kind: usageFactHistoryKindRepair, UserID: &userID, TrackedRevision: 1,
			SourceEpoch: epoch, Status: usageFactHistoryJobRunning, LeaseOwner: "old-repair", LeaseUntil: now.Add(time.Hour).Unix()},
		{ID: "fhl-semantic-old", Kind: usageFactHistoryKindLocalAudit, UserID: &userID, TrackedRevision: 1,
			SourceEpoch: epoch, Status: usageFactHistoryJobComplete, CompletedAt: nowUnix},
		{ID: "fhs-semantic-old", Kind: usageFactHistoryKindSourceAudit, UserID: &userID, TrackedRevision: 1,
			SourceEpoch: epoch, Status: usageFactHistoryJobPaused, Attempts: 5},
	}
	for i := range ancillary {
		ancillary[i].CreatedAt, ancillary[i].UpdatedAt = nowUnix, nowUnix
	}
	if err := m.usageFactsStore().Create(&primary).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&ancillary).Error; err != nil {
		t.Fatal(err)
	}

	if err := m.reconcileUsageFactHistoryJobs(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var gotPrimary UsageFactJob
	if err := m.usageFactsStore().First(&gotPrimary, "id = ?", primary.ID).Error; err != nil {
		t.Fatal(err)
	}
	if gotPrimary.Status != usageFactHistoryJobQueued || gotPrimary.Kind != usageFactHistoryKindDiscover || gotPrimary.CompletedAt != 0 {
		t.Fatalf("primary coverage job was not rebuilt from discovery: %+v", gotPrimary)
	}
	for _, old := range ancillary {
		var got UsageFactJob
		if err := m.usageFactsStore().First(&got, "id = ?", old.ID).Error; err != nil {
			t.Fatal(err)
		}
		if got.Status != usageFactHistoryJobCancelled || got.LeaseOwner != "" || got.LeaseUntil != 0 {
			t.Fatalf("old-semantic maintenance job remained claimable: %+v", got)
		}
	}
	var state UsageFactMemberState
	if err := m.usageFactsStore().First(&state, "user_id = ?", userID).Error; err != nil {
		t.Fatal(err)
	}
	if state.ClassificationVersion != userTrafficClassificationVersion ||
		state.QuerySemanticsVersion != usageFactQuerySemanticsVersion || state.CoverageStatus != "discovering" {
		t.Fatalf("member was relabelled without a fresh discovery checkpoint: %+v", state)
	}
}

func TestNoHistoryInvalidationPublishesRemainingMemberBounds(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 21, 1)
	prepareUsageHistoryCommitMember(t, m, 22, 1)
	hour := time.Date(2026, 5, 21, 10, 0, 0, 0, usageCST).Unix()
	earlyFloor := usageFactDayStart(hour - 10*usageFactDaySeconds)
	keptFloor := usageFactDayStart(hour - 5*usageFactDaySeconds)
	through := hour + 2*usageFactHourSeconds
	nowUnix := time.Now().Unix()
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", 21).Updates(map[string]any{
		"source_history_status": "no_history", "coverage_status": "ready", "verification_status": "complete",
		"source_floor_hour": earlyFloor, "source_floor_checked_at": nowUnix, "source_epoch": epoch,
		"classification_version": userTrafficClassificationVersion, "query_semantics_version": usageFactQuerySemanticsVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	uid := int64(21)
	job := UsageFactJob{
		ID: usageFactHistoryJobID(uid, 1), Kind: usageFactHistoryKindVerify, UserID: &uid,
		TrackedRevision: 1, SourceEpoch: epoch, FromTs: earlyFloor, ThroughTs: through, NextHour: through,
		Status: usageFactHistoryJobComplete, CreatedAt: nowUnix, UpdatedAt: nowUnix, CompletedAt: nowUnix,
	}
	if err := m.usageFactsStore().Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	published := []UsageFactPublishedMember{
		{UserID: 21, TrackedRevision: 1, SourceFloorHour: earlyFloor, PublishedAt: nowUnix},
		{UserID: 22, TrackedRevision: 1, SourceFloorHour: keptFloor, PublishedAt: nowUnix},
	}
	if err := m.usageFactsStore().Create(&published).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"traffic_class_version": userTrafficClassificationVersion,
		"published_fingerprint": portalMemberFingerprintFromIDs([]int64{21, 22}),
		"published_window_days": 10, "published_range_start": earlyFloor, "published_through": through,
		"published_at": nowUnix, "generation": 10, "serving_generation": 10,
	}).Error; err != nil {
		t.Fatal(err)
	}
	m.publishUsageFactGenerations(10, 10)
	m.setUsageFactsPublishedReadiness(true, earlyFloor, through)
	if _, err := m.prodDB.Exec(`INSERT INTO logs
(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`+"`group`"+`,token_id,token_name)
VALUES(1,21,1,?,2,'m',100,1,1,'g',1,'t')`, hour+1); err != nil {
		t.Fatal(err)
	}

	result, err := m.syncUsageFactHourWithOptions(context.Background(), hour, []int64{21}, usageFactHourSyncOptions{
		invalidateNoHistory: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.InvalidatedNoHistoryUserIDs) != 1 || result.InvalidatedNoHistoryUserIDs[0] != 21 {
		t.Fatalf("no-history member was not invalidated: %+v", result)
	}
	var state UsageFactSyncState
	if err := m.usageFactsStore().First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.PublishedRangeStart != keptFloor || m.usageFactsReadyFrom.Load() != keptFloor ||
		m.usageFactsReadyThrough.Load() != state.PublishedThrough {
		t.Fatalf("remaining publication bounds were not atomically exposed: db=%+v atomic_from=%d atomic_through=%d",
			state, m.usageFactsReadyFrom.Load(), m.usageFactsReadyThrough.Load())
	}
}

func TestProvenMismatchReopensUnaffectedPublishedMembers(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsReadEnabled = true
	prepareUsageHistoryCommitMember(t, m, 31, 1)
	prepareUsageHistoryCommitMember(t, m, 32, 1)
	now := time.Date(2026, 5, 22, 1, 0, 0, 0, usageCST)
	through := time.Date(2026, 5, 22, 0, 0, 0, 0, usageCST).Unix()
	aFloor := through - 10*usageFactDaySeconds
	bFloor := through - 5*usageFactDaySeconds
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	nowUnix := now.Unix()
	seedReady := func(userID, floor int64) {
		t.Helper()
		if err := m.usageFactsStore().Model(&UsageFactMemberState{}).Where("user_id = ?", userID).Updates(map[string]any{
			"source_floor_hour": floor, "source_first_log_hour": nil, "source_ceiling_hour": nil,
			"source_floor_checked_at": nowUnix, "source_history_status": "no_history", "source_epoch": epoch,
			"coverage_status": "ready", "coverage_through_hour": through, "tail_through_hour": through,
			"verification_status": "complete", "verify_next_hour": through, "verified_through_hour": through,
			"verified_at": nowUnix, "classification_version": userTrafficClassificationVersion,
			"query_semantics_version": usageFactQuerySemanticsVersion, "updated_at": nowUnix,
		}).Error; err != nil {
			t.Fatal(err)
		}
		uid := userID
		job := UsageFactJob{
			ID: usageFactHistoryJobID(uid, 1), Kind: usageFactHistoryKindVerify, UserID: &uid,
			TrackedRevision: 1, SourceEpoch: epoch, FromTs: floor, ThroughTs: through,
			NextHour: through, VerifyNextHour: through, TotalHours: (through - floor) / usageFactHourSeconds,
			CompletedHours: (through - floor) / usageFactHourSeconds, VerifiedHours: (through - floor) / usageFactHourSeconds,
			Status: usageFactHistoryJobComplete, CreatedAt: nowUnix, UpdatedAt: nowUnix, CompletedAt: nowUnix,
		}
		if err := m.usageFactsStore().Create(&job).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.usageFactsStore().Create(&UsageFactPublishedMember{
			UserID: uid, TrackedRevision: 1, SourceEpoch: epoch,
			ClassificationVersion: userTrafficClassificationVersion, QuerySemanticsVersion: usageFactQuerySemanticsVersion,
			SourceFloorHour: floor, VerifiedThroughHour: through, PublishedAt: nowUnix,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	seedReady(31, aFloor)
	seedReady(32, bFloor)
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"traffic_class_version": userTrafficClassificationVersion,
		"published_fingerprint": portalMemberFingerprintFromIDs([]int64{31, 32}),
		"published_window_days": 10, "published_range_start": aFloor, "published_through": through,
		"published_at": nowUnix, "generation": 20, "serving_generation": 20,
	}).Error; err != nil {
		t.Fatal(err)
	}
	m.publishUsageFactGenerations(20, 20)
	m.setUsageFactsPublishedReadiness(true, aFloor, through)
	aID := int64(31)
	audit := UsageFactJob{
		ID: "fhl-test-31", Kind: usageFactHistoryKindLocalAudit, UserID: &aID,
		TrackedRevision: 1, SourceEpoch: epoch, FromTs: aFloor, ThroughTs: through,
		NextHour: aFloor, Status: usageFactHistoryJobRunning, LeaseOwner: "audit-owner",
		LeaseUntil: now.Add(time.Minute).Unix(), CreatedAt: nowUnix, UpdatedAt: nowUnix,
	}
	if err := m.usageFactsStore().Create(&audit).Error; err != nil {
		t.Fatal(err)
	}
	badDay := through - usageFactDaySeconds
	cause := &usageFactMemberDayAuditError{UserID: 31, DayTs: badDay, Kind: "test mismatch"}
	if err := m.persistProvenUsageFactMismatch(audit, audit.LeaseOwner, 31, badDay, cause,
		"test mismatch", "test", now); err != nil {
		t.Fatal(err)
	}
	if !m.usageFactsReadEnabled() || m.usageFactsReadyFrom.Load() != bFloor || m.usageFactsReadyThrough.Load() != through {
		t.Fatalf("unaffected member did not resume on the reduced signed snapshot: ready=%v from=%d through=%d",
			m.usageFactsReadEnabled(), m.usageFactsReadyFrom.Load(), m.usageFactsReadyThrough.Load())
	}
	var publishedIDs []int64
	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Order("user_id").Pluck("user_id", &publishedIDs).Error; err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(publishedIDs) != "[32]" {
		t.Fatalf("repairing member remained published: %v", publishedIDs)
	}
}

func TestFullHistoryPublisherClearsMetadataWithLastStaleMember(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	m.cfg.UsageFactsReadEnabled = true
	prepareUsageHistoryCommitMember(t, m, 41, 1)
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, usageCST)
	through := m.usageFactFinalizedHour(now)
	floor := usageFactDayStart(through - 5*usageFactDaySeconds)
	nowUnix := now.Unix()
	if err := m.usageFactsStore().Create(&UsageFactPublishedMember{
		UserID: 41, TrackedRevision: 1, SourceEpoch: "stale-source-epoch",
		ClassificationVersion: userTrafficClassificationVersion,
		QuerySemanticsVersion: usageFactQuerySemanticsVersion,
		SourceFloorHour:       floor, VerifiedThroughHour: usageFactDayStart(through), PublishedAt: nowUnix,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
		"traffic_class_version": userTrafficClassificationVersion,
		"published_fingerprint": portalMemberFingerprintFromIDs([]int64{41}),
		"published_window_days": 5, "published_range_start": floor, "published_through": through,
		"published_at": nowUnix, "generation": 7, "serving_generation": 7,
	}).Error; err != nil {
		t.Fatal(err)
	}
	m.publishUsageFactGenerations(7, 7)
	m.setUsageFactsPublishedReadiness(true, floor, through)

	published, err := m.publishUsageFactFullHistorySnapshot(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	if published.PublishedAt != 0 || published.PublishedFingerprint != "" || published.PublishedWindowDays != 0 ||
		published.PublishedRangeStart != 0 || published.PublishedThrough != 0 {
		t.Fatalf("last stale member left contradictory publication metadata: %+v", published)
	}
	var count int64
	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 || m.usageFactsReadReady.Load() || published.ServingGeneration != 8 {
		t.Fatalf("empty publication was not atomically fail-closed: rows=%d ready=%v generation=%d",
			count, m.usageFactsReadReady.Load(), published.ServingGeneration)
	}
}

func prepareUsageHistoryCommitMember(t *testing.T, m *Monitor, userID int64, revision int64) {
	t.Helper()
	if err := m.storeDB.Create(&TrackedUser{UserID: userID, Username: fmt.Sprintf("u%d", userID), AddedAt: time.Now().Unix()}).Error; err != nil {
		t.Fatal(err)
	}
	if revision != 1 {
		if err := m.storeDB.Model(&UsageMemberControl{}).Where("user_id = ?", userID).Update("tracked_revision", revision).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.usageFactsStore().Create(&UsageFactMemberState{
		UserID: userID, Active: true, TrackedRevision: revision, CoverageStatus: "backfilling",
	}).Error; err != nil {
		t.Fatal(err)
	}
}

func TestCommitUsageFactHistoryRangeIsAtomicIdempotentAndVersioned(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 1, 1)
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	if _, err := m.prodDB.Exec(`INSERT INTO logs
(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`+"`group`"+`,token_id,token_name,other)
VALUES(1,1,10,?,2,'m-a',1000,100,20,'g-a',5,'token-a','')`, day+60); err != nil {
		t.Fatal(err)
	}
	result, err := m.fetchUsageFactHistoryRange(context.Background(), day, day+usageFactDaySeconds, []int64{1})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.commitUsageFactHistoryRange(context.Background(), result, "history-job-1", map[int64]int64{1: 1}); err != nil {
		t.Fatal(err)
	}
	var facts []UsageDailyFact
	if err := m.usageFactsStore().Where("user_id = ? AND date_ts = ?", 1, day).Find(&facts).Error; err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].ConsumeQuota != 1000 {
		t.Fatalf("committed facts wrong: %+v", facts)
	}
	var proof UsageFactMemberDayState
	if err := m.usageFactsStore().First(&proof, "user_id = ? AND date_ts = ?", 1, day).Error; err != nil {
		t.Fatal(err)
	}
	if !usageFactMemberDayHistoryReady(proof) || proof.SourceRows != 1 || proof.Requests != 1 || proof.JobID != "history-job-1" || proof.Attempts != 1 {
		t.Fatalf("history proof is not publishable: %+v", proof)
	}
	var state UsageFactSyncState
	if err := m.usageFactsStore().First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	firstGeneration := state.Generation
	if err := m.commitUsageFactHistoryRange(context.Background(), result, "history-job-1", map[int64]int64{1: 1}); err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().First(&proof, "user_id = ? AND date_ts = ?", 1, day).Error; err != nil {
		t.Fatal(err)
	}
	if proof.Attempts != 2 {
		t.Fatalf("idempotent replacement must preserve attempt audit: %+v", proof)
	}
	var factCount int64
	if err := m.usageFactsStore().Model(&UsageDailyFact{}).Where("user_id = ? AND date_ts = ?", 1, day).Count(&factCount).Error; err != nil {
		t.Fatal(err)
	}
	if factCount != 1 {
		t.Fatalf("idempotent replacement duplicated facts: %d", factCount)
	}
	if err := m.usageFactsStore().First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.Generation != firstGeneration+1 {
		t.Fatalf("each atomic candidate replacement must advance one generation: before=%d after=%d", firstGeneration, state.Generation)
	}
}

func TestCommitUsageFactHistoryRangeRejectsStaleMemberRevision(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 1, 1)
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	result := usageFactHistoryRange{
		FromTs: day, ThroughTs: day + usageFactDaySeconds, UserIDs: []int64{1},
		Facts: map[usageFactMemberDayKey][]UsageDailyFact{},
		Controls: map[usageFactMemberDayKey]usageFactHistoryControl{
			{userID: 1, dayTs: day}: {UserID: 1, DateTs: day},
		},
	}
	if err := m.storeDB.Model(&UsageMemberControl{}).Where("user_id = ?", 1).Update("tracked_revision", 2).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.commitUsageFactHistoryRange(context.Background(), result, "stale-job", map[int64]int64{1: 1}); !errors.Is(err, errUsageMemberControlIntegrity) {
		t.Fatalf("stale history job must be fenced: %v", err)
	}
	var count int64
	if err := m.usageFactsStore().Model(&UsageFactMemberDayState{}).Where("user_id = ? AND date_ts = ?", 1, day).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("stale job wrote a proof: %d", count)
	}
}

func TestCommitUsageFactHistoryRangeRollsBackDeleteOnInsertFailure(t *testing.T) {
	m := newUsageHistoryTestMonitor(t)
	prepareUsageHistoryCommitMember(t, m, 1, 1)
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	old := UsageDailyFact{DateTs: day, UserID: 1, ChannelID: 9, Grp: "old", ModelName: "old", TokenID: 9, Requests: 9, ConsumeQuota: 900}
	if err := m.usageFactsStore().Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	oldProof := UsageFactMemberDayState{UserID: 1, DateTs: day, ContentHash: "old-proof", UpdatedAt: 1}
	if err := m.usageFactsStore().Create(&oldProof).Error; err != nil {
		t.Fatal(err)
	}
	duplicate := UsageDailyFact{DateTs: day, UserID: 1, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, Requests: 1, ConsumeQuota: 100}
	key := usageFactMemberDayKey{userID: 1, dayTs: day}
	result := usageFactHistoryRange{
		FromTs: day, ThroughTs: day + usageFactDaySeconds, UserIDs: []int64{1}, Rows: 2,
		Facts: map[usageFactMemberDayKey][]UsageDailyFact{key: {duplicate, duplicate}},
		Controls: map[usageFactMemberDayKey]usageFactHistoryControl{
			key: {UserID: 1, DateTs: day, SourceRows: 2, Requests: 2, ConsumeQuota: 200},
		},
	}
	if err := m.commitUsageFactHistoryRange(context.Background(), result, "duplicate-job", map[int64]int64{1: 1}); err == nil {
		t.Fatal("duplicate primary keys must fail the facts transaction")
	}
	var got UsageDailyFact
	if err := m.usageFactsStore().First(&got, "user_id = ? AND date_ts = ?", 1, day).Error; err != nil {
		t.Fatal(err)
	}
	if got.ChannelID != old.ChannelID || got.Requests != old.Requests || got.ConsumeQuota != old.ConsumeQuota {
		t.Fatalf("old day was not rolled back: %+v", got)
	}
	var proof UsageFactMemberDayState
	if err := m.usageFactsStore().First(&proof, "user_id = ? AND date_ts = ?", 1, day).Error; err != nil {
		t.Fatal(err)
	}
	if proof.ContentHash != "old-proof" {
		t.Fatalf("old proof was not rolled back: %+v", proof)
	}
}

func BenchmarkValidateUsageFactHistoryResult50By7(b *testing.B) {
	day := time.Date(2026, 5, 11, 0, 0, 0, 0, usageCST).Unix()
	ids := make([]int64, usageFactHistoryMaxMembers)
	result := usageFactHistoryRange{
		FromTs: day, ThroughTs: day + usageFactHistoryMaxDays*usageFactDaySeconds,
		Facts:    make(map[usageFactMemberDayKey][]UsageDailyFact),
		Controls: make(map[usageFactMemberDayKey]usageFactHistoryControl),
	}
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	result.UserIDs = ids
	for n := 0; n < b.N; n++ {
		if err := validateUsageFactHistoryResult(&result); err != nil {
			b.Fatal(fmt.Errorf("validate: %w", err))
		}
	}
}
