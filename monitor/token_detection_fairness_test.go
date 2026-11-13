package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// Use the real one-minute cadence after retries reach the one-hour ceiling.
// All fixtures and database writes are local, synthetic diagnostic evidence.
func TestIndependentReviewMinuteCadenceCapacityDoesNotStarveNormal(t *testing.T) {
	m, now, _, normal := detectionFairnessFixture(t)
	policy := detectionPolicy()
	for minute := int64(0); minute < 120; minute++ {
		_, err := m.runTokenDetectionRound(context.Background(), now+minute*60)
		if err != nil && !errors.Is(err, errTokenDetectionCapacity) {
			t.Fatal(err)
		}
		var count int64
		if err := m.storeDB.Model(&TokenDetectionCurrent{}).Where("source_epoch = ? AND log_id = ?", normal.SourceEpoch, normal.LogID).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			t.Logf("normal work progressed after %d minutes", minute)
			return
		}
	}
	// Control: the normal job succeeds through the exact production processor.
	// This distinguishes scheduling starvation from a global capacity failure.
	at := now + 120*60
	if err := m.storeDB.Model(&TokenDetectionState{}).Where("source_epoch = ?", normal.SourceEpoch).Updates(map[string]any{"lease_token": "independent-control", "lease_until": at + 30}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.processTokenDetection(context.Background(), normal.SourceEpoch, normal.LogID, policy, "independent-control", at); err != nil {
		t.Fatalf("invalid fixture, normal job itself cannot run: %v", err)
	}
	t.Fatal("processable normal request starved for 120 real-cadence rounds: 1500 older one-hour retries always fill the 25 changed-job slots")
}

func detectionFairnessFixture(t *testing.T) (*Monitor, int64, string, TokenMeteringFact) {
	t.Helper()
	return detectionFairnessFixtureSize(t, 1500)
}

func detectionFairnessFixtureSize(t *testing.T, backlog int64) (*Monitor, int64, string, TokenMeteringFact) {
	t.Helper()
	m, now, dbPath := detectionFixture(t)
	base := detectionFact(t, m, 1, now-100, 1, 10000, 0, 0)
	if err := m.storeDB.Where("source_epoch = ?", base.SourceEpoch).Delete(&TokenMeteringFact{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Where("source_epoch = ?", base.SourceEpoch).Delete(&TokenMeteringRef{}).Error; err != nil {
		t.Fatal(err)
	}
	policy := detectionPolicy()
	hash := tokenDetectionPolicyHash(policy)
	facts := make([]TokenMeteringFact, 0, backlog*4+1)
	heads := make([]TokenDetectionCurrent, 0, backlog*3)
	retries := make([]TokenDetectionRetry, 0, backlog)
	for i := int64(0); i < backlog; i++ {
		for j := int64(1); j <= 4; j++ {
			f := base
			f.LogID = i*10 + j
			f.UserID = 10000 + i
			f.CreatedAt = now - 600 + j
			f.ChannelID = 1 + j%2
			if j == 4 {
				f.CreatedAt, f.ChannelID = now-100, 3
				retries = append(retries, TokenDetectionRetry{SourceEpoch: f.SourceEpoch, LogID: f.LogID, FactRevision: f.Revision, LinkRevision: f.LinkRevision, PolicyHash: hash, Attempts: 7, NextAt: now, ExpiresAt: now + 86400, Reason: "local_capacity_exhausted"})
			} else {
				// Already settled seeds; only the newer target requests are queued.
				heads = append(heads, TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: f.LogID, CreatedAt: f.CreatedAt, UserID: f.UserID, ChannelID: f.ChannelID, Model: f.Model, UserGroup: f.UserGroup, FactRevision: f.Revision, LinkRevision: f.LinkRevision, PolicyHash: hash, Revision: 1, Stage: 3, DetectionState: "evidence_insufficient", FirstEvaluatedAt: now - 10, ExpiresAt: now + 86400, ResultJSON: "{}"})
			}
			facts = append(facts, f)
		}
	}
	normal := base
	normal.LogID, normal.UserID, normal.CreatedAt = 999999, 999999, now-1
	normal.RequestLength = nil
	facts = append(facts, normal)
	for _, rows := range []any{&facts, &heads, &retries} {
		if err := m.storeDB.CreateInBatches(rows, 100).Error; err != nil {
			t.Fatal(err)
		}
	}
	detectionReferenceCapacity(t, m, now+86400)
	return m, now, dbPath, normal
}

func TestTokenDetectionFairnessMinuteBacklogRestartRecovery(t *testing.T) {
	m, now, dbPath, normal := detectionFairnessFixture(t)
	policyHash := tokenDetectionPolicyHash(detectionPolicy())
	// Independent groups keep correction/review changes from altering the
	// reference sufficiency of the 1500 capacity-blocked target requests.
	const correctionID, reviewID = int64(2000001), int64(2000002)
	for _, id := range []int64{correctionID, reviewID} {
		f := normal
		f.LogID, f.UserID, f.CreatedAt = id, id, now-600
		if err := m.storeDB.Create(&f).Error; err != nil {
			t.Fatal(err)
		}
		c := TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: id, CreatedAt: f.CreatedAt, UserID: id, ChannelID: f.ChannelID, Model: f.Model, UserGroup: f.UserGroup, FactRevision: f.Revision, LinkRevision: f.LinkRevision, PolicyHash: policyHash, Revision: 1, Stage: 3, DetectionState: "evidence_insufficient", FirstEvaluatedAt: now - 10, ExpiresAt: now + 86400, ResultJSON: "{}"}
		if id == reviewID {
			c.Stage, c.NextAt = 0, now
		}
		if err := m.storeDB.Create(&c).Error; err != nil {
			t.Fatal(err)
		}
	}
	detectionCorrectQuota(t, m, correctionID)
	for minute := int64(0); minute < 70; minute++ {
		at := now + minute*60
		if minute == 35 {
			m = detectionReviewReopen(t, m, dbPath)
		}
		if minute%10 == 0 {
			f := normal
			f.LogID = 1000000 + minute
			f.UserID = f.LogID
			f.CreatedAt = at - 1
			if err := m.storeDB.Create(&f).Error; err != nil {
				t.Fatal(err)
			}
		}
		jobs, err := tokenDetectionJobs(m.storeDB, normal.SourceEpoch, policyHash, at)
		if err != nil || len(jobs) > 50 {
			t.Fatalf("bounded selection: %d %v", len(jobs), err)
		}
		seen := map[int64]bool{}
		for _, j := range jobs {
			if seen[j.LogID] {
				t.Fatal("duplicate queue assignment", j.LogID)
			}
			seen[j.LogID] = true
		}
		if _, err = m.runTokenDetectionRound(context.Background(), at); err != nil && !errors.Is(err, errTokenDetectionCapacity) {
			t.Fatal(err)
		}
		if minute%10 == 0 {
			var c TokenDetectionCurrent
			if err = m.storeDB.First(&c, "source_epoch=? AND log_id=?", normal.SourceEpoch, 1000000+minute).Error; err != nil {
				t.Fatalf("new work starved at minute %d: %v", minute, err)
			}
		}
		if minute == 0 {
			var corrected, reviewed TokenDetectionCurrent
			if err = m.storeDB.First(&corrected, "source_epoch=? AND log_id=?", normal.SourceEpoch, correctionID).Error; err != nil {
				t.Fatal(err)
			}
			if err = m.storeDB.First(&reviewed, "source_epoch=? AND log_id=?", normal.SourceEpoch, reviewID).Error; err != nil {
				t.Fatal(err)
			}
			if corrected.FactRevision != 2 || reviewed.Stage != 1 {
				t.Fatalf("correction/review starved: %d %d", corrected.FactRevision, reviewed.Stage)
			}
		}
	}
	var untouched int64
	if err := m.storeDB.Model(&TokenDetectionRetry{}).Where("next_at <= ?", now).Count(&untouched).Error; err != nil || untouched != 0 {
		t.Fatalf("old retries never got a slot: %d %v", untouched, err)
	}
	var blocked int64
	if err := m.storeDB.Model(&TokenDetectionRetry{}).Count(&blocked).Error; err != nil || blocked != 1500 {
		t.Fatalf("fixture no longer retains all 1500 capacity-blocked requests: %d %v", blocked, err)
	}
	m = detectionReviewReopen(t, m, dbPath)
	if err := m.storeDB.Model(&TokenDetectionReference{}).Where("bucket=?", "capacity-bucket").Update("expires_at", now).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenDetection(context.Background(), now+8000); err != nil {
		t.Fatal(err)
	}
	// Capacity recovery must make durable progress under the same budgets.
	// Full lifecycle throughput of 1500 independent reference cohorts is a
	// separate load test, not an assumed 80-minute scheduling SLA.
	remaining := detectionFairnessRecover(t, m, dbPath, normal.SourceEpoch, now+8000, 1500, 8)
	if remaining == 0 || remaining >= 1500 {
		t.Fatalf("large-backlog recovery must retain and advance pending work: %d", remaining)
	}
}

func TestTokenDetectionFairnessCapacityRecoveryDrains(t *testing.T) {
	const backlog = int64(60) // More than two per-lane selection batches.
	m, now, dbPath, normal := detectionFairnessFixtureSize(t, backlog)
	for minute := int64(0); minute < 4; minute++ {
		if _, err := m.runTokenDetectionRound(context.Background(), now+minute*60); err != nil && !errors.Is(err, errTokenDetectionCapacity) {
			t.Fatalf("capacity pressure must remain visible: %v", err)
		}
	}
	var untouched int64
	if err := m.storeDB.Model(&TokenDetectionRetry{}).Where("next_at <= ?", now).Count(&untouched).Error; err != nil || untouched != 0 {
		t.Fatalf("retry rotation missed work: %d %v", untouched, err)
	}
	m = detectionReviewReopen(t, m, dbPath)
	if err := m.storeDB.Model(&TokenDetectionReference{}).Where("bucket=?", "capacity-bucket").Update("expires_at", now).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenDetection(context.Background(), now+8000); err != nil {
		t.Fatal(err)
	}
	if remaining := detectionFairnessRecover(t, m, dbPath, normal.SourceEpoch, now+8000, backlog, 20); remaining != 0 {
		t.Fatalf("capacity recovered but persisted retries did not drain: %d", remaining)
	}
}

// Exercise the production worker, including partial rounds and a database
// reopen, without treating a deadline or an outstanding retry as success.
func detectionFairnessRecover(t *testing.T, m *Monitor, dbPath, epoch string, at, backlog, rounds int64) int64 {
	t.Helper()
	lastPending, stalled, budgetYields := backlog, 0, 0
	var first TokenDetectionRevision
	for minute := int64(0); minute < rounds; minute++ {
		if minute == 4 {
			m = detectionReviewReopen(t, m, dbPath)
		}
		// SQLite can report its interrupt error rather than wrapping the
		// context error. Independently observe the same five-second budget.
		roundCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		processed, err := m.runTokenDetectionRound(roundCtx, at+minute*60)
		budgetExpired := errors.Is(roundCtx.Err(), context.DeadlineExceeded)
		cancel()
		if err != nil && !budgetExpired {
			t.Fatal(err)
		}
		if processed < 0 || processed > 50 {
			t.Fatal("round exceeded the existing count budget", processed)
		}
		var state TokenDetectionState
		if err := m.storeDB.First(&state, "source_epoch=?", epoch).Error; err != nil {
			t.Fatal(err)
		}
		if state.LeaseToken != "" || state.LeaseUntil != 0 {
			t.Fatal("finished or budget-limited round pinned the lease", state.LeaseUntil)
		}
		var pending, completed int64
		if err := m.storeDB.Model(&TokenDetectionRetry{}).Count(&pending).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND log_id < ? AND log_id % 10 = 4", epoch, backlog*10).Count(&completed).Error; err != nil {
			t.Fatal(err)
		}
		if completed+pending != backlog || pending > lastPending {
			t.Fatalf("recovery lost or duplicated work: completed=%d pending=%d previous=%d", completed, pending, lastPending)
		}
		if pending < lastPending {
			stalled = 0
		} else {
			stalled++
		}
		if stalled >= 4 && pending > 0 {
			t.Fatal("recovered retries made no progress across all four minute positions")
		}
		lastPending = pending
		if err != nil && budgetExpired {
			if state.LastReason != "local_detection_incomplete" {
				t.Fatal("budget expiry was reported as success", state.LastReason)
			}
			budgetYields++
		} else if pending > 0 && state.LastReason != "local_capacity_exhausted" {
			t.Fatal("pending retry disappeared from health", state.LastReason)
		} else if pending == 0 && state.LastReason != "" {
			t.Fatal("fully recovered round retained stale capacity health", state.LastReason)
		}
		if completed > 0 && first.Revision == 0 {
			if err := m.storeDB.Where("source_epoch=? AND log_id < ? AND log_id % 10 = 4 AND revision=1", epoch, backlog*10).Order("log_id").First(&first).Error; err != nil {
				t.Fatal(err)
			}
			var frozen tokenDetectionFrozen
			if err := json.Unmarshal([]byte(first.FrozenJSON), &frozen); err != nil || frozen.Result.Reference.Status != "statistical" {
				t.Fatalf("recovery bypassed the statistical reference capacity: %s %v", frozen.Result.Reference.Status, err)
			}
		}
		// Continue past reopening even when the smaller backlog drains early.
		if pending == 0 && minute >= 4 {
			break
		}
	}
	if first.Revision == 0 {
		t.Fatal("recovered reference was never frozen")
	}
	m = detectionReviewReopen(t, m, dbPath)
	if got := detectionFrozenRead(t, m, first.LogID, first.Revision); got.FrozenJSON != first.FrozenJSON {
		t.Fatal("recovery rewrote the first frozen reference")
	}
	t.Logf("recovery after %d-minute budget: completed=%d pending=%d bounded yields=%d", rounds, backlog-lastPending, lastPending, budgetYields)
	return lastPending
}

func TestTokenDetectionFairnessReservesAllFourLanes(t *testing.T) {
	m, now, _, normal := detectionFairnessFixture(t)
	for i := int64(0); i < 60; i++ {
		f := normal
		f.LogID = 1000000 + i
		f.UserID = f.LogID
		if err := m.storeDB.Create(&f).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Model(&TokenMeteringFact{}).Where("log_id % 10 = 1").Update("revision", 2).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Where("log_id % 10 = 2").Update("next_at", now).Error; err != nil {
		t.Fatal(err)
	}
	for minute := int64(0); minute < 4; minute++ {
		jobs, err := tokenDetectionJobs(m.storeDB, normal.SourceEpoch, tokenDetectionPolicyHash(detectionPolicy()), now+minute*60)
		if err != nil || len(jobs) != 50 {
			t.Fatalf("budget: %d %v", len(jobs), err)
		}
		counts := [4]int{}
		laneOf := func(id int64) int {
			if id >= 999999 {
				return 0
			}
			switch id % 10 {
			case 1:
				return 1
			case 4:
				return 2
			case 2:
				return 3
			default:
				t.Fatalf("unexpected job %d", id)
				return -1
			}
		}
		for _, job := range jobs {
			counts[laneOf(job.LogID)]++
		}
		for lane, count := range counts {
			// These corrected facts are recent. They reserve the same primary
			// share as first evaluations, while retries and reviews still run.
			minimum := []int{18, 18, 1, 1}[lane]
			if count < minimum {
				t.Fatalf("lane %d has %d slots: %v", lane, count, counts)
			}
		}
		if lane := laneOf(jobs[0].LogID); lane != 0 && lane != 1 {
			t.Fatal("recent first evaluation/correction did not lead the round")
		}
	}
}
