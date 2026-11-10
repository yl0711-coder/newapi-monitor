package monitor

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenDetectionBudgetYieldContinuesBacklogWithoutIdlePoll(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	for id := int64(1); id <= 160; id++ {
		detectionFact(t, m, id, now-600, 3, 20000, 0, 0)
		if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id=?", id).Update("user_id", id+1000).Error; err != nil {
			t.Fatal(err)
		}
	}
	m.cfg.TokenProcessingBatchRows, m.cfg.TokenProcessingPollSeconds = 50, 60
	m.cfg.TokenFactsEnabled = false // This test isolates the real local detector loop.
	started := make(chan time.Time, 4)
	type finish struct {
		at      time.Time
		reason  string
		inserts int64
	}
	finished := make(chan finish, 4)
	var inserts atomic.Int64
	if err := m.tokenStore().Callback().Create().Before("gorm:create").Register("budget_detection_cost", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_detection_currents" {
			inserts.Add(1)
			waitSourceLifecycle(tx.Statement.Context, 5*time.Millisecond)
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.tokenStore().Callback().Create().Remove("budget_detection_cost"); err != nil {
			t.Error(err)
		}
	}()
	if err := m.tokenStore().Callback().Update().Before("gorm:update").Register("budget_detection_round", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_detection_states" {
			return
		}
		fields, ok := tx.Statement.Dest.(map[string]any)
		if !ok {
			return
		}
		lease, ok := fields["lease_token"].(string)
		if !ok {
			return
		}
		if lease != "" {
			select {
			case started <- time.Now():
			default:
			}
		} else {
			reason, _ := fields["last_reason"].(string)
			select {
			case finished <- finish{time.Now(), reason, inserts.Load()}:
			default:
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.tokenStore().Callback().Update().Remove("budget_detection_round"); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		until := time.Now().Add(2 * time.Second)
		for time.Now().Before(until) {
			if m.tokenDetectionMu.TryLock() {
				m.tokenDetectionMu.Unlock()
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Error("canceled detection did not release its round")
	}()
	m.startTokenMeteringMaintenanceAt(ctx, func() time.Time { return time.Unix(now, 0) })
	var first time.Time
	select {
	case first = <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("detector did not start")
	}
	var yielded finish
	select {
	case yielded = <-finished:
	case <-time.After(7 * time.Second):
		t.Fatal("detector did not finish a bounded round")
	}
	if yielded.reason != "" || yielded.at.Before(first) || yielded.inserts != 50 {
		t.Fatal("fixture did not exhaust its row budget with retained work", yielded.reason, yielded.at.Sub(first), yielded.inserts)
	}
	select {
	case second := <-started:
		if second.Sub(yielded.at) >= time.Second {
			t.Fatal("successful budget yield idled the retained backlog", second.Sub(yielded.at))
		}
	case <-time.After(time.Second):
		t.Fatal("successful budget yield inserted an idle poll before unfinished work")
	}
}

func TestTokenDetectionTimeBudgetReportsRemainingWork(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	for id := int64(1); id <= 40; id++ {
		detectionFact(t, m, id, now-600, 3, 20000, 0, 0)
	}
	if err := m.tokenStore().Callback().Create().Before("gorm:create").Register("bounded_detection_cost", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_detection_currents" {
			waitSourceLifecycle(tx.Statement.Context, 40*time.Millisecond)
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.tokenStore().Callback().Create().Remove("bounded_detection_cost"); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	processed, remaining, err := m.runTokenDetectionRoundProgress(ctx, now)
	if err != nil || !remaining || processed <= 0 || processed >= 40 {
		t.Fatal("a successful bounded prefix lost its continuation", processed, remaining, err)
	}
	var persisted int64
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Count(&persisted).Error; err != nil || persisted != int64(processed) {
		t.Fatal("progress includes uncommitted results", persisted, processed, err)
	}
	var state TokenDetectionState
	if err := m.tokenStore().First(&state).Error; err != nil || state.LastReason != "" || state.LastFailed != 0 || state.LeaseToken != "" {
		t.Fatal("a cooperative yield became a failure or retained its lease", state, err)
	}
	if err := m.tokenStore().Callback().Create().Remove("bounded_detection_cost"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.runTokenDetectionRound(context.Background(), now+1); err != nil {
		t.Fatal("committed prefix did not resume", err)
	}
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Count(&persisted).Error; err != nil || persisted != 40 {
		t.Fatal("remaining identities were lost after a cooperative yield", persisted, err)
	}
}

func TestTokenDetectionMissingEvidenceWaitsForChangedFacts(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	fact := detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	length := *fact.RequestLength
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id=?", fact.LogID).
		Updates(map[string]any{"link_state": "waiting", "evidence_level": "unavailable", "length_state": "missing", "request_length": nil}).Error; err != nil {
		t.Fatal(err)
	}
	if n, err := m.runTokenDetectionRound(context.Background(), now); err != nil || n != 1 {
		t.Fatal("association wait blocked independent rules", n, err)
	}
	initial, initialResult := detectionHead(t, m, fact.LogID)
	if initial.DetectionState != "partially_evaluated" || !initialResult.Rules[0].Eligible || initialResult.Rules[1].Eligible || !initialResult.Rules[3].Eligible {
		t.Fatal("independent checks lost association gap", initial, initialResult.Rules)
	}
	// Association changes must re-evaluate the same head, even if length is
	// still missing. Time alone must not repeatedly rewrite that result.
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id=?", fact.LogID).
		Updates(map[string]any{"link_state": "unique", "link_revision": fact.LinkRevision + 1}).Error; err != nil {
		t.Fatal(err)
	}
	if n, err := m.runTokenDetectionRound(context.Background(), now+1); err != nil || n != 1 {
		t.Fatal("initial evidence-gap evaluation", n, err)
	}
	var head TokenDetectionCurrent
	if err := m.tokenStore().First(&head).Error; err != nil {
		t.Fatal(err)
	}
	frozen := detectionFrozenRead(t, m, fact.LogID, head.Revision)
	var result tokenDetectionResult
	if err := json.Unmarshal([]byte(head.ResultJSON), &result); err != nil || result.Rules[1].Eligible || result.Reference.Reason != "comparable_fields_missing" || head.NextAt != 0 {
		t.Fatal("missing evidence was hidden or repeatedly polled", head.NextAt, result, err)
	}
	if n, err := m.runTokenDetectionRound(context.Background(), now+120); err != nil || n != 0 {
		t.Fatal("time alone re-evaluated unchanged missing evidence", n, err)
	}
	// Older retained heads can have the former timer. Clear only scheduling;
	// keep the decision revision and its original frozen evidence unchanged.
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Where("log_id=?", fact.LogID).Update("next_at", now+60).Error; err != nil {
		t.Fatal(err)
	}
	if n, err := m.runTokenDetectionRound(context.Background(), now+120); err != nil || n != 1 {
		t.Fatal("legacy insufficient timer did not drain", n, err)
	}
	var retained TokenDetectionCurrent
	if err := m.tokenStore().First(&retained).Error; err != nil || retained.Revision != head.Revision || retained.DecisionHash != head.DecisionHash || retained.NextAt != 0 {
		t.Fatal("retiring a legacy timer altered the evidence", retained, err)
	}
	finalLinkRevision := fact.LinkRevision + 2
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id=?", fact.LogID).
		Updates(map[string]any{"link_state": "unique", "evidence_level": "exact", "length_state": "valid", "request_length": length, "link_revision": finalLinkRevision}).Error; err != nil {
		t.Fatal(err)
	}
	if n, err := m.runTokenDetectionRound(context.Background(), now+121); err != nil || n != 1 {
		t.Fatal("new association evidence did not resume detection", n, err)
	}
	if err := m.tokenStore().First(&head).Error; err != nil || head.LinkRevision != finalLinkRevision || head.NextAt <= now+121 {
		t.Fatal("comparable evidence did not restore staged reference checks", head, err)
	}
	if got := detectionFrozenRead(t, m, fact.LogID, frozen.Revision); got.FrozenJSON != frozen.FrozenJSON {
		t.Fatal("evidence repair rewrote the original decision")
	}
}

func TestTokenDetectionLegacyContractTimerReevaluates(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	fact := detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id=?", fact.LogID).
		Update("request_length", nil).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now)
	head, result := detectionHead(t, m, fact.LogID)
	if !result.Price.Supported {
		t.Fatal("fixture needs the former supported price")
	}
	// Reproduce a legacy guessed operand which is rejected at consumption
	// before the bounded migration has reached this fact's revision.
	parsed := tokenMeteringDecode(t, fact)
	unit := parsed.Fields["quota_per_unit"]
	unit.Kind = "source_contract_v1"
	parsed.Fields["quota_per_unit"] = unit
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id=?", fact.LogID).
		Update("source_facts", tokenDetectionJSON(parsed)).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Where("log_id=?", fact.LogID).Update("next_at", now+60).Error; err != nil {
		t.Fatal(err)
	}
	detectionRun(t, m, now+120)
	updated, result := detectionHead(t, m, fact.LogID)
	if updated.Revision <= head.Revision || result.Price.Supported || updated.NextAt != 0 {
		t.Fatal("legacy timer optimization kept an unverified price", updated.Revision, result.Price, updated.NextAt)
	}
}
