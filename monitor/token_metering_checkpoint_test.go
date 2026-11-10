package monitor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTokenMeteringBudgetYieldScheduleContentionDoesNotAddIdleMinute(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	state, claimed, err := m.claimTokenMetering(context.Background(), now)
	if err != nil || !claimed {
		t.Fatal("claim", claimed, err)
	}
	w, found, err := m.nextTokenMeteringWindow(context.Background(), state.SourceEpoch, state.LeaseToken, now, false)
	if err != nil || !found {
		t.Fatal("window", found, err)
	}
	if err := m.failTokenMeteringWindow(context.Background(), state.LeaseToken, w, now, "source_read_failed"); err != nil {
		t.Fatal(err)
	}
	m.tokenLocalWriteMu.Lock()
	delay := m.tokenMeteringRoundDelay(context.Background(), 0, nil, now)
	m.tokenLocalWriteMu.Unlock()
	if delay != 2*time.Second {
		t.Fatal("schedule contention became source backoff", delay)
	}
	// An early wake is only a scheduling hint. The failed window stays blocked.
	if _, found, err := m.nextTokenMeteringWindow(context.Background(), state.SourceEpoch, state.LeaseToken, now+2, false); err != nil || found {
		t.Fatal("early wake bypassed persisted source backoff", found, err)
	}
	if err := m.tokenStore().Migrator().DropTable(&TokenMeteringState{}); err != nil {
		t.Fatal(err)
	}
	if delay := m.tokenMeteringRoundDelay(context.Background(), 0, nil, now); delay != time.Minute {
		t.Fatal("schema failure was treated as a budget yield", delay)
	}
}

func TestTokenMeteringWriterWaitDoesNotAddIdleMinute(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	wait := &tokenLocalWaitError{err: context.DeadlineExceeded}
	if got := m.tokenMeteringRoundDelay(context.Background(), 0, wait, now); got != 2*time.Second {
		t.Fatalf("writer scheduling wait became a minute of collection idle time: %s", got)
	}
	m.tokenMeteringPartialPage.Store(true)
	if got := m.tokenMeteringRoundDelay(context.Background(), 0, wait, now); got != 100*time.Millisecond {
		t.Fatalf("writer wait after a committed suffix did not resume promptly: %s", got)
	}
}

func TestTokenMeteringFailureCheckpointRetainsBackoffWithoutAnotherWriter(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	ctx := context.Background()
	state, claimed, err := m.claimTokenMetering(ctx, now)
	if err != nil || !claimed {
		t.Fatal("claim", claimed, err)
	}
	w, found, err := m.nextTokenMeteringWindow(ctx, state.SourceEpoch, state.LeaseToken, now, false)
	if err != nil || !found {
		t.Fatal("forward window", found, err)
	}
	var frontier *tokenMeteringFrontier
	for attempt := 1; attempt <= 2; attempt++ {
		delay := tokenMeteringSourceBackoff(state.SourceEpoch, w.StartAt, attempt)
		if err := m.failTokenMeteringWindowCheckpoint(ctx, state.LeaseToken, w, now, "source_read_failed", &frontier, 16); err != nil {
			t.Fatal(err)
		}
		if frontier == nil || frontier.ScanThrough != w.StartAt || frontier.NextAt != now+delay {
			t.Fatal("checkpoint differs from the persisted retry", frontier)
		}
		failure := &tokenMeteringReadError{err: context.DeadlineExceeded, queryStarted: true, frontier: frontier}
		func() {
			m.tokenLocalWriteMu.Lock()
			defer m.tokenLocalWriteMu.Unlock()
			if delay := m.tokenMeteringRoundDelay(ctx, 1, failure, now); delay > 10*time.Second || delay <= 0 {
				t.Fatal("maintenance blocked the independent latest schedule", delay)
			}
			if delay := m.tokenMeteringRoundDelay(ctx, 1, errors.Join(failure, errors.New("local failure")), now); delay != time.Minute {
				t.Fatal("source checkpoint concealed a local failure", delay)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if delay := m.tokenMeteringRoundDelay(canceled, 1, failure, now); delay != time.Minute {
				t.Fatal("source checkpoint ignored cancellation", delay)
			}
		}()
		if _, found, err := m.nextTokenMeteringWindow(ctx, state.SourceEpoch, state.LeaseToken, now+1, false); err != nil || found {
			t.Fatal("source retry deadline bypassed", found, err)
		}
	}
}

func TestTokenMeteringRolledBackFailureDoesNotPublishCheckpoint(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	ctx := context.Background()
	state, claimed, err := m.claimTokenMetering(ctx, now)
	if err != nil || !claimed {
		t.Fatal("claim", claimed, err)
	}
	w, found, err := m.nextTokenMeteringWindow(ctx, state.SourceEpoch, state.LeaseToken, now, false)
	if err != nil || !found {
		t.Fatal("forward window", found, err)
	}
	if err := m.tokenStore().Exec(`CREATE TEMP TRIGGER reject_failure_checkpoint BEFORE UPDATE ON token_metering_states
		BEGIN SELECT RAISE(ABORT,'failure checkpoint unavailable'); END`).Error; err != nil {
		t.Fatal(err)
	}
	var frontier *tokenMeteringFrontier
	if err := m.failTokenMeteringWindowCheckpoint(ctx, state.LeaseToken, w, now, "source_read_failed", &frontier, 16); err == nil || frontier != nil {
		t.Fatal("rolled-back failure published a usable schedule", frontier, err)
	}
	var persisted TokenMeteringWindow
	if err := m.tokenStore().First(&persisted, "source_epoch=? AND start_at=?", w.SourceEpoch, w.StartAt).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.Attempts != 0 || persisted.NextAt != 0 || persisted.LastReason != "" {
		t.Fatal("failure window escaped rollback", persisted)
	}
}

func TestTokenMeteringEarlierCheckpointCannotBypassLaterWindowBackoff(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	ctx := context.Background()
	state, claimed, err := m.claimTokenMetering(ctx, now)
	if err != nil || !claimed {
		t.Fatal("claim", claimed, err)
	}
	w, found, err := m.nextTokenMeteringWindow(ctx, state.SourceEpoch, state.LeaseToken, now, false)
	if err != nil || !found {
		t.Fatal("forward window", found, err)
	}
	replay := TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: state.StartAt - 300, EndAt: state.StartAt, Replay: true}
	if err := m.tokenStore().Create(&replay).Error; err != nil {
		t.Fatal(err)
	}
	var frontier *tokenMeteringFrontier
	if err := m.failTokenMeteringWindowCheckpoint(ctx, state.LeaseToken, replay, now, "source_read_failed", &frontier, 16); err != nil {
		t.Fatal(err)
	}
	if err := m.commitTokenMeteringPage(ctx, state.LeaseToken, w, nil, nil, nil, true, now); err != nil {
		t.Fatal(err)
	}
	next := TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: w.EndAt, EndAt: w.EndAt + 300, NextAt: now + 45}
	if err := m.tokenStore().Create(&next).Error; err != nil {
		t.Fatal(err)
	}
	failure := &tokenMeteringReadError{err: context.DeadlineExceeded, queryStarted: true, frontier: frontier}
	if delay := m.tokenMeteringRoundDelay(ctx, 1, failure, now); delay != 2*time.Second {
		t.Fatal("expected an early scheduling hint from the committed failure", delay)
	}
	if _, found, err := m.nextTokenMeteringWindow(ctx, state.SourceEpoch, state.LeaseToken, now+2, false); err != nil || found {
		t.Fatal("earlier checkpoint bypassed the new frontier's durable deadline", found, err)
	}
}
