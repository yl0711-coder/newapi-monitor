package monitor

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

type localBusyTestError struct{}

func (localBusyTestError) Error() string { return "busy snapshot" }
func (localBusyTestError) Code() int     { return 517 }

func TestStabilityLocalRetryIsBoundedAndDoesNotRetryPermanentErrors(t *testing.T) {
	attempts := 0
	var delays []time.Duration
	err := retryStabilityLocalWriteWithWait(context.Background(), func(context.Context) error { attempts++; return localBusyTestError{} }, func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	})
	if !stabilityLocalWriteBusy(err) || attempts != stabilityLocalWriteAttempts {
		t.Fatalf("attempts=%d err=%v", attempts, err)
	}
	if len(delays) != stabilityLocalWriteAttempts-1 {
		t.Fatalf("unexpected waits: %v", delays)
	}
	wantDelay := stabilityLocalWriteDelay
	for _, delay := range delays {
		if delay != wantDelay {
			t.Fatalf("delay=%v want=%v", delay, wantDelay)
		}
		wantDelay = min(wantDelay*2, stabilityLocalWriteMaxDelay)
	}
	attempts = 0
	permanent := errors.New("control totals differ")
	err = retryStabilityLocalWrite(context.Background(), func(context.Context) error { attempts++; return permanent })
	if !errors.Is(err, permanent) || attempts != 1 {
		t.Fatal("permanent error retried")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = retryStabilityLocalWrite(ctx, func(context.Context) error { t.Fatal("canceled write executed"); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestStabilityLocalRetryRecoversRealSQLiteLockUpgrade(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	locker, err := sql.Open("sqlite", m.cfg.StorePath+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Close()
	locker.SetMaxOpenConns(1)
	if _, err := locker.Exec("BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = locker.Exec("ROLLBACK") }()
	released := make(chan error, 1)
	go func() { time.Sleep(600 * time.Millisecond); _, err := locker.Exec("COMMIT"); released <- err }()
	attempts := 0
	err = retryStabilityLocalWrite(context.Background(), func(ctx context.Context) error {
		attempts++
		return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var count int64
			if err := tx.Model(&TrackedUser{}).Count(&count).Error; err != nil {
				return err
			}
			return tx.Create(&TrackedUser{UserID: 99121, Username: "local-writer"}).Error
		})
	})
	if err != nil || attempts < 2 {
		t.Fatalf("real lock upgrade not recovered: attempts=%d err=%v", attempts, err)
	}
	if err := <-released; err != nil {
		t.Fatal(err)
	}
}

func TestStabilityLocalRetryHonorsCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	attempts := 0
	err := retryStabilityLocalWrite(ctx, func(context.Context) error {
		attempts++
		return localBusyTestError{}
	})
	if !errors.Is(err, context.DeadlineExceeded) || attempts > 1 {
		t.Fatalf("retry ignored caller deadline: attempts=%d err=%v", attempts, err)
	}
}

func TestStabilityBackfillLocalContentionDoesNotRefetchSource(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.StabilityBackfillDelayMS = -1
	m.cfg.StabilityBackfillSourceDutyPercent = 100
	hour := time.Date(2026, 8, 5, 10, 0, 0, 0, cstLocation).Unix()
	job := StabilityBackfillJob{ID: "local-busy-no-refetch", FromTs: hour, ToTs: hour + 3600,
		Status: "queued", TotalHours: 1, CurrentBatchHours: 1}
	if err := m.storeDB.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	writes := 0
	const callback = "test:backfill_local_contention"
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register(callback, func(db *gorm.DB) {
		if db.Statement.Table == "stability_hour_samples" {
			writes++
			if writes <= 4 {
				_ = db.AddError(localBusyTestError{})
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.storeDB.Callback().Create().Remove(callback) }()
	queries := 0
	fetch := func(context.Context, int64, int64) (stabilityRangeResult, error) {
		queries++
		return stabilityRangeResult{Hours: map[int64]stabilityHourTraffic{
			hour: {Users: []StabilityHourSample{{HourTs: hour, ChannelID: 1, ModelName: "m", Grp: "g", Success: 3, Tokens: 99, Quota: 100}}},
		}, SourceQueries: 1}, nil
	}
	m.stabilityBackfillRunning.Store(true)
	m.runStabilityBackfillWithFetcher(context.Background(), job.ID, fetch)
	if err := m.storeDB.First(&job, "id = ?", job.ID).Error; err != nil {
		t.Fatal(err)
	}
	var state StabilityHourIngestState
	if err := m.storeDB.First(&state, "hour_ts = ?", hour).Error; err != nil {
		t.Fatal(err)
	}
	if queries != 1 || writes != 5 || job.Status != "complete" || state.Status != "complete" || state.Requests != 3 || state.Quota != 100 {
		t.Fatalf("contention replayed source or failed atomic publication: queries=%d writes=%d job=%+v state=%+v", queries, writes, job, state)
	}
}

func TestStabilityHourRetriesRollbackWholeReplacement(t *testing.T) {
	m := newTestMonitor(t)
	hour := int64(1800000000)
	attempts := 0
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("test:busy_hour", func(db *gorm.DB) {
		if db.Statement.Table == "stability_hour_samples" {
			attempts++
			if attempts < 3 {
				_ = db.AddError(localBusyTestError{})
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.storeDB.Callback().Create().Remove("test:busy_hour") }()
	rows := []StabilityHourSample{{HourTs: hour, ChannelID: 1, ModelName: "m", Grp: "g", Success: 3, Tokens: 99, Quota: 100}}
	if err := m.replaceStabilityHourTraffic(hour, rows, nil, StabilityHourIngestState{}); err != nil {
		t.Fatal(err)
	}
	var state StabilityHourIngestState
	if err := m.storeDB.First(&state, "hour_ts = ?", hour).Error; err != nil {
		t.Fatal(err)
	}
	var stored []StabilityHourSample
	if err := m.storeDB.Where("hour_ts = ?", hour).Find(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if attempts != 3 || state.Status != "complete" || state.Requests != 3 || len(stored) != 1 || stored[0].Tokens != 99 {
		t.Fatalf("non-atomic retry: state=%+v rows=%+v attempts=%d", state, stored, attempts)
	}
}
