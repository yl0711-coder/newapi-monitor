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
	err := retryStabilityLocalWrite(context.Background(), func(context.Context) error { attempts++; return localBusyTestError{} })
	if !stabilityLocalWriteBusy(err) || attempts != stabilityLocalWriteAttempts {
		t.Fatalf("attempts=%d err=%v", attempts, err)
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
	go func() { time.Sleep(120 * time.Millisecond); _, err := locker.Exec("COMMIT"); released <- err }()
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
