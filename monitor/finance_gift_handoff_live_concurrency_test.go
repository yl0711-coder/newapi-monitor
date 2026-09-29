//go:build unix

package monitor

import (
	"context"
	"testing"
	"time"
)

func TestFinanceGiftLiveWriterContentionAndResume(t *testing.T) {
	m, _, a := giftLiveFixture(t)
	before := giftLiveFacts(t, m)
	other, closeOther, err := giftLocalDatabase(m.cfg.UsageFactsStorePath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeOther()
	tx := other.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	if err := tx.Exec("UPDATE finance_user_hour_states SET updated_at=updated_at").Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	started := time.Now()
	waits := 0
	_, err = m.runFinanceGiftAuthorizedLive(ctx, a.TaskID, func(context.Context, time.Duration) error { waits++; return nil })
	if err == nil || time.Since(started) > financeGiftHandoffWriteBudget+time.Second || waits != 0 {
		t.Fatal("contended provisioning must stop before execution", err, waits, time.Since(started))
	}
	if err := tx.Rollback().Error; err != nil {
		t.Fatal(err)
	}
	if before != giftLiveFacts(t, m) {
		t.Fatal("contended attempt changed facts")
	}
	result, err := m.runFinanceGiftAuthorizedLive(context.Background(), a.TaskID, handoffNoWait)
	if err != nil || result.Status != "complete" {
		t.Fatal("explicit resume failed", result, err)
	}
}

func TestFinanceGiftLiveCooldownPermitsNormalPublication(t *testing.T) {
	m, _, a := giftLiveFixture(t)
	other, closeOther, err := giftLocalDatabase(m.cfg.UsageFactsStorePath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeOther()
	var latest int64
	if err := other.Raw("SELECT MAX(hour_ts) FROM finance_user_hour_states").Scan(&latest).Error; err != nil {
		t.Fatal(err)
	}
	hour := latest + 7200
	before := ""
	waits := 0
	result, err := m.runFinanceGiftAuthorizedLive(context.Background(), a.TaskID, func(ctx context.Context, _ time.Duration) error {
		waits++
		if waits != 1 {
			return ctx.Err()
		}
		bounded, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if err := handoffPublishTestHour(bounded, other, hour, 777); err != nil {
			return err
		}
		before = giftMixedMonetarySnapshot(t, m.usageFactsDB)
		return nil
	})
	if err != nil || result.Status != "complete" || before == "" || before != giftMixedMonetarySnapshot(t, m.usageFactsDB) {
		t.Fatal("normal publication blocked or overwritten", result, err)
	}
	var quota int64
	if err := other.Raw("SELECT consume_quota FROM finance_user_hour_facts WHERE hour_ts=? AND user_id=7", hour).Scan(&quota).Error; err != nil || quota != 777 {
		t.Fatal("new facts lost", quota, err)
	}
}
