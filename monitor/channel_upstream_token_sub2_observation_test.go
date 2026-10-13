package monitor

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

func sub2ObservationLegacy(t *testing.T, account ChannelUpstreamAccount, items []sub2PricingUsageItem, day int64) map[int64][]ChannelUpstreamPricingHourEvidence {
	t.Helper()
	rows, err := buildSub2PricingEvidence(account, items, day+86400)
	if err != nil {
		t.Fatal(err)
	}
	legacy := make(map[int64][]ChannelUpstreamPricingHourEvidence)
	for hour := day; hour < day+86400; hour += 3600 {
		legacy[hour] = nil
	}
	for _, row := range rows {
		legacy[row.HourTs] = append(legacy[row.HourTs], row)
	}
	return legacy
}

func TestSub2TokenObservationIndependentScansNotRetries(t *testing.T) {
	f := newSub2ShadowHTTPFixture(t)
	f.configure(t, "575", 1)
	f.m.cfg.ChannelCostClosureEnabled = true
	ctx := context.Background()
	key, keyID := []byte(f.m.cfg.ChannelCostHMACKey), f.m.cfg.ChannelCostHMACKeyID
	load := func() ChannelUpstreamTokenScanCheckpoint {
		t.Helper()
		row, err := f.m.loadSub2TokenScanCheckpoint(ctx, f.account, f.day, key, keyID)
		if err != nil {
			t.Fatal(err)
		}
		return row
	}
	rows, done, err := f.fetch(10)
	if err != nil || !done {
		t.Fatal(err)
	}
	first := load()
	if first.StableScans != 1 || first.SuccessRevision != first.Revision || first.SuccessHash == "" {
		t.Fatal("missing first observation")
	}
	// Replayed completion after process restart must not count as scan two.
	f.m = &Monitor{storeDB: f.m.storeDB, cfg: f.m.cfg}
	retry, err := f.m.recordSub2TokenObservation(ctx, f.account, f.day, first, rows, f.day+86401, key, keyID)
	if err != nil || retry != first {
		t.Fatal("completion retry changed observation", err)
	}
	for scan := 0; scan < 2; scan++ {
		if err := f.m.deletePricingPageCheckpoint(ctx, f.account.Domain, newAPIUpstreamAccountEpoch(f.account), f.day); err != nil {
			t.Fatal(err)
		}
		_, done, err = f.fetch(10)
		if err != nil || !done {
			t.Fatal(err)
		}
		current := load()
		if current.StableScans != 2 || current.SuccessRevision <= first.SuccessRevision || current.SuccessHash != first.SuccessHash {
			t.Fatal("independent stable scan not recognized")
		}
	}
	// Same account amount, different token attribution: restart verification.
	f.changed.Store(true)
	if err := f.m.deletePricingPageCheckpoint(ctx, f.account.Domain, newAPIUpstreamAccountEpoch(f.account), f.day); err != nil {
		t.Fatal(err)
	}
	changedRows, done, err := f.fetch(10)
	if err != nil || !done || !reflect.DeepEqual(rows, changedRows) {
		t.Fatal("legacy amount changed", err)
	}
	changed := load()
	if changed.StableScans != 1 || changed.SuccessHash == first.SuccessHash {
		t.Fatal("identity-only change incorrectly remained stable")
	}
	for _, model := range []any{&ChannelUpstreamCostHourEvidence{}, &ChannelUpstreamCostHourState{}, &ChannelCostSourceBinding{}, &ChannelEconomicsDirtyHour{}} {
		var count int64
		if err := f.m.storeDB.Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("observation published %T: %v", model, err)
		}
	}
}

func TestSub2TokenObservationFinalProbeAndWriteFailureDoNotVerify(t *testing.T) {
	for _, mode := range []string{"changed-probe", "write-failed"} {
		t.Run(mode, func(t *testing.T) {
			f := newSub2ShadowHTTPFixture(t)
			f.configure(t, "575", 1)
			f.m.cfg.ChannelCostClosureEnabled = true
			if mode == "changed-probe" {
				f.changeOnFinalProbe.Store(true)
			} else {
				if err := f.m.storeDB.Exec(`CREATE TRIGGER fail_observation BEFORE UPDATE OF success_revision ON channel_upstream_token_scan_checkpoints BEGIN SELECT RAISE(ABORT,'synthetic observation failure'); END`).Error; err != nil {
					t.Fatal(err)
				}
			}
			rows, done, err := f.fetch(10)
			if err != nil || !done {
				t.Fatal("shadow failure interrupted legacy", err)
			}
			assertSub2ShadowLegacyTotal(t, rows)
			cursor, err := f.m.loadSub2TokenScanCheckpoint(context.Background(), f.account, f.day, []byte(f.m.cfg.ChannelCostHMACKey), f.m.cfg.ChannelCostHMACKeyID)
			if err != nil || cursor.Revision != 2 || cursor.SuccessRevision != 0 || cursor.StableScans != 0 {
				t.Fatal("complete pages mistaken for successful scan", err)
			}
			if f.requests.Load() != 3 {
				t.Fatal("shadow added upstream requests")
			}
		})
	}
}

func TestSub2TokenObservationRejectsIncompleteStaleAndUnclosed(t *testing.T) {
	m, account, items, day, key := sub2TokenStoreFixture(t)
	ctx := context.Background()
	legacy := sub2ObservationLegacy(t, account, items, day)
	first, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[:2], 1), 0, day+86400, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.recordSub2TokenObservation(ctx, account, day, first, legacy, day+86400, key, "v1"); err == nil {
		t.Fatal("partial scan accepted")
	}
	complete, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[2:], 2), first.Revision, day+86400, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.recordSub2TokenObservation(ctx, account, day, complete, legacy, day+86399, key, "v1"); err == nil {
		t.Fatal("open day accepted")
	}
	badLegacy := sub2ObservationLegacy(t, account, items, day)
	delete(badLegacy, day+23*3600)
	if _, err := m.recordSub2TokenObservation(ctx, account, day, complete, badLegacy, day+86400, key, "v1"); err == nil {
		t.Fatal("missing account hour accepted")
	}
	m.cfg.LocalSnapshotOnly = true
	if _, err := m.recordSub2TokenObservation(ctx, account, day, complete, legacy, day+86400, key, "v1"); !errors.Is(err, errUpstreamTokenReadOnly) {
		t.Fatal("readonly observation wrote", err)
	}
	m.cfg.LocalSnapshotOnly = false
	observed, err := m.recordSub2TokenObservation(ctx, account, day, complete, legacy, day+86401, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	stored, err := m.loadSub2TokenScanCheckpoint(ctx, account, day, key, "v1")
	if err != nil || stored != observed || stored.UpdatedAt != complete.UpdatedAt {
		t.Fatal("observation changed page progress timestamp", err)
	}
	if _, err := m.resetSub2TokenScanCheckpoint(ctx, account, day, complete.Revision, day+86401, key, "v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.recordSub2TokenObservation(ctx, account, day, complete, legacy, day+86402, key, "v1"); !errors.Is(err, errSub2TokenCursorConflict) {
		t.Fatal("delayed finish after reset accepted", err)
	}
}

func TestSub2TokenObservationConcurrentFinishAndAccountChange(t *testing.T) {
	m, account, items, day, key := sub2TokenStoreFixture(t)
	ctx := context.Background()
	legacy := sub2ObservationLegacy(t, account, items, day)
	sqlDB, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	first, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[:2], 1), 0, day+86400, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	complete, err := m.saveSub2TokenScanPage(ctx, account, day, sub2TokenPage(items[2:], 2), first.Revision, day+86400, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			other := &Monitor{storeDB: m.storeDB}
			row, err := other.recordSub2TokenObservation(ctx, account, day, complete, legacy, day+86400, key, "v1")
			if err == nil && row.StableScans != 1 {
				err = errors.New("duplicate completion counted as second scan")
			}
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", account.Domain).Update("account", "different-upstream-user").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.recordSub2TokenObservation(ctx, account, day, complete, legacy, day+86401, key, "v1"); !errors.Is(err, errSub2TokenCursorConflict) {
		t.Fatal("old account completion accepted", err)
	}
}
