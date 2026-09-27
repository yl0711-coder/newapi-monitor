package monitor

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func giftEvidenceCacheFixture(t *testing.T) (*Monitor, *atomic.Int64) {
	t.Helper()
	m := newFinanceReportTestMonitor(t, "gift-cache.example")
	t.Cleanup(func() { m.Close() })
	db := m.usageFactsStore()
	for hour := int64(0); hour < 3*3600; hour += 3600 {
		publishFinanceGiftTestHour(t, m, hour, 7, "epoch", 500_000, 100+hour)
		credits := financeCreditHourFetch{}
		if hour == 0 {
			grant := FinanceCreditEvent{SourceLogID: 1, EventAt: 10, TargetUserID: 7, UserCreatedAt: 1, Action: "quota_add", Unit: "USD_quota", ValueMicro: 100_000_000, NetChangeMicro: 100_000_000, Cohort: "trial_candidate_within_24h", EligibleTrial: true}
			grant.EvidenceHash = financeCreditEventHash(grant)
			credits = financeCreditHourFetch{Events: []FinanceCreditEvent{grant}, SourceRows: 1}
		}
		if _, err := replaceFinanceCreditHour(context.Background(), db, hour, 20_000, "epoch", credits); err != nil {
			t.Fatal(err)
		}
	}
	reads := &atomic.Int64{}
	if err := db.Callback().Row().Before("gorm:row").Register("test:gift-evidence-reads", func(tx *gorm.DB) {
		if tx.Statement.Table == "finance_gift_boundary_events" {
			reads.Add(1)
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Row().Remove("test:gift-evidence-reads") })
	return m, reads
}

func TestFinanceGiftEvidenceCacheReusesOnlyUnchangedProofs(t *testing.T) {
	m, reads := giftEvidenceCacheFixture(t)
	load := func(to int64) financeGiftAllocationResult {
		t.Helper()
		r, err := m.loadFinanceGiftAllocation(context.Background(), 0, 0, to)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	cold := load(7200)
	if !cold.Coverage.Complete || reads.Load() != 1 {
		t.Fatalf("cold read: %+v reads=%d", cold.Coverage, reads.Load())
	}
	warm := load(7200)
	if !reflect.DeepEqual(cold, warm) || reads.Load() != 1 {
		t.Fatal("unchanged inputs rescanned or changed money")
	}
	tail := load(10800)
	if !tail.Coverage.Complete || reads.Load() != 2 || tail.Allocation.PeriodGiftConsumptionMicroUSD != 3_000_000 {
		t.Fatal("new hour did not incrementally read")
	}
	// Re-publish a historical hour, retaining its timestamp and epoch. The
	// content hash, not timestamp precision, must invalidate the old evidence.
	publishFinanceGiftTestHour(t, m, 3600, 7, "epoch", 1_000_000, 3700)
	changed := load(10800)
	if changed.Allocation.PeriodGiftConsumptionMicroUSD != 4_000_000 || reads.Load() != 3 {
		t.Fatalf("historical correction not applied: %+v reads=%d", changed.Allocation, reads.Load())
	}
	if err := m.usageFactsStore().Model(&FinanceGiftBoundaryState{}).Where("hour_ts=?", 3600).Update("status", "failed").Error; err != nil {
		t.Fatal(err)
	}
	if r := load(10800); r.Coverage.Complete {
		t.Fatal("cached detail concealed failed proof")
	}
}

func TestFinanceGiftEvidenceCacheSeparatesScopeAndUnknownEvidence(t *testing.T) {
	m, reads := giftEvidenceCacheFixture(t)
	load := func(groups map[string]bool, users map[int64]bool) financeGiftAllocationResult {
		t.Helper()
		r, err := m.loadFinanceGiftAllocationForScope(context.Background(), 0, 0, 10800, groups, users)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	base := load(nil, nil)
	unknown := load(map[string]bool{"test": false}, nil)
	if !base.Coverage.Complete || unknown.Coverage.Complete || unknown.Coverage.ScopeUnknownEvents != 3 {
		t.Fatal("policy change reused business classification")
	}
	before := reads.Load()
	if !reflect.DeepEqual(unknown, load(map[string]bool{"test": false}, nil)) || reads.Load() != before {
		t.Fatal("verified unknown scope was rescanned or relabeled")
	}
	internal := load(map[string]bool{"test": false}, map[int64]bool{7: true})
	if !internal.Coverage.Complete || internal.Coverage.ScopeUnknownEvents != 0 || internal.Allocation.PeriodGiftConsumptionMicroUSD != 0 {
		t.Fatal("internal-account policy reused stale evidence")
	}
	// Scope-only historical repair: amounts unchanged, event/proof hash changes.
	for hour := int64(0); hour < 10800; hour += 3600 {
		e := FinanceGiftBoundaryEvent{SourceLogID: 100 + hour, HourTs: hour, UserID: 7, EventAt: hour + 100, Kind: "usage", Quota: 500_000, Group: "business", GroupKnown: true}
		e.EvidenceHash = financeGiftBoundaryEventHash(e)
		// The epoch-zero fixture needs an explicit delete before Save: GORM
		// considers zero-valued composite primary keys a new record.
		if hour == 0 {
			if err := m.usageFactsStore().Where("hour_ts=? AND user_id=?", hour, 7).Delete(&FinanceGiftBoundaryState{}).Error; err != nil {
				t.Fatal(err)
			}
		}
		if _, err := replaceFinanceGiftBoundaryUserHour(context.Background(), m.usageFactsStore(), hour, 7, hour+10_000, "epoch", []FinanceGiftBoundaryEvent{e}); err != nil {
			t.Fatal(err)
		}
	}
	repaired := load(map[string]bool{"test": false}, nil)
	if !repaired.Coverage.Complete || repaired.Allocation != base.Allocation {
		t.Fatal("scope repair not visible or changed money")
	}
}

func TestFinanceGiftEvidenceCacheBoundsAndConcurrentReaders(t *testing.T) {
	m, _ := giftEvidenceCacheFixture(t)
	cache := m.getFinanceGiftEvidenceCache()
	if cache.maxEntries != financeGiftEvidenceCacheEntries || cache.maxBytes != financeGiftEvidenceCacheBytes {
		t.Fatal("missing cache bounds")
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := m.loadFinanceGiftAllocation(context.Background(), 0, 0, 10800)
			if err != nil || !r.Coverage.Complete || r.Allocation.PeriodGiftConsumptionMicroUSD != 3_000_000 {
				t.Errorf("concurrent result: %+v %v", r.Coverage, err)
			}
		}()
	}
	wg.Wait()
	entries, bytes := cache.size()
	if entries > financeGiftEvidenceCacheEntries || bytes > financeGiftEvidenceCacheBytes {
		t.Fatal("cache exceeded bound")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.loadFinanceGiftAllocation(ctx, 0, 0, 10800); err == nil {
		t.Fatal("canceled read returned cached success")
	}
	// Expired entries are misses, never stale financial evidence.
	var state FinanceGiftBoundaryState
	if err := m.usageFactsStore().First(&state, "hour_ts=?", 0).Error; err != nil {
		t.Fatal(err)
	}
	version, _ := m.financeGiftEvidenceGuard.version(context.Background(), m.financeFactsReadStore())
	key := financeGiftEvidenceKey(state, version+"|"+financeGiftEvidenceScopeKey(nil, nil))
	for _, payload := range []string{`broken`, `null`, `{}`} {
		cache.Put(key, []byte(payload), time.Hour, time.Now())
		r, err := m.loadFinanceGiftAllocation(context.Background(), 0, 0, 10800)
		if err != nil || !r.Coverage.Complete || r.Allocation.PeriodGiftConsumptionMicroUSD != 3_000_000 {
			t.Fatalf("bad cache entry was not rebuilt: %v", err)
		}
	}
	payload, ok := cache.Get(key, time.Now())
	if !ok {
		t.Fatal("fixture must rebuild a fresh entry")
	}
	cache.Put(key, payload, financeGiftEvidenceCacheTTL, time.Now().Add(-2*financeGiftEvidenceCacheTTL))
	if _, ok := cache.Get(key, time.Now()); ok {
		t.Fatal("expired evidence is reusable")
	}
	// Tiny budgets may force every hour to be read again, never alter money.
	m.financeGiftEvidenceCache = newBoundedByteCache(1, 128)
	for i := 0; i < 2; i++ {
		r, err := m.loadFinanceGiftAllocation(context.Background(), 0, 0, 10800)
		if err != nil || r.Allocation.PeriodGiftConsumptionMicroUSD != 3_000_000 {
			t.Fatal("eviction changed allocation")
		}
	}
}

func TestFinanceGiftCacheGuardReadOnlyAndClosedFallback(t *testing.T) {
	m, reads := giftEvidenceCacheFixture(t)
	ctx := context.Background()
	if _, err := m.loadFinanceGiftAllocation(ctx, 0, 0, 10800); err != nil {
		t.Fatal(err)
	}
	g := &m.financeGiftEvidenceGuard
	if _, err := g.conn.ExecContext(ctx, "DELETE FROM finance_gift_boundary_events"); err == nil {
		t.Fatal("cache guard must be read-only")
	}
	g.close()
	before := reads.Load()
	for i := 0; i < 2; i++ {
		r, err := m.loadFinanceGiftAllocation(ctx, 0, 0, 10800)
		if err != nil || !r.Coverage.Complete {
			t.Fatalf("guard loss must fall back to verification, not fail report: %v", err)
		}
	}
	if reads.Load() != before+2 {
		t.Fatal("closed guard reused cached evidence")
	}
}

func TestFinanceGiftEvidenceCacheRejectsConcurrentUnpublishedMutation(t *testing.T) {
	m, _ := giftEvidenceCacheFixture(t)
	ctx := context.Background()
	if _, err := m.loadFinanceGiftAllocation(ctx, 0, 0, 10800); err != nil {
		t.Fatal(err)
	}
	var rows []FinanceGiftBoundaryState
	if err := m.usageFactsStore().Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	states := make(map[financeGiftUserHourKey]FinanceGiftBoundaryState)
	for _, row := range rows {
		states[financeGiftUserHourKey{HourTs: row.HourTs, UserID: row.UserID}] = row
	}
	changed := false
	err := m.walkFinanceGiftHourEvidence(ctx, m.financeFactsReadStore(), states, nil, nil, func(_ financeGiftUserHourKey, _ financeGiftHourEvidence) {
		if !changed {
			changed = true
			if err := m.usageFactsStore().Exec("UPDATE finance_gift_boundary_events SET quota=1 WHERE source_log_id=100").Error; err != nil {
				t.Fatal(err)
			}
		}
	})
	if !errors.Is(err, errFinanceFactsChanged) {
		t.Fatalf("concurrent mutation must retry under a new version, got %v", err)
	}
}
