package monitor

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func financePublicationRaceFixture(t *testing.T) (*Monitor, financeReportRequest) {
	t.Helper()
	m := newFinanceReportTestMonitor(t, "race.example")
	from := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	if err := m.storeDB.Create(&ChannelSnap{ID: 41, BaseDomain: "race.example", Models: "initial"}).Error; err != nil {
		t.Fatal(err)
	}
	config, err := m.financeReportConfigurationHash(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := m.financeReportSourceFingerprint(context.Background(), from.Unix(), from.Add(time.Hour).Unix())
	if err != nil {
		t.Fatal(err)
	}
	return m, financeReportRequest{from: from, to: from.Add(time.Hour), configurationHash: config, sourceFingerprint: fingerprint}
}

// Simulates a committed local source publication after the request's version
// probe but before report reads, without sleeping or contacting a real source.
func injectFinancePublicationRace(t *testing.T, m *Monitor, everyBuild bool) *atomic.Int64 {
	t.Helper()
	count := &atomic.Int64{}
	name := "test:finance_publication_race"
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register(name, func(tx *gorm.DB) {
		if tx.Statement.Table != "channel_finance_settings" {
			return
		}
		n := count.Add(1)
		if everyBuild || n == 1 {
			if err := m.storeDB.Exec("UPDATE channel_snaps SET models=? WHERE id=41", fmt.Sprintf("revision-%d", n)).Error; err != nil {
				_ = tx.AddError(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.storeDB.Callback().Query().Remove(name) })
	return count
}

func TestFinancePublicationRaceRetriesUnderAcceptedVersion(t *testing.T) {
	m, request := financePublicationRaceFixture(t)
	count := injectFinancePublicationRace(t, m, false)
	got, status, err := m.financeReportPayload(context.Background(), request, true)
	if err != nil || len(got) == 0 || status != "refresh" || count.Load() != 2 {
		t.Fatalf("retry result status=%s attempts=%d err=%v", status, count.Load(), err)
	}
	if _, ok := m.getFinanceReportCache().Get(request.cacheKey(), time.Now()); ok {
		t.Fatal("accepted report stored under rejected source version")
	}
	request.sourceFingerprint, err = m.financeReportSourceFingerprint(context.Background(), request.from.Unix(), request.to.Unix())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m.getFinanceReportCache().Get(request.cacheKey(), time.Now()); !ok {
		t.Fatal("accepted source version not cached")
	}
}

func TestFinanceContinuousPublicationRaceKeepsOnlySameScopeLastGood(t *testing.T) {
	for _, scenario := range []string{"same", "other_range", "other_configuration", "expired", "cold"} {
		t.Run(scenario, func(t *testing.T) {
			m, request := financePublicationRaceFixture(t)
			old := request
			old.sourceFingerprint = "previous"
			written := time.Now()
			switch scenario {
			case "other_range":
				old.to = old.to.Add(time.Hour)
			case "other_configuration":
				old.configurationHash = "different"
			case "expired":
				written = written.Add(-financeReportCacheTTL - financeReportCacheStaleGrace - time.Minute)
			}
			want := []byte(`{"enabled":true,"generated_at":123}`)
			if scenario != "cold" {
				m.rememberFinanceReport(old, want, written)
			}
			count := injectFinancePublicationRace(t, m, true)
			got, status, err := m.financeReportPayload(context.Background(), request, true)
			if count.Load() != 2 {
				t.Fatalf("unbounded/reduced attempts: %d", count.Load())
			}
			if scenario == "same" {
				if err != nil || status != "stale-retry" || string(got) != string(want) {
					t.Fatalf("lost last good: %s %s %v", status, got, err)
				}
			} else if !errors.Is(err, errFinanceFactsChanged) || len(got) != 0 {
				t.Fatalf("unrelated/expired/unverified report published: %s %s %v", status, got, err)
			}
			if _, ok := m.getFinanceReportCache().Get(request.cacheKey(), time.Now()); ok {
				t.Fatal("rejected report cached")
			}
		})
	}
}

func TestFinanceRetryDoesNotHideConfigurationChange(t *testing.T) {
	m, request := financePublicationRaceFixture(t)
	request.configurationHash = "old-configuration"
	m.rememberFinanceReport(request, []byte(`{"enabled":true}`), time.Now())
	got, _, err := m.financeReportPayload(context.Background(), request, true)
	if err == nil || errors.Is(err, errFinanceFactsChanged) || len(got) != 0 {
		t.Fatalf("configuration error hidden: %s %v", got, err)
	}
}

func TestFinanceMonthlyPublicationRaceDoesNotCacheMixedComponent(t *testing.T) {
	m, request := financePublicationRaceFixture(t)
	var changed atomic.Bool
	name := "test:finance_monthly_publication_race"
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register(name, func(tx *gorm.DB) {
		if tx.Statement.Table == "channel_snaps" && changed.CompareAndSwap(false, true) {
			if err := m.storeDB.Exec("UPDATE channel_snaps SET base_domain='new.example' WHERE id=41").Error; err != nil {
				_ = tx.AddError(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.storeDB.Callback().Query().Remove(name) })
	_, _, err := m.buildFinancePeriodComponent(context.Background(), stabilityScope{FromTs: request.from.Unix(), ToTs: request.to.Unix()}, time.Now().Unix(), request.configurationHash, nil, channelFinanceSnapshot{}, financeInternalTestCostEvidence{SourceComplete: true, Complete: true}, financeConfiguredInternalEvidence{Complete: true}, nil)
	if !changed.Load() || !errors.Is(err, errFinanceFactsChanged) {
		t.Fatalf("monthly race not detected: changed=%t err=%v", changed.Load(), err)
	}
	if entries, _ := m.getFinancePeriodCache().size(); entries != 0 {
		t.Fatalf("mixed monthly cache published: %d", entries)
	}
}
