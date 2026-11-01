package monitor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func monthlySeriesPublicationFixture(t *testing.T, m *Monitor, group CustomerGroup, now time.Time) ([]MonthlyBillEvidence, *monthlyPreparedDisplayPublication, *MonthlyBillExpectedAuthorization) {
	t.Helper()
	last, _, _ := monthlyWindow("", now)
	evidence := []MonthlyBillEvidence{
		monthlyFrozenEvidence(group.ID, last.AddDate(0, -2, 0).Format("2006-01"), 1160872085, 2500000000, 1513097892, now),
		monthlyFrozenEvidence(group.ID, last.AddDate(0, -1, 0).Format("2006-01"), 2147774193, 0, 2500, now),
		monthlyFrozenEvidence(group.ID, last.Format("2006-01"), 2147771693, 0, 0, now),
	}
	var sources []monthlyDisplayPublicationSource
	for _, e := range evidence {
		sources = append(sources, monthlyDisplayPublicationSource{Evidence: monthlyTestRaw(t, e), Account: group.Name, Names: map[int64]string{43: "synthetic"}})
	}
	prepared, err := monthlyPrepareDisplayPublication(context.Background(), monthlyDisplaySeriesScope{GroupID: group.ID, SourceIdentity: monthlySeriesTestOrigin, From: evidence[0].Month, Through: evidence[2].Month}, sources, now)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := loadMonthlyBillAuthorization(context.Background(), m.storeDB, group.ID)
	if err != nil {
		t.Fatal(err)
	}
	expected := &MonthlyBillExpectedAuthorization{GroupID: group.ID, AuthVersion: group.PortalAuthVer, MemberFingerprint: scope.Fingerprint, MemberManifest: scope.Manifest}
	return evidence, prepared, expected
}

func monthlySeriesRecordCount(t *testing.T, db *gorm.DB, want int64) {
	t.Helper()
	var count int64
	if err := db.Model(&PortalMonthlyBill{}).Count(&count).Error; err != nil || count != want {
		t.Fatal("unexpected number of saved bills", count, want, err)
	}
}

func TestMonthlyDisplayPublicationEveryOrderReadDownloadAndRestart(t *testing.T) {
	for _, order := range [][]int{{0, 1, 2}, {2, 1, 0}, {0, 2, 1}, {1, 2, 0}, {1, 0, 2}, {2, 0, 1}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			m, router, group, cookie := monthlyFrozenFixture(t)
			t.Cleanup(m.Close)
			now := time.Now()
			evidence, prepared, auth := monthlySeriesPublicationFixture(t, m, group, now)
			for _, index := range order {
				e := evidence[index]
				if _, err := importMonthlyBillPrepared(context.Background(), m.storeDB, monthlyTestRaw(t, e), true, now, auth, prepared); err != nil {
					t.Fatal("complete series could not publish in customer-selected order", index, err)
				}
			}
			monthlySeriesRecordCount(t, m.storeDB, 3)
			for i, e := range evidence {
				record := monthlyFrozenRecord(t, m.storeDB, group.ID, e.Month)
				p, err := readPublishedMonthlyBill(record)
				want := prepared.plan.Months[i]
				if err != nil || p.DisplayBalances.Organization.Opening != want.Organization.Opening || p.DisplayBalances.Organization.Closing != want.Organization.Closing || p.DisplayBalances.Members[0].Opening != want.Members[0].Wallet.Opening || p.DisplayBalances.Members[0].Closing != want.Members[0].Wallet.Closing {
					t.Fatal("SQLite publication depends on selection order", e.Month, err)
				}
				if p.Consumption.Amount != monthlyUniversalMoney(e.Users[0].ConsumeQuota) || *p.MonthEndBalance != monthlyUniversalMoney(e.Users[0].ClosingQuota) {
					t.Fatal("source consumption or original ending was edited")
				}
				view := portalDo(router, "GET", "/api/monthly-statement?month="+e.Month, "", cookie)
				if view.Code != 200 || strings.Contains(view.Body.String(), prepared.plan.AnchorSHA256) || strings.Contains(view.Body.String(), prepared.scope.SourceIdentity) {
					t.Fatal("published view unavailable or private origin leaked", view.Code)
				}
				book, err := monthlyStatementXLSX(p)
				if err != nil {
					t.Fatal(err)
				}
				download := portalDo(router, "GET", "/api/monthly-statement/download?month="+e.Month+"&publication="+p.Publication, "", cookie)
				if download.Code != 200 || !bytes.Equal(book, download.Body.Bytes()) {
					t.Fatal("viewed and downloaded publication differ", download.Code)
				}
				if _, err := importMonthlyBillPrepared(context.Background(), m.storeDB, monthlyTestRaw(t, e), true, now.Add(time.Second), auth, prepared); err != nil || monthlyFrozenRecord(t, m.storeDB, group.ID, e.Month) != record {
					t.Fatal("replay changed an issued bill", err)
				}
			}
			// Reopen a copy, reconstruct the source preparation (not an in-memory
			// anchor cache), and verify that the persisted private marker agrees.
			path := filepath.Join(t.TempDir(), "restart.db")
			if err := m.storeDB.Exec("VACUUM INTO ?", path).Error; err != nil {
				t.Fatal(err)
			}
			restarted := &Monitor{cfg: Settings{SessionSecret: "synthetic-series"}}
			if err := restarted.openStore(path); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(restarted.Close)
			_, rebuilt, rebound := monthlySeriesPublicationFixture(t, restarted, group, now)
			if rebuilt.plan.AnchorSHA256 != prepared.plan.AnchorSHA256 {
				t.Fatal("reconstruction moved the fixed origin")
			}
			for _, e := range evidence {
				before := monthlyFrozenRecord(t, restarted.storeDB, group.ID, e.Month)
				if _, err := importMonthlyBillPrepared(context.Background(), restarted.storeDB, monthlyTestRaw(t, e), true, now, rebound, rebuilt); err != nil || before != monthlyFrozenRecord(t, restarted.storeDB, group.ID, e.Month) {
					t.Fatal("restart/retry changed customer money", err)
				}
			}
		})
	}
}

func TestMonthlyDisplayPublicationDryRunRollbackAndRecovery(t *testing.T) {
	m, _, group, _ := monthlyFrozenFixture(t)
	t.Cleanup(m.Close)
	now := time.Now()
	evidence, prepared, auth := monthlySeriesPublicationFixture(t, m, group, now)
	raw := monthlyTestRaw(t, evidence[2])
	if _, err := importMonthlyBillPrepared(context.Background(), m.storeDB, raw, false, now, auth, prepared); err != nil {
		t.Fatal(err)
	}
	monthlySeriesRecordCount(t, m.storeDB, 0)
	injected := errors.New("synthetic series storage failure")
	const callback = "monthly_series_storage_failure"
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "portal_monthly_bills" {
			_ = tx.AddError(injected)
		}
	}); err != nil {
		t.Fatal(err)
	}
	_, err := importMonthlyBillPrepared(context.Background(), m.storeDB, raw, true, now, auth, prepared)
	if removeErr := m.storeDB.Callback().Create().Remove(callback); removeErr != nil {
		t.Fatal(removeErr)
	}
	if !errors.Is(err, injected) {
		t.Fatal("storage fault not propagated", err)
	}
	monthlySeriesRecordCount(t, m.storeDB, 0)
	if _, err := importMonthlyBillPrepared(context.Background(), m.storeDB, raw, true, now, auth, prepared); err != nil {
		t.Fatal("retry failed after transaction rollback", err)
	}
	monthlySeriesRecordCount(t, m.storeDB, 1)
	before := monthlyFrozenRecord(t, m.storeDB, group.ID, evidence[2].Month)
	// Both a target evidence change and a private source-only reference change
	// are rejected. Matching money is not sufficient proof of the same source.
	changed := evidence[2]
	changed.SourceReference += "; different source"
	if _, err := importMonthlyBillPrepared(context.Background(), m.storeDB, monthlyTestRaw(t, changed), true, now, auth, prepared); err == nil || before != monthlyFrozenRecord(t, m.storeDB, group.ID, evidence[2].Month) {
		t.Fatal("changed evidence replaced a saved bill", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := importMonthlyBillPrepared(ctx, m.storeDB, monthlyTestRaw(t, evidence[0]), true, now, auth, prepared); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled operation was not stopped", err)
	}
	monthlySeriesRecordCount(t, m.storeDB, 1)
}

func TestMonthlyDisplayPublicationRechecksAuthorizationAndFrozenSnapshot(t *testing.T) {
	for _, kind := range []string{"auth", "source", "origin", "start", "marker", "payload", "private_evidence", "prepared_mutation", "anchor", "plan"} {
		t.Run(kind, func(t *testing.T) {
			m, _, group, _ := monthlyFrozenFixture(t)
			t.Cleanup(m.Close)
			now := time.Now()
			evidence, prepared, auth := monthlySeriesPublicationFixture(t, m, group, now)
			if _, err := importMonthlyBillPrepared(context.Background(), m.storeDB, monthlyTestRaw(t, evidence[2]), true, now, auth, prepared); err != nil {
				t.Fatal(err)
			}
			old := monthlyFrozenRecord(t, m.storeDB, group.ID, evidence[2].Month)
			switch kind {
			case "auth":
				if err := m.storeDB.Model(&CustomerGroup{}).Where("id=?", group.ID).Update("portal_auth_ver", group.PortalAuthVer+1).Error; err != nil {
					t.Fatal(err)
				}
			case "source":
				prepared.evidence[evidence[2].Month] = strings.Repeat("b", 64)
			case "origin":
				prepared.scope.SourceIdentity = strings.Repeat("b", 64)
			case "start":
				prepared.scope.From = evidence[1].Month
			case "marker":
				if err := m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id=? AND month=?", group.ID, old.Month).Update("publication_id", monthlySeriesPublicationPrefix+"malformed").Error; err != nil {
					t.Fatal(err)
				}
			case "payload":
				if err := m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id=? AND month=?", group.ID, old.Month).Update("payload", "{}").Error; err != nil {
					t.Fatal(err)
				}
			case "private_evidence":
				if err := m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id=? AND month=?", group.ID, old.Month).Update("evidence", "{}").Error; err != nil {
					t.Fatal(err)
				}
			case "prepared_mutation":
				prepared.sources[0].Statement.Account = "changed after preparation"
			case "anchor":
				prepared.plan.AnchorSHA256 = strings.Repeat("b", 64)
			case "plan":
				prepared.plan.Months[0].Organization.Closing = "1.000000"
			}
			before := monthlyFrozenRecord(t, m.storeDB, group.ID, old.Month)
			if _, err := importMonthlyBillPrepared(context.Background(), m.storeDB, monthlyTestRaw(t, evidence[0]), true, now, auth, prepared); err == nil {
				t.Fatal("stale authorization/source/publication snapshot accepted")
			}
			monthlySeriesRecordCount(t, m.storeDB, 1)
			if before != monthlyFrozenRecord(t, m.storeDB, group.ID, old.Month) {
				t.Fatal("rejected transaction changed an existing publication")
			}
		})
	}
}

func TestMonthlyDisplayPublicationLegacyCompatibilityAndOldWriterStop(t *testing.T) {
	for _, compatible := range []bool{true, false} {
		t.Run(fmt.Sprint(compatible), func(t *testing.T) {
			m, _, group, _ := monthlyFrozenFixture(t)
			t.Cleanup(m.Close)
			now := time.Now()
			evidence, prepared, auth := monthlySeriesPublicationFixture(t, m, group, now)
			index := 0
			if !compatible {
				index = 2 // legacy standalone later month has the wrong cent anchor
			}
			if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, evidence[index]), true, now); err != nil {
				t.Fatal(err)
			}
			old := monthlyFrozenRecord(t, m.storeDB, group.ID, evidence[index].Month)
			_, err := importMonthlyBillPrepared(context.Background(), m.storeDB, monthlyTestRaw(t, evidence[1]), true, now, auth, prepared)
			if compatible && err != nil || !compatible && err == nil {
				t.Fatal("legacy compatibility decision wrong", err)
			}
			if monthlyFrozenRecord(t, m.storeDB, group.ID, evidence[index].Month) != old {
				t.Fatal("legacy bill rewritten during adoption")
			}
			if compatible {
				if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, evidence[2]), true, now); !errors.Is(err, errMonthlyDisplaySeriesIncomplete) {
					t.Fatal("old request-order-dependent writer extended a new series", err)
				}
				monthlySeriesRecordCount(t, m.storeDB, 2)
			} else {
				monthlySeriesRecordCount(t, m.storeDB, 1)
			}
		})
	}
}

func TestMonthlyDisplayPublicationConcurrentDifferentMonthsAndReplay(t *testing.T) {
	m, _, group, _ := monthlyFrozenFixture(t)
	t.Cleanup(m.Close)
	now := time.Now()
	evidence, prepared, auth := monthlySeriesPublicationFixture(t, m, group, now)
	var raw [][]byte
	for _, e := range evidence {
		raw = append(raw, monthlyTestRaw(t, e))
	}
	var workers sync.WaitGroup
	start, failures := make(chan struct{}), make(chan error, 6)
	for worker := range 6 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for repeat := range 3 {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				var err error
				for attempt := 0; attempt < 50; attempt++ {
					_, err = importMonthlyBillPrepared(ctx, m.storeDB, raw[(worker+repeat)%3], true, now, auth, prepared)
					if err == nil || !strings.Contains(err.Error(), "locked") && !strings.Contains(err.Error(), "SQLITE_BUSY") {
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
				cancel()
				if err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	close(start)
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Fatal("concurrent series generation failed", err)
	}
	monthlySeriesRecordCount(t, m.storeDB, 3)
	for i, e := range evidence {
		p, err := readPublishedMonthlyBill(monthlyFrozenRecord(t, m.storeDB, group.ID, e.Month))
		if err != nil || !reflect.DeepEqual(p.DisplayBalances.Organization, monthlyDisplayBalance{Opening: prepared.plan.Months[i].Organization.Opening, Closing: prepared.plan.Months[i].Organization.Closing}) {
			t.Fatal("parallel publication changed fixed cent amounts", err)
		}
	}
}

func TestMonthlyDisplayPublicationPreparationNeverInventsMissingSources(t *testing.T) {
	m, _, group, _ := monthlyFrozenFixture(t)
	t.Cleanup(m.Close)
	now := time.Now()
	evidence, prepared, _ := monthlySeriesPublicationFixture(t, m, group, now)
	makeSources := func() []monthlyDisplayPublicationSource {
		var sources []monthlyDisplayPublicationSource
		for _, e := range evidence {
			sources = append(sources, monthlyDisplayPublicationSource{Evidence: monthlyTestRaw(t, e), Account: group.Name, Names: map[int64]string{43: "synthetic"}})
		}
		return sources
	}
	for _, kind := range []string{"missing_month", "duplicate", "missing_name", "wrong_user", "wrong_group", "oversized", "too_many", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			sources := makeSources()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch kind {
			case "missing_month":
				sources = append(sources[:1], sources[2])
			case "duplicate":
				sources[1] = sources[0]
			case "missing_name":
				sources[0].Names = nil
			case "wrong_user":
				sources[0].Names = map[int64]string{44: "another account"}
			case "wrong_group":
				e := evidence[0]
				e.GroupID++
				sources[0].Evidence = monthlyTestRaw(t, e)
			case "oversized":
				sources[0].Evidence = bytes.Repeat([]byte(" "), MonthlyPreparationInputLimit+1)
			case "too_many":
				sources = make([]monthlyDisplayPublicationSource, monthlyDisplaySeriesMonthLimit+1)
			case "cancelled":
				cancel()
			}
			if p, err := monthlyPrepareDisplayPublication(ctx, prepared.scope, sources, now); err == nil || p != nil {
				t.Fatal("invalid source produced a usable partial preparation", err)
			}
		})
	}
	// Construction must detach caller-owned raw bytes and name maps.
	sources := makeSources()
	copyPrepared, err := monthlyPrepareDisplayPublication(context.Background(), prepared.scope, sources, now)
	if err != nil {
		t.Fatal(err)
	}
	for i := range sources {
		for j := range sources[i].Evidence {
			sources[i].Evidence[j] = 0
		}
		sources[i].Names[43] = "caller mutation"
	}
	if !reflect.DeepEqual(copyPrepared, prepared) {
		t.Fatal("prepared data retained a caller-owned mutable slice or map")
	}
	monthlySeriesRecordCount(t, m.storeDB, 0)
}

func TestMonthlyDisplayPublicationExtensionRetainsOriginAcrossRenames(t *testing.T) {
	m, _, group, _ := monthlyFrozenFixture(t)
	t.Cleanup(m.Close)
	now := time.Now()
	evidence, original, auth := monthlySeriesPublicationFixture(t, m, group, now)
	var sources []monthlyDisplayPublicationSource
	for _, e := range evidence {
		sources = append(sources, monthlyDisplayPublicationSource{Evidence: monthlyTestRaw(t, e), Account: group.Name, Names: map[int64]string{43: "synthetic"}})
	}
	scope := original.scope
	scope.Through = evidence[1].Month
	initial, err := monthlyPrepareDisplayPublication(context.Background(), scope, sources[:2], now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := importMonthlyBillPrepared(context.Background(), m.storeDB, sources[1].Evidence, true, now, auth, initial); err != nil {
		t.Fatal(err)
	}
	frozen := monthlyFrozenRecord(t, m.storeDB, group.ID, evidence[1].Month)
	if err := m.storeDB.Model(&CustomerGroup{}).Where("id=?", group.ID).Update("name", "renamed organization").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TrackedUser{}).Where("user_id=?", 43).Update("username", "renamed account").Error; err != nil {
		t.Fatal(err)
	}
	sources[2].Account, sources[2].Names[43] = "renamed organization", "renamed account"
	extended, err := monthlyPrepareDisplayPublication(context.Background(), original.scope, sources, now)
	if err != nil || extended.plan.AnchorSHA256 != initial.plan.AnchorSHA256 {
		t.Fatal("extending the range/renaming moved the retained first source", err)
	}
	// A fully valid, money-identical plan from a different origin is still not
	// the same private series. This must fail even though all wallet links match.
	otherScope := original.scope
	otherScope.SourceIdentity = strings.Repeat("b", 64)
	other, err := monthlyPrepareDisplayPublication(context.Background(), otherScope, sources, now)
	if err != nil || !reflect.DeepEqual(other.plan.Months, extended.plan.Months) {
		t.Fatal("origin-isolation fixture must have identical source amounts", err)
	}
	if _, err := importMonthlyBillPrepared(context.Background(), m.storeDB, sources[2].Evidence, true, now, auth, other); !errors.Is(err, errMonthlyDisplaySeriesFrozen) {
		t.Fatal("money-identical but different source origin replaced the retained origin", err)
	}
	for _, index := range []int{2, 0, 1} {
		if _, err := importMonthlyBillPrepared(context.Background(), m.storeDB, sources[index].Evidence, true, now, auth, extended); err != nil {
			t.Fatal("rename prevented a valid new/older month or replay", index, err)
		}
	}
	if monthlyFrozenRecord(t, m.storeDB, group.ID, evidence[1].Month) != frozen {
		t.Fatal("extension or rename rewrote a saved publication")
	}
	for i, wantName := range []string{"synthetic", "synthetic", "renamed account"} {
		p, err := readPublishedMonthlyBill(monthlyFrozenRecord(t, m.storeDB, group.ID, evidence[i].Month))
		if err != nil || p.Members[0].Username != wantName {
			t.Fatal("retained historical labels were replaced by current names", i, err)
		}
	}
}
