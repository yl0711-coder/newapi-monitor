package monthlydraft

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestRegistrationMonthsUsePlatformTimeNotTrafficOrMonitorEnrollment(t *testing.T) {
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, first, last string
		at, now           time.Time
		count             int
	}{
		{"September", "2026-09", "2026-09", time.Date(2026, 9, 12, 0, 0, 0, 0, time.UTC), now, 1},
		{"new_current_month", "2026-10", "2026-09", time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC), now, 0},
		{"CST_start", "2026-09", "2026-09", time.Date(2026, 8, 31, 16, 0, 0, 0, time.UTC), now, 1},
		{"before_CST_start", "2026-08", "2026-09", time.Date(2026, 8, 31, 15, 59, 59, 0, time.UTC), now, 2},
		{"CST_new_year", "2026-12", "2026-12", time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 12, 31, 16, 0, 0, 0, time.UTC), 1},
		{"leap_month", "2024-02", "2024-02", time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC), time.Date(2024, 2, 29, 16, 0, 0, 0, time.UTC), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := registrationMonths([]monthlysource.RegistrationTime{{UserID: 201, At: tc.at.Unix()}}, tc.now, 1, nil)
			if err != nil || got.FirstMonth != tc.first || got.LastMonth != tc.last || len(got.Months) != tc.count {
				t.Fatal(got, err)
			}
		})
	}
	rows := []monthlysource.RegistrationTime{{UserID: 201, At: time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC).Unix()}, {UserID: 202, At: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC).Unix()}}
	cal, err := registrationMonths(rows, now, 1, nil)
	if err != nil || !reflect.DeepEqual(cal.Months, []string{"2026-08", "2026-09"}) {
		t.Fatal("later member blocked existing account months", cal, err)
	}
	limited := &generationScope{exact: map[GenerationTarget]struct{}{{GroupID: 1, Month: "2026-09"}: {}, {GroupID: 2, Month: "2026-08"}: {}}}
	cal, err = registrationMonths(rows, now, 1, limited)
	if err != nil || !reflect.DeepEqual(cal.Months, []string{"2026-09"}) {
		t.Fatal("registration broadened the explicit rollout", cal, err)
	}
	for _, invalid := range [][]monthlysource.RegistrationTime{nil, {{UserID: 201, At: 0}}, {{UserID: 201, At: now.Add(time.Hour).Unix()}}, {{UserID: 201, At: rows[0].At}, {UserID: 201, At: rows[1].At}}} {
		if _, err := registrationMonths(invalid, now, 1, nil); err == nil {
			t.Fatal("unknown metadata was converted into a month")
		}
	}
}

func TestRegistrationCalendarActualSourceCacheGateAndMembership(t *testing.T) {
	f, auth, r, wallet, driver := walletPreparationFixture(t)
	// Deliberately over-broad SYNTHETIC rollout: the registration rule must
	// reject these months itself, not pass only because rollout rejects first.
	r.scope.exact[GenerationTarget{GroupID: 1, Month: "2026-08"}] = struct{}{}
	r.scope.exact[GenerationTarget{GroupID: 1, Month: time.Now().In(time.FixedZone("CST", 28800)).Format("2006-01")}] = struct{}{}
	d := &Deployment{runtime: r}
	if err := d.EnableRegistrationCalendar(wallet.funding); err != nil {
		t.Fatal(err)
	}
	if !d.MonthlyCalendarEnabled() {
		t.Fatal("calendar not installed")
	}
	if _, err := d.ReadMonthlyCalendar(t.Context(), 1, 1); err == nil {
		t.Fatal("stopped service read a source")
	}
	if driver.queries.Load() != 0 {
		t.Fatal("construction queried source")
	}
	if err := r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopRuntime(t, r) })
	for range 3 {
		cal, err := d.ReadMonthlyCalendar(t.Context(), 1, 1)
		if err != nil || !reflect.DeepEqual(cal.Months, []string{"2026-09"}) || cal.FirstMonth != "2026-09" {
			t.Fatal(cal, err)
		}
	}
	if driver.queries.Load() != 2 || driver.writes.Load() != 0 {
		t.Fatal("metadata not cached/read-only", driver.queries.Load())
	}
	for _, month := range []string{"2026-08", time.Now().In(time.FixedZone("CST", 28800)).Format("2006-01")} {
		if _, err := r.SubmitMonthly(t.Context(), 1, 1, month); !errors.Is(err, monitor.MonthlyGenerationFailure("invalid_request")) {
			t.Fatal("out of range month submitted", month)
		}
	}
	view, err := r.ReadMonthly(t.Context(), 1, 1, "2026-09")
	if err != nil || view.State != "not_requested" {
		t.Fatal(view, err)
	}
	var jobs int
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_source_preparation`).Scan(&jobs); err != nil || jobs != 0 {
		t.Fatal("metadata queued work", jobs, err)
	}
	if _, err := d.ReadMonthlyCalendar(t.Context(), 2, 1); err == nil || driver.queries.Load() != 2 {
		t.Fatal("foreign organization read source", err)
	}
	if err := auth.Create(&monitor.TrackedUser{UserID: 203, GroupID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := d.ReadMonthlyCalendar(t.Context(), 1, 1); err != nil || driver.queries.Load() != 4 {
		t.Fatal("new roster reused old cache", err)
	}
	if err := auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := d.ReadMonthlyCalendar(t.Context(), 1, 1); err == nil {
		t.Fatal("revoked session used cache")
	}
}

func TestRegistrationCalendarSourceFailureNoFallbackAndStopDrains(t *testing.T) {
	for _, mode := range []string{"failure", "missing", "revoke", "stop"} {
		t.Run(mode, func(t *testing.T) {
			_, auth, r, wallet, _ := walletPreparationFixture(t)
			d := &Deployment{runtime: r}
			if err := d.EnableRegistrationCalendar(wallet.funding); err != nil {
				t.Fatal(err)
			}
			s := r.service.(*registrationCalendarService)
			entered := make(chan struct{})
			s.read = func(ctx context.Context, ids []int64) ([]monthlysource.RegistrationTime, error) {
				switch mode {
				case "stop":
					close(entered)
					<-ctx.Done()
					return nil, ctx.Err()
				case "failure":
					return nil, errors.New("synthetic source unavailable")
				case "missing":
					return nil, nil
				case "revoke":
					if err := auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
						return nil, err
					}
				}
				rows := []monthlysource.RegistrationTime{}
				for _, id := range ids {
					rows = append(rows, monthlysource.RegistrationTime{UserID: id, At: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()})
				}
				return rows, nil
			}
			if err := r.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { stopRuntime(t, r) })
			if mode == "stop" {
				done := make(chan error, 1)
				go func() { _, err := d.ReadMonthlyCalendar(t.Context(), 1, 1); done <- err }()
				select {
				case <-entered:
				case <-time.After(3 * time.Second):
					t.Fatal("lookup never started")
				}
				stopRuntime(t, r)
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("stop returned a usable range")
					}
				case <-time.After(time.Second):
					t.Fatal("source lookup escaped stop")
				}
			} else if cal, err := d.ReadMonthlyCalendar(t.Context(), 1, 1); err == nil || len(cal.Months) != 0 {
				t.Fatal("source failure returned guessed range", cal, err)
			}
			if len(s.cache) != 0 {
				t.Fatal("failed result cached")
			}
		})
	}
}

func TestRegistrationCalendarMonthRolloverDoesNotFreezeInCachedMetadata(t *testing.T) {
	_, _, r, wallet, driver := walletPreparationFixture(t)
	d := &Deployment{runtime: r}
	if err := d.EnableRegistrationCalendar(wallet.funding); err != nil {
		t.Fatal(err)
	}
	s := r.service.(*registrationCalendarService)
	// Deliberately broad local test service; production rollout is unchanged.
	s.scope = nil
	now := time.Date(2026, 10, 31, 15, 59, 59, 0, time.UTC)
	s.now = func() time.Time { return now }
	if err := r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopRuntime(t, r) })
	first, err := d.ReadMonthlyCalendar(t.Context(), 1, 1)
	if err != nil || first.LastMonth != "2026-09" {
		t.Fatal(first, err)
	}
	now = now.Add(time.Second)
	next, err := d.ReadMonthlyCalendar(t.Context(), 1, 1)
	if err != nil || next.LastMonth != "2026-10" || !reflect.DeepEqual(next.Months, []string{"2026-09", "2026-10"}) || driver.queries.Load() != 2 {
		t.Fatal("month rollover reused old ceiling or queried source", next, err)
	}
	now = now.Add(10 * time.Minute)
	if _, err := d.ReadMonthlyCalendar(t.Context(), 1, 1); err != nil || driver.queries.Load() != 4 {
		t.Fatal("expired registration metadata never refreshed", err)
	}
}
