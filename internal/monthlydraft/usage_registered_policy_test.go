package monthlydraft

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

func TestUsageRegisteredPolicyExplicitAndClosedMonthBoundary(t *testing.T) {
	for _, cfg := range []GenerationRollout{
		{Policy: GenerationPolicyUsageRegistered},
		{Enabled: true, Policy: "all"},
		{Enabled: true, Policy: " usage-registered"},
		{Enabled: true, Policy: GenerationPolicyUsageRegistered, Targets: []GenerationTarget{{GroupID: 1, Month: "2026-09"}}},
	} {
		if _, err := rolloutScope(cfg, time.Now()); err == nil {
			t.Fatal("ambiguous scope accepted", cfg)
		}
	}
	s, err := rolloutScope(GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}, time.Now())
	if err != nil || s == nil || !s.registered || len(s.exact) != 0 || s.authorize != nil {
		t.Fatal("implicit authority/targets installed", err)
	}
	now := time.Date(2026, 10, 31, 15, 59, 59, 0, time.UTC)
	s.now = func() time.Time { return now }
	for _, gid := range []int64{1, 81733, 9000001} {
		if !s.allows(gid, "2026-09") || s.allows(gid, "2026-10") {
			t.Fatal("unexpected closed-month eligibility", gid)
		}
	}
	for _, month := range []string{"", "*", "2026-9", "2026-09 ", "2026-00", "2026-13", "2019-12", "2027-01"} {
		if s.allows(1, month) {
			t.Fatal("bad month allowed", month)
		}
	}
	if s.allows(0, "2026-09") || s.allows(-1, "2026-09") {
		t.Fatal("invalid group allowed")
	}
	now = now.Add(time.Second)
	if !s.allows(81733, "2026-10") || s.allows(1, "2026-11") {
		t.Fatal("month boundary requires a scope/config rebuild")
	}
	now = time.Date(2026, 12, 31, 16, 0, 0, 0, time.UTC)
	if !s.allows(1, "2026-12") || s.allows(1, "2027-01") {
		t.Fatal("Beijing year boundary")
	}
	if _, err := decodePreparationInbox([]byte(`{"version":1,"entries":[]}`), s); err == nil {
		t.Fatal("registered policy accepted old manual inbox")
	}
}

func TestUsageRegisteredPolicyCannotStartWithoutRegistrationGuard(t *testing.T) {
	f := generationFixtureNew(t)
	_, ro, _ := usageAuthorityFixture(t)
	r, err := NewControlledLocalRuntime(f.g, ro, GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(t.Context()); !errors.Is(err, ErrGenerationInput) || r.Phase() != "disabled" || r.worker.Status().Running {
		t.Fatal("broad policy started without source eligibility", err)
	}
	generationAdmissionCounts(t, f.receiver, 0, 0)
	stopRuntime(t, r)
}

func usageRegisteredFixture(t *testing.T) (generationFixture, *gorm.DB, *LocalRuntime, *Deployment, *sourcePreparationDriver, *atomic.Int64) {
	t.Helper()
	f, auth, r, wallet, driver := walletPreparationFixture(t)
	s, err := rolloutScope(GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	clock := &atomic.Int64{}
	// Replay a past boundary so the independent existing generation core's
	// real wall-clock check also regards the newly closed month as closed.
	clock.Store(time.Date(2026, 9, 30, 15, 59, 59, 0, time.UTC).UnixNano())
	s.now = func() time.Time { return time.Unix(0, clock.Load()) }
	r.scope, r.worker.scope, r.preparation.scope = s, s, s
	// The fixture's original admission intentionally permits only one month;
	// the PRODUCT calendar/authorization policy is what this fixture exercises.
	r.preparation.admit = func(ctx context.Context, _ SourcePreparationRequest) error { return ctx.Err() }
	d := &Deployment{runtime: r}
	if err := d.EnableRegistrationCalendar(wallet.funding); err != nil {
		t.Fatal(err)
	}
	return f, auth, r, d, driver, clock
}

func TestUsageRegisteredPolicyNewGroupAndMonthWithoutTargetEdits(t *testing.T) {
	f, auth, r, d, driver, clock := usageRegisteredFixture(t)
	if driver.queries.Load() != 0 {
		t.Fatal("construction read source")
	}
	if err := r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopRuntime(t, r) })
	cal, err := d.ReadMonthlyCalendar(t.Context(), 1, 1)
	if err != nil || len(cal.Months) != 0 {
		t.Fatal(cal, err)
	}
	if _, err := r.SubmitMonthly(t.Context(), 1, 1, "2026-08"); err == nil {
		t.Fatal("preregistration month allowed")
	}
	if _, err := r.SubmitMonthly(t.Context(), 1, 1, "2026-09"); err == nil {
		t.Fatal("current month allowed")
	}
	clock.Add(int64(time.Second))
	cal, err = d.ReadMonthlyCalendar(t.Context(), 1, 1)
	if err != nil || !reflect.DeepEqual(cal.Months, []string{"2026-09"}) {
		t.Fatal("new month did not appear without restart", cal, err)
	}
	if driver.queries.Load() != 2 {
		t.Fatal("rollover repeated cached registration reads", driver.queries.Load())
	}
	group := monitor.CustomerGroup{ID: 81733, Name: "new synthetic policy group", PortalEmail: "new-policy@example.invalid", PortalAuthVer: 9}
	if err := auth.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	if err := auth.Create(&monitor.TrackedUser{UserID: 871601, GroupID: group.ID}).Error; err != nil {
		t.Fatal(err)
	}
	other, err := d.ReadMonthlyCalendar(t.Context(), group.ID, 9)
	if err != nil || !reflect.DeepEqual(other.Months, cal.Months) {
		t.Fatal("new Usage organization required a monthly target", other, err)
	}
	if view, err := r.ReadMonthly(t.Context(), group.ID, 9, "2026-09"); err != nil || view.State != "not_requested" {
		t.Fatal("valid new month was not eligible", view, err)
	}
	var count int
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_source_preparation`).Scan(&count); err != nil || count != 0 {
		t.Fatal("calendar/status caused preparation work", count, err)
	}
	view, err := r.SubmitMonthly(t.Context(), group.ID, 9, "2026-09")
	if err != nil || view.State == "not_prepared" {
		t.Fatal("new group could not submit scoped preparation", view, err)
	}
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_source_preparation WHERE group_id=?`, group.ID).Scan(&count); err != nil || count != 1 {
		t.Fatal("new group preparation identity", count, err)
	}
	if driver.writes.Load() != 0 {
		t.Fatal("eligibility wrote source")
	}
}

func TestUsageRegisteredPolicyRevocationMembershipAndUnknownRegistration(t *testing.T) {
	_, auth, r, d, driver, _ := usageRegisteredFixture(t)
	if err := r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopRuntime(t, r) })
	if _, err := d.ReadMonthlyCalendar(t.Context(), 1, 1); err != nil {
		t.Fatal(err)
	}
	old, _, err := LoadUsageGenerationOwner(t.Context(), auth, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Create(&monitor.TrackedUser{UserID: 203, GroupID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := r.scope.authorize(t.Context(), monthlytasks.Request{Owner: old, Month: "2026-09"}); err == nil {
		t.Fatal("old complete-member identity authorized")
	}
	if driver.queries.Load() != 4 {
		t.Fatal("changed membership reused cached registration")
	}
	before := driver.queries.Load()
	if err := auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := d.ReadMonthlyCalendar(t.Context(), 1, 1); err == nil || driver.queries.Load() != before {
		t.Fatal("revoked version used registration source", err)
	}
	if _, err := d.ReadMonthlyCalendar(t.Context(), 77777, 1); err == nil || driver.queries.Load() != before {
		t.Fatal("unknown organization used source", err)
	}
	s := r.service.(*registrationCalendarService)
	s.read = func(context.Context, []int64) ([]monthlysource.RegistrationTime, error) {
		return nil, errors.New("synthetic registration source unavailable")
	}
	if _, err := d.ReadMonthlyCalendar(t.Context(), 1, 2); err == nil {
		t.Fatal("source error treated as zero/known registration")
	}
	s.read = func(_ context.Context, ids []int64) ([]monthlysource.RegistrationTime, error) {
		rows := []monthlysource.RegistrationTime{}
		for _, id := range ids {
			rows = append(rows, monthlysource.RegistrationTime{UserID: id, At: 0})
		}
		return rows, nil
	}
	if _, err := d.ReadMonthlyCalendar(t.Context(), 1, 2); err == nil {
		t.Fatal("unknown registration manufactured a month")
	}
}

func TestUsageRegisteredPolicyGuardChecksBeforeExistingFinancialGuard(t *testing.T) {
	f, auth, r, wallet, driver := walletPreparationFixture(t)
	s, err := rolloutScope(GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	r.scope, r.worker.scope, r.preparation.scope = s, s, s
	var prior atomic.Int32
	f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error { prior.Add(1); return nil }
	d := &Deployment{runtime: r}
	if err := d.EnableRegistrationCalendar(wallet.funding); err != nil {
		t.Fatal(err)
	}
	owner, _, err := LoadUsageGenerationOwner(t.Context(), auth, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []string{"2026-08", now.In(time.FixedZone("CST", 28800)).Format("2006-01"), now.AddDate(0, 2, 0).Format("2006-01")} {
		if err := f.g.verifyCurrent(t.Context(), monthlytasks.Request{Owner: owner, Month: m}); err == nil {
			t.Fatal("financial worker bypassed month gate", m)
		}
	}
	if prior.Load() != 0 {
		t.Fatal("invalid eligibility reached the original guard")
	}
	if err := f.g.verifyCurrent(t.Context(), monthlytasks.Request{Owner: owner, Month: "2026-09"}); err != nil || prior.Load() != 1 {
		t.Fatal("valid task did not retain original guard", err)
	}
	calendar := r.service.(*registrationCalendarService)
	calendar.cache = map[monthlytasks.Owner]registrationCalendarEntry{}
	driver.onQuery = func() error { return errors.New("synthetic registration connection failure") }
	if err := f.g.verifyCurrent(t.Context(), monthlytasks.Request{Owner: owner, Month: "2026-09"}); err == nil || errors.Is(err, ErrGenerationChanged) || prior.Load() != 1 || len(calendar.cache) != 0 {
		t.Fatal("transient metadata failure became approval/revocation or was cached", err)
	}
	driver.onQuery = nil
	if err := f.g.verifyCurrent(t.Context(), monthlytasks.Request{Owner: owner, Month: "2026-09"}); err != nil || prior.Load() != 2 {
		t.Fatal("registration metadata did not recover", err)
	}
	if err := auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_email", "").Error; err != nil {
		t.Fatal(err)
	}
	if err := f.g.verifyCurrent(t.Context(), monthlytasks.Request{Owner: owner, Month: "2026-09"}); err == nil || prior.Load() != 2 {
		t.Fatal("revoked task reached financial guard", err)
	}
	stopRuntime(t, r)
}

func TestUsageRegisteredPolicyRevocationDuringSourceReadCannotCacheAuthority(t *testing.T) {
	f, auth, r, d, driver, _ := usageRegisteredFixture(t)
	var changed bool
	driver.onQuery = func() error {
		if !changed {
			changed = true
			return auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error
		}
		return nil
	}
	if err := r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopRuntime(t, r) })
	if _, err := d.ReadMonthlyCalendar(t.Context(), 1, 1); err == nil {
		t.Fatal("authorization changed during source query was accepted")
	}
	if len(r.service.(*registrationCalendarService).cache) != 0 {
		t.Fatal("raced authority cached")
	}
	var jobs int
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_source_preparation`).Scan(&jobs); err != nil || jobs != 0 {
		t.Fatal("revoked metadata created preparation work", jobs, err)
	}
}

func TestUsageRegisteredPolicyCurrentRegistrationAndUnknownValues(t *testing.T) {
	s, err := rolloutScope(GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC)
	rows := []monthlysource.RegistrationTime{{UserID: 1, At: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Unix()}}
	cal, err := registrationMonths(rows, now, 81733, s)
	if err != nil || len(cal.Months) != 0 || cal.FirstMonth != "2026-10" || cal.FirstAvailableAt != "2026-11-01" {
		t.Fatal("new current-month customer offered a closed bill", cal, err)
	}
	for _, v := range []int64{0, -1, now.Add(time.Hour).Unix()} {
		rows[0].At = v
		if _, err := registrationMonths(rows, now, 1, s); err == nil {
			t.Fatal("unknown/future registration accepted")
		}
	}
	rows[0].At = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()
	rows = append(rows, monthlysource.RegistrationTime{UserID: 2, At: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Unix()})
	cal, err = registrationMonths(rows, now, 1, s)
	if err != nil || strings.Join(cal.Months, ",") != "2026-09" {
		t.Fatal("later member changed group origin", cal, err)
	}
}

func TestUsageRegisteredPolicyActualDeploymentInstallsLazyEligibility(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	f.cfg.Rollout = GenerationRollout{Enabled: true, Policy: GenerationPolicyUsageRegistered}
	d := f.open(t)
	d.runtime.scope.now = func() time.Time { return time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC) }
	if err := d.Start(t.Context()); !errors.Is(err, ErrGenerationInput) || d.runtime.Phase() != "disabled" {
		t.Fatal("product owner started without registered source", err)
	}
	_, config := capturedPreparationConfigFixture(t, f)
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", config, capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	if d.capturedPreparation.source.Stats().OpenConnections != 0 || d.runtime.scope.authorize == nil || !d.MonthlyCalendarEnabled() {
		t.Fatal("startup was not lazy or missed guard")
	}
	if err := d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	calendar, err := d.ReadMonthlyCalendar(t.Context(), f.req.Owner.GroupID, f.req.Owner.AuthVersion)
	if err != nil || calendar.FirstMonth != "2026-09" || !reflect.DeepEqual(calendar.Months, []string{"2026-09"}) {
		t.Fatal("actual installed policy calendar", calendar, err)
	}
	view, err := d.ReadMonthly(t.Context(), f.req.Owner.GroupID, f.req.Owner.AuthVersion, "2026-09")
	if err != nil || view.State != "not_requested" {
		t.Fatal("actual product policy did not allow first request", view, err)
	}
	preparationCounts(t, f.f, 0, 0)
	res := d.capturedPreparation
	closeDeploymentTest(t, d)
	assertCapturedPreparationDBClosed(t, res.source)
	assertCapturedPreparationDBClosed(t, res.capture)
}
