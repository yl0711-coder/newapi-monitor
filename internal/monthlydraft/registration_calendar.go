package monthlydraft

import (
	"context"
	"errors"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

type registrationCalendarEntry struct {
	rows []monthlysource.RegistrationTime
	at   time.Time
}

// Wrap the existing service; do not replace its task, money, selection or
// publication path. Cached metadata is keyed by COMPLETE authorized roster,
// not the organization label, and never caches financial amounts or failures.
type registrationCalendarService struct {
	base  monitor.MonthlyGenerationService
	auth  *gorm.DB
	scope *generationScope
	read  func(context.Context, []int64) ([]monthlysource.RegistrationTime, error)
	now   func() time.Time
	mu    sync.Mutex
	cache map[monthlytasks.Owner]registrationCalendarEntry
}

// EnableRegistrationCalendar is explicit before Start, AFTER on-demand source
// preparation installation. It starts no worker, writes no schema/data, does
// not broaden rollout and borrows the already validated SELECT-only source.
func (d *Deployment) EnableRegistrationCalendar(source *FundingSourcePreparation) error {
	if d == nil || d.runtime == nil || d.readOnly || source == nil || source.source == nil || source.authorization == nil {
		return ErrGenerationInput
	}
	r := d.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.phase != "disabled" || r.preparation == nil {
		return ErrGenerationBusy
	}
	if _, exists := r.service.(*registrationCalendarService); exists {
		return ErrGenerationInput
	}
	actualDB, err := r.preparation.auth.DB()
	if err != nil {
		return err
	}
	sourceDB, err := source.authorization.DB()
	if err != nil {
		return err
	}
	actual, err := generationDatabaseFile(actualDB)
	if err != nil {
		return err
	}
	other, err := generationDatabaseFile(sourceDB)
	if err != nil || !os.SameFile(actual, other) {
		return ErrGenerationInput
	}
	calendar := &registrationCalendarService{base: r.service, auth: r.preparation.auth, scope: r.scope, now: time.Now,
		cache: map[monthlytasks.Owner]registrationCalendarEntry{},
		read: func(ctx context.Context, ids []int64) ([]monthlysource.RegistrationTime, error) {
			return monthlysource.ReadRegistrationTimes(ctx, source.source, source.sourceIdentity, ids)
		}}
	if r.scope != nil && r.scope.registered {
		if r.scope.authorize != nil || r.scope.now == nil || r.worker == nil || r.worker.g == nil || r.worker.g.verifyCurrent == nil {
			return ErrGenerationInput
		}
		calendar.now = r.scope.now
		r.preparation.now = r.scope.now
		if r.worker.dynamicSeries {
			if !generationHex(source.sourceIdentity, 32) || r.worker.seriesIdentity != "" {
				return ErrGenerationInput
			}
			r.worker.seriesIdentity = source.sourceIdentity
		}
		r.scope.authorize = calendar.authorizeRequest
		prior := r.worker.g.verifyCurrent
		r.worker.g.verifyCurrent = func(ctx context.Context, req monthlytasks.Request) error {
			if err := calendar.authorizeRequest(ctx, req); err != nil {
				return err
			}
			return prior(ctx, req)
		}
	}
	r.service = calendar
	return nil
}

// Recheck the complete Usage owner as well as the registration window. The
// cached registration rows are roster-keyed; authorization is NEVER cached.
func (s *registrationCalendarService) authorizeRequest(ctx context.Context, req monthlytasks.Request) error {
	if ctx == nil {
		return ErrGenerationInput
	}
	if err := s.check(ctx, req.Owner.GroupID, req.Owner.AuthVersion, req.Month); err != nil {
		return err
	}
	owner, _, err := LoadUsageGenerationOwner(ctx, s.auth, req.Owner.GroupID, req.Owner.AuthVersion)
	if err != nil {
		return err
	}
	if owner != req.Owner {
		return ErrGenerationChanged
	}
	return ctx.Err()
}

func (d *Deployment) MonthlyCalendarEnabled() bool {
	if d == nil || d.runtime == nil {
		return false
	}
	d.runtime.mu.Lock()
	defer d.runtime.mu.Unlock()
	_, ok := d.runtime.service.(*registrationCalendarService)
	return ok
}

func (d *Deployment) ReadMonthlyCalendar(parent context.Context, gid, version int64) (monitor.MonthlyGenerationCalendar, error) {
	var empty monitor.MonthlyGenerationCalendar
	if parent == nil || d == nil || d.runtime == nil {
		return empty, monitor.MonthlyGenerationFailure("temporarily_unavailable")
	}
	r := d.runtime
	r.mu.Lock()
	s, ok := r.service.(*registrationCalendarService)
	if !ok || r.phase != "running" || r.ctx.Err() != nil {
		r.mu.Unlock()
		return empty, monitor.MonthlyGenerationFailure("temporarily_unavailable")
	}
	r.active++
	lifetime := r.ctx
	r.mu.Unlock()
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	stop := context.AfterFunc(lifetime, cancel)
	defer func() {
		stop()
		cancel()
		r.mu.Lock()
		r.active--
		r.finishLocked()
		r.mu.Unlock()
	}()
	calendar, err := s.calendar(ctx, gid, version)
	if ctx.Err() != nil || lifetime.Err() != nil {
		return empty, monitor.MonthlyGenerationFailure("temporarily_unavailable")
	}
	return calendar, err
}

func (s *registrationCalendarService) calendar(ctx context.Context, gid, version int64) (monitor.MonthlyGenerationCalendar, error) {
	var empty monitor.MonthlyGenerationCalendar
	owner, ids, err := LoadUsageGenerationOwner(ctx, s.auth, gid, version)
	if err != nil {
		return empty, preparationFailure(err)
	}
	if s.scope != nil && !s.scope.registered {
		allowed := false
		for target := range s.scope.exact {
			allowed = allowed || target.GroupID == gid
		}
		if !allowed {
			return empty, preparationFailure(ErrGenerationChanged)
		}
	}
	now := s.now()
	s.mu.Lock()
	entry, exists := s.cache[owner]
	s.mu.Unlock()
	if !exists || now.Before(entry.at) || now.Sub(entry.at) >= 10*time.Minute {
		rows, err := s.read(ctx, ids)
		if err != nil {
			if errors.Is(err, monthlysource.ErrRegistrationBusy) {
				err = ErrGenerationBusy
			}
			return empty, preparationFailure(err)
		}
		// Verify complete ID identity again at this adapter boundary. Metadata
		// is never accepted from a request body or from a different roster.
		if len(rows) != len(ids) {
			return empty, preparationFailure(ErrGenerationEvidence)
		}
		for i, row := range rows {
			if row.UserID != ids[i] || row.At <= 0 || row.At > now.Unix() {
				return empty, preparationFailure(ErrGenerationEvidence)
			}
		}
		entry = registrationCalendarEntry{rows: append([]monthlysource.RegistrationTime(nil), rows...), at: now}
	}
	current, _, err := LoadUsageGenerationOwner(ctx, s.auth, gid, version)
	if err != nil {
		return empty, preparationFailure(err)
	}
	if err := ctx.Err(); err != nil {
		return empty, preparationFailure(err)
	}
	if current != owner {
		return empty, preparationFailure(ErrGenerationChanged)
	}
	calendar, err := registrationMonths(entry.rows, now, gid, s.scope)
	if err != nil {
		return empty, preparationFailure(err)
	}
	s.mu.Lock()
	for key, old := range s.cache {
		if now.Sub(old.at) >= 10*time.Minute || key.GroupID == gid && key != owner {
			delete(s.cache, key)
		}
	}
	if _, cached := s.cache[owner]; cached || len(s.cache) < 256 {
		s.cache[owner] = entry
	}
	s.mu.Unlock()
	return calendar, nil
}

func registrationMonths(rows []monthlysource.RegistrationTime, now time.Time, gid int64, scope *generationScope) (monitor.MonthlyGenerationCalendar, error) {
	var empty monitor.MonthlyGenerationCalendar
	zone := time.FixedZone("Usage-UTC+8", 8*3600)
	current := now.In(zone)
	month := time.Date(current.Year(), current.Month(), 1, 0, 0, 0, 0, zone)
	first := ""
	if len(rows) == 0 || len(rows) > monthlysource.WalletBatchLimit {
		return empty, ErrGenerationEvidence
	}
	var lastID int64
	for _, row := range rows {
		registered := time.Unix(row.At, 0).In(zone)
		if row.UserID <= lastID || row.At <= 0 || registered.Year() < 2020 || registered.After(now) {
			return empty, ErrGenerationEvidence
		}
		lastID = row.UserID
		value := registered.Format("2006-01")
		if first == "" || value < first {
			first = value
		}
	}
	out := monitor.MonthlyGenerationCalendar{FirstMonth: first, LastMonth: month.AddDate(0, -1, 0).Format("2006-01"), Months: []string{}}
	registered, _ := time.ParseInLocation("2006-01", first, zone)
	out.FirstAvailableAt = registered.AddDate(0, 1, 0).Format("2006-01-02")
	if scope != nil && !scope.registered {
		for target := range scope.exact {
			if target.GroupID == gid && target.Month >= first && target.Month <= out.LastMonth {
				out.Months = append(out.Months, target.Month)
			}
		}
		sort.Strings(out.Months)
	} else {
		for at := registered; at.Before(month); at = at.AddDate(0, 1, 0) {
			out.Months = append(out.Months, at.Format("2006-01"))
		}
	}
	if len(out.Months) > 120 {
		return empty, ErrGenerationCapacity
	}
	return out, nil
}

func (s *registrationCalendarService) check(ctx context.Context, gid, version int64, month string) error {
	calendar, err := s.calendar(ctx, gid, version)
	if err != nil {
		return err
	}
	i := sort.SearchStrings(calendar.Months, month)
	if i == len(calendar.Months) || calendar.Months[i] != month {
		return monitor.MonthlyGenerationFailure("invalid_request")
	}
	return nil
}

func (s *registrationCalendarService) SubmitMonthly(ctx context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	if err := s.check(ctx, gid, version, month); err != nil {
		return monitor.MonthlyGenerationView{}, err
	}
	return s.base.SubmitMonthly(ctx, gid, version, month)
}

func (s *registrationCalendarService) ReadMonthly(ctx context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	if err := s.check(ctx, gid, version, month); err != nil {
		return monitor.MonthlyGenerationView{}, err
	}
	return s.base.ReadMonthly(ctx, gid, version, month)
}
