package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SimpleMonthlyGeneration is the customer "generate monthly bill" service:
// the customer clicks once, a single background worker builds the bill from a
// read-replica snapshot plus verified local usage facts, and publishes it
// through the existing validated publication path. Existing list/detail/Excel
// readers are unchanged.

// PortalMonthlySimpleJob is one customer request (organization, month).
type PortalMonthlySimpleJob struct {
	GroupID     int64  `gorm:"primaryKey;autoIncrement:false"`
	Month       string `gorm:"primaryKey;size:7"`
	AuthVersion int64
	State       string `gorm:"size:16;index"` // queued, running, retry, review, done
	Attempts    int
	// PublishAttempts counts failed publications only; earlier waiting
	// retries (facts not ready, balance moving) must not use them up.
	PublishAttempts int `gorm:"not null;default:0"`
	NextAt          int64
	LeaseUntil      int64
	LastError       string `gorm:"size:512"` // internal only, never returned to customers
	CreatedAt       int64
	UpdatedAt       int64
}

// PortalMonthlySimpleBoundary freezes an account's month-boundary balance the
// first time a bill depends on it. Raw is the reconciled wallet quota; Cents is
// the displayed amount. Both are immutable once written.
type PortalMonthlySimpleBoundary struct {
	UserID   int64  `gorm:"primaryKey;autoIncrement:false"`
	Month    string `gorm:"primaryKey;size:7"` // balance at 00:00 CST on the 1st
	GroupID  int64  `gorm:"index"`
	Raw      int64
	Cents    int64
	FrozenAt int64
}

// PortalMonthlySimpleAudit keeps the internal reconciliation of each issued
// bill (exact balances, logged totals, and WalletResidual = NewAPI wallet minus
// the log-implied balance at the bill's month end; internal only).
type PortalMonthlySimpleAudit struct {
	GroupID        int64  `gorm:"primaryKey;autoIncrement:false"`
	Month          string `gorm:"primaryKey;size:7"`
	UserID         int64  `gorm:"primaryKey;autoIncrement:false"`
	OpeningRaw     int64
	ClosingRaw     int64
	CreditRaw      int64
	LogConsume     int64
	LogRefund      int64
	Consume        int64
	WalletResidual int64
	OpeningCents   int64
	ClosingCents   int64
	ConsumeCents   int64
	RefundCents    int64
	CreditCents    int64
	Source         string `gorm:"size:256"`
	CreatedAt      int64
}

// A held request is retried when the customer asks again after this long.
const simpleReviewRetryAfter = 10 * 60

const (
	simpleJobQueued  = "queued"
	simpleJobRunning = "running"
	simpleJobRetry   = "retry"
	simpleJobReview  = "review"
	simpleJobDone    = "done"
)

type SimpleMonthlyGeneration struct {
	m             *Monitor
	source        simpleBillSource
	facts         simpleFactsSource
	now           func() time.Time
	wake          chan struct{}
	residualLimit int64
	maxAttempts   int
	settle        time.Duration // gap between the two snapshot reads

	// Registration time of an account never changes, so it is read from the
	// replica once and kept: customer clicks and calendar reads then answer
	// from memory and never hold an admission slot on a replica query.
	regMu    sync.Mutex
	regCache map[int64]int64 // user ID -> registration unix time

	// Customer-facing subscription plans (MONITOR_PORTAL_SUBSCRIPTION_PLANS)
	// and per-subscription overrides (MONITOR_PORTAL_SUBSCRIPTION_OVERRIDES).
	plans        map[int64]simplePlan
	subOverrides map[int64]simplePlan
}

// PortalMonthlySimpleSubscription is the subscription part of an issued bill
// (orders opened in the month, subscription-paid usage). Its SHA-256 is part
// of the bill's evidence source reference, so a reader can verify it.
type PortalMonthlySimpleSubscription struct {
	GroupID   int64  `gorm:"primaryKey;autoIncrement:false"`
	Month     string `gorm:"primaryKey;size:7"`
	Data      string
	Digest    string `gorm:"size:64"`
	CreatedAt int64
}

const simpleSubscriptionRefPrefix = ";subscriptions=sha256:"

// SimpleSubscriptionConfig parses the plan and override JSON objects, keyed by
// NewAPI plan ID / subscription ID:
//
//	{"2": {"name": "GPT模型调用服务月卡", "price": "10000.00"}}
//	{"16": {"trial": true}}
func SimpleSubscriptionConfig(plansJSON, overridesJSON string) (map[int64]simplePlan, map[int64]simplePlan, error) {
	parse := func(raw string) (map[int64]simplePlan, error) {
		out := map[int64]simplePlan{}
		if strings.TrimSpace(raw) == "" {
			return out, nil
		}
		var in map[string]struct {
			Name  string `json:"name"`
			Price string `json:"price"`
			Trial bool   `json:"trial"`
		}
		dec := json.NewDecoder(strings.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&in); err != nil {
			return nil, fmt.Errorf("subscription config: %w", err)
		}
		for k, v := range in {
			id, err := strconv.ParseInt(k, 10, 64)
			if err != nil || id <= 0 {
				return nil, fmt.Errorf("subscription config: invalid id %q", k)
			}
			p := simplePlan{Name: strings.TrimSpace(v.Name), Trial: v.Trial}
			if v.Price != "" {
				r, ok := new(big.Rat).SetString(v.Price)
				if !ok || r.Sign() < 0 {
					return nil, fmt.Errorf("subscription config: invalid price %q", v.Price)
				}
				c := new(big.Rat).Mul(r, big.NewRat(100, 1))
				if !c.IsInt() {
					return nil, fmt.Errorf("subscription config: price %q is not whole cents", v.Price)
				}
				p.PriceCents = c.Num().Int64()
			}
			out[id] = p
		}
		return out, nil
	}
	plans, err := parse(plansJSON)
	if err != nil {
		return nil, nil, err
	}
	overrides, err := parse(overridesJSON)
	return plans, overrides, err
}

// SetSubscriptionPlans installs the customer-facing plan configuration.
func (g *SimpleMonthlyGeneration) SetSubscriptionPlans(plans, overrides map[int64]simplePlan) {
	g.plans, g.subOverrides = plans, overrides
}

// NewSimpleMonthlyGeneration requires the read-only NewAPI connection, the
// local store and the usage facts store. It starts nothing.
func (m *Monitor) NewSimpleMonthlyGeneration() (*SimpleMonthlyGeneration, error) {
	if m == nil || m.storeDB == nil || m.usageFactsDB == nil || m.prodDB == nil || !m.cfg.PortalMonthlyStatementEnabled {
		return nil, errors.New("simple monthly generation requires the NewAPI read connection, usage facts and enabled monthly statements")
	}
	if err := m.storeDB.AutoMigrate(&PortalMonthlySimpleJob{}, &PortalMonthlySimpleBoundary{}, &PortalMonthlySimpleAudit{}, &PortalMonthlySimpleSubscription{}); err != nil {
		return nil, err
	}
	g := newSimpleMonthlyGeneration(m, newSimpleReplicaSource(m.prodDB), simpleLocalFacts{db: m.usageFactsDB})
	plans, overrides, err := SimpleSubscriptionConfig(os.Getenv("MONITOR_PORTAL_SUBSCRIPTION_PLANS"), os.Getenv("MONITOR_PORTAL_SUBSCRIPTION_OVERRIDES"))
	if err != nil {
		return nil, err
	}
	g.SetSubscriptionPlans(plans, overrides)
	return g, nil
}

func newSimpleMonthlyGeneration(m *Monitor, src simpleBillSource, facts simpleFactsSource) *SimpleMonthlyGeneration {
	return &SimpleMonthlyGeneration{m: m, source: src, facts: facts, now: time.Now, wake: make(chan struct{}, 1),
		residualLimit: 5 * 500000, maxAttempts: 40, settle: 20 * time.Second, regCache: map[int64]int64{},
		plans: map[int64]simplePlan{}, subOverrides: map[int64]simplePlan{}}
}

// ---- customer-facing service (MonthlyGenerationService + calendar) ----

func (g *SimpleMonthlyGeneration) MonthlyCalendarEnabled() bool { return true }

func (g *SimpleMonthlyGeneration) ReadMonthlyCalendar(ctx context.Context, gid, version int64) (MonthlyGenerationCalendar, error) {
	var out MonthlyGenerationCalendar
	scope, err := LoadMonthlyGenerationScope(ctx, g.m.storeDB, gid, version)
	if err != nil {
		return out, err
	}
	first, err := g.firstMonth(ctx, scope)
	if err != nil {
		return out, err
	}
	current := time.Date(g.now().In(usageCST).Year(), g.now().In(usageCST).Month(), 1, 0, 0, 0, 0, usageCST)
	start, _ := simpleMonthStart(first)
	out = MonthlyGenerationCalendar{FirstMonth: first, LastMonth: current.AddDate(0, -1, 0).Format("2006-01"),
		FirstAvailableAt: start.AddDate(0, 1, 0).Format("2006-01-02"), Months: []string{}}
	for t := start; t.Before(current); t = t.AddDate(0, 1, 0) {
		out.Months = append(out.Months, t.Format("2006-01"))
	}
	if len(out.Months) > 120 {
		return MonthlyGenerationCalendar{}, MonthlyGenerationFailure("temporarily_unavailable")
	}
	return out, nil
}

func (g *SimpleMonthlyGeneration) firstMonth(ctx context.Context, scope MonthlyGenerationScope) (string, error) {
	if len(scope.UserIDs) == 0 {
		return "", MonthlyGenerationFailure("invalid_request")
	}
	reg, err := g.registrations(ctx, scope.GroupID, scope.UserIDs)
	if err != nil {
		return "", MonthlyGenerationFailure("temporarily_unavailable")
	}
	first := ""
	for _, uid := range scope.UserIDs {
		if m := simpleMonthOf(reg[uid]); first == "" || m < first {
			first = m
		}
	}
	return first, nil
}

func (g *SimpleMonthlyGeneration) registrations(ctx context.Context, gid int64, ids []int64) (map[int64]int64, error) {
	out := make(map[int64]int64, len(ids))
	var missing []int64
	g.regMu.Lock()
	for _, id := range ids {
		if at, ok := g.regCache[id]; ok {
			out[id] = at
		} else {
			missing = append(missing, id)
		}
	}
	g.regMu.Unlock()
	if len(missing) == 0 {
		return out, nil
	}
	reg, err := g.source.Registrations(ctx, missing)
	if err != nil {
		return nil, err
	}
	g.regMu.Lock()
	for _, id := range missing {
		at, ok := reg[id]
		if !ok || at <= 0 {
			g.regMu.Unlock()
			return nil, errors.New("member registration time missing")
		}
		g.regCache[id] = at
		out[id] = at
	}
	g.regMu.Unlock()
	return out, nil
}

// warmRegistrations loads the registration time of every organization member
// in the background at startup, in small batches.
func (g *SimpleMonthlyGeneration) warmRegistrations(ctx context.Context) {
	var ids []int64
	if err := g.m.storeDB.WithContext(ctx).Model(&TrackedUser{}).Where("group_id > 0").Order("user_id").Pluck("user_id", &ids).Error; err != nil {
		slog.Warn("月度账单成员注册时间预加载失败", "err", err)
		return
	}
	for i := 0; i < len(ids); i += 100 {
		if ctx.Err() != nil {
			return
		}
		batch := ids[i:min(i+100, len(ids))]
		if _, err := g.registrations(ctx, 0, batch); err != nil {
			slog.Warn("月度账单成员注册时间预加载失败", "err", err)
			return // loaded on demand instead
		}
	}
	slog.Info("月度账单成员注册时间已预加载", "members", len(ids))
}

// eligible checks the month is a closed month on or after the organization's
// first registration month.
func (g *SimpleMonthlyGeneration) eligible(ctx context.Context, scope MonthlyGenerationScope, month string) error {
	if _, _, err := monthlyWindow(month, g.now()); err != nil {
		return MonthlyGenerationFailure("invalid_request")
	}
	first, err := g.firstMonth(ctx, scope)
	if err != nil {
		return err
	}
	if month < first {
		return MonthlyGenerationFailure("invalid_request")
	}
	return nil
}

func (g *SimpleMonthlyGeneration) ReadMonthly(ctx context.Context, gid, version int64, month string) (MonthlyGenerationView, error) {
	scope, err := LoadMonthlyGenerationScope(ctx, g.m.storeDB, gid, version)
	if err != nil {
		return MonthlyGenerationView{}, err
	}
	if err := g.eligible(ctx, scope, month); err != nil {
		return MonthlyGenerationView{}, err
	}
	var job PortalMonthlySimpleJob
	err = g.m.storeDB.WithContext(ctx).First(&job, "group_id = ? AND month = ?", gid, month).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return MonthlyGenerationView{Month: month, State: "not_requested"}, nil
	}
	if err != nil {
		return MonthlyGenerationView{}, MonthlyGenerationFailure("temporarily_unavailable")
	}
	view := MonthlyGenerationView{Month: month, State: simpleCustomerState(job.State)}
	view.Progress = g.progress(ctx, job)
	return view, nil
}

// simpleTypicalSeconds is a typical generation time (two snapshot reads 20 s
// apart plus the usage read), used only for the customer's progress estimate.
const simpleTypicalSeconds = 35

// progress estimates the customer-visible progress of one job: requests ahead
// of it in the queue, time since it was requested (or started), and a typical
// remaining duration.
func (g *SimpleMonthlyGeneration) progress(ctx context.Context, job PortalMonthlySimpleJob) *MonthlyGenerationProgress {
	now := g.now().Unix()
	switch job.State {
	case simpleJobQueued:
		var ahead int64
		g.m.storeDB.WithContext(ctx).Model(&PortalMonthlySimpleJob{}).
			Where("(state = ? AND lease_until > ?) OR (state IN (?, ?) AND next_at <= ? AND (next_at < ? OR (next_at = ? AND (created_at < ? OR (created_at = ? AND (group_id < ? OR (group_id = ? AND month < ?)))))))",
				simpleJobRunning, now, simpleJobQueued, simpleJobRetry, now, job.NextAt, job.NextAt, job.CreatedAt, job.CreatedAt, job.GroupID, job.GroupID, job.Month).Count(&ahead)
		return &MonthlyGenerationProgress{QueueAhead: int(ahead), ElapsedSeconds: max(0, now-job.CreatedAt), EstimatedSeconds: (ahead + 1) * simpleTypicalSeconds}
	case simpleJobRunning, simpleJobRetry:
		started := job.UpdatedAt // set when the job was claimed
		return &MonthlyGenerationProgress{ElapsedSeconds: max(0, now-started), EstimatedSeconds: simpleTypicalSeconds}
	}
	return nil
}

func simpleCustomerState(state string) string {
	switch state {
	case simpleJobQueued:
		return "queued"
	case simpleJobRunning, simpleJobRetry:
		return "processing" // automatic retries are not a customer-visible error
	case simpleJobReview:
		return "needs_review"
	default:
		return "not_requested" // done without a saved bill (e.g. withdrawn): may request again
	}
}

func (g *SimpleMonthlyGeneration) SubmitMonthly(ctx context.Context, gid, version int64, month string) (MonthlyGenerationView, error) {
	scope, err := LoadMonthlyGenerationScope(ctx, g.m.storeDB, gid, version)
	if err != nil {
		return MonthlyGenerationView{}, err
	}
	if err := g.eligible(ctx, scope, month); err != nil {
		return MonthlyGenerationView{}, err
	}
	now := g.now().Unix()
	// One write statement (no read-then-write transaction): SQLite then waits
	// for a concurrent writer under busy_timeout instead of failing at once.
	//   - new request: queued;
	//   - done: the HTTP layer already returned any saved bill, so requeue;
	//   - review: requeue when the organization changed (new auth version), or
	//     when the customer asks again at least simpleReviewRetryAfter seconds
	//     after it was held (data may have settled, e.g. a later month issued);
	//   - queued/running/retry: unchanged, including updated_at (the progress
	//     clock); a new auth version is picked up by the next attempt.
	// Every expression on the right reads the row as it was before the update.
	var state string
	err = g.m.storeDB.WithContext(ctx).Raw(`INSERT INTO portal_monthly_simple_jobs
(group_id, month, auth_version, state, attempts, publish_attempts, next_at, lease_until, last_error, created_at, updated_at)
VALUES (?, ?, ?, ?, 0, 0, ?, 0, '', ?, ?)
ON CONFLICT (group_id, month) DO UPDATE SET
  state = CASE WHEN state = ? OR (state = ? AND (auth_version <> excluded.auth_version OR updated_at <= excluded.updated_at - ?)) THEN ? ELSE state END,
  attempts = CASE WHEN state = ? OR (state = ? AND (auth_version <> excluded.auth_version OR updated_at <= excluded.updated_at - ?)) THEN 0 ELSE attempts END,
  publish_attempts = CASE WHEN state = ? OR (state = ? AND (auth_version <> excluded.auth_version OR updated_at <= excluded.updated_at - ?)) THEN 0 ELSE publish_attempts END,
  next_at = CASE WHEN state = ? OR (state = ? AND (auth_version <> excluded.auth_version OR updated_at <= excluded.updated_at - ?)) THEN excluded.next_at ELSE next_at END,
  lease_until = CASE WHEN state = ? OR (state = ? AND (auth_version <> excluded.auth_version OR updated_at <= excluded.updated_at - ?)) THEN 0 ELSE lease_until END,
  last_error = CASE WHEN state = ? OR (state = ? AND (auth_version <> excluded.auth_version OR updated_at <= excluded.updated_at - ?)) THEN '' ELSE last_error END,
  auth_version = excluded.auth_version,
  updated_at = CASE WHEN state IN (?, ?, ?) OR (state = ? AND auth_version = excluded.auth_version AND updated_at > excluded.updated_at - ?) THEN updated_at ELSE excluded.updated_at END
RETURNING state`,
		gid, month, version, simpleJobQueued, now, now, now,
		simpleJobDone, simpleJobReview, simpleReviewRetryAfter, simpleJobQueued,
		simpleJobDone, simpleJobReview, simpleReviewRetryAfter,
		simpleJobDone, simpleJobReview, simpleReviewRetryAfter,
		simpleJobDone, simpleJobReview, simpleReviewRetryAfter,
		simpleJobDone, simpleJobReview, simpleReviewRetryAfter,
		simpleJobDone, simpleJobReview, simpleReviewRetryAfter,
		simpleJobQueued, simpleJobRunning, simpleJobRetry, simpleJobReview, simpleReviewRetryAfter).Scan(&state).Error
	if err != nil || state == "" {
		slog.Warn("月度账单生成申请保存失败", "group", gid, "month", month, "err", err)
		return MonthlyGenerationView{}, MonthlyGenerationFailure("temporarily_unavailable")
	}
	select {
	case g.wake <- struct{}{}:
	default:
	}
	return MonthlyGenerationView{Month: month, State: simpleCustomerState(state)}, nil
}

// ---- worker ----

// Start runs the single background worker until ctx ends.
func (g *SimpleMonthlyGeneration) Start(ctx context.Context) error {
	g.requeueHeld(ctx)
	go g.warmRegistrations(ctx)
	go g.loop(ctx)
	return nil
}

// requeueHeld gives every held request one fresh attempt after a restart, so a
// fixed release settles requests held by an earlier version without manual
// database work. Nothing is published unless every check passes again; a
// request that still cannot be settled simply returns to review.
func (g *SimpleMonthlyGeneration) requeueHeld(ctx context.Context) {
	now := g.now().Unix()
	res := g.m.storeDB.WithContext(ctx).Model(&PortalMonthlySimpleJob{}).Where("state = ?", simpleJobReview).
		Updates(map[string]any{"state": simpleJobQueued, "attempts": 0, "publish_attempts": 0, "next_at": now, "lease_until": 0, "updated_at": now})
	if res.Error != nil {
		slog.Warn("月度账单待复核任务重新排队失败", "err", res.Error)
	} else if res.RowsAffected > 0 {
		slog.Info("月度账单待复核任务已重新排队", "count", res.RowsAffected)
	}
	// This process runs the only worker, so a request still leased at startup
	// was interrupted by the restart (e.g. a release). Resume it now instead of
	// waiting for the 15-minute lease to expire. Freezing is idempotent and an
	// already issued bill is detected, so a rerun never duplicates a bill.
	res = g.m.storeDB.WithContext(ctx).Model(&PortalMonthlySimpleJob{}).Where("state = ?", simpleJobRunning).
		Updates(map[string]any{"state": simpleJobQueued, "next_at": now, "lease_until": 0, "updated_at": now})
	if res.Error != nil {
		slog.Warn("月度账单中断任务恢复失败", "err", res.Error)
	} else if res.RowsAffected > 0 {
		slog.Info("月度账单中断任务已恢复排队", "count", res.RowsAffected)
	}
}

func (g *SimpleMonthlyGeneration) loop(ctx context.Context) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		for g.runOne(ctx) {
			if ctx.Err() != nil {
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-g.wake:
		}
	}
}

// runOne claims and processes at most one due job; it reports whether a job ran.
func (g *SimpleMonthlyGeneration) runOne(ctx context.Context) bool {
	job, ok := g.claim(ctx)
	if !ok {
		return false
	}
	runCtx, cancel := context.WithTimeout(ctx, 12*time.Minute)
	started := time.Now()
	err := g.generate(runCtx, job)
	cancel()
	g.finish(ctx, job, err)
	slog.Info("月度账单任务耗时", "group", job.GroupID, "month", job.Month, "ok", err == nil, "seconds", time.Since(started).Round(100*time.Millisecond).Seconds())
	return true
}

func (g *SimpleMonthlyGeneration) claim(ctx context.Context) (PortalMonthlySimpleJob, bool) {
	var jobs []PortalMonthlySimpleJob
	now := g.now().Unix()
	// Single UPDATE ... RETURNING on the queued writer: it takes SQLite's
	// writer slot up front and waits for it (startup backups and syncs write
	// concurrently), instead of failing the poll with SQLITE_BUSY.
	err := g.m.queuedWriteStore().WithContext(ctx).Raw(`UPDATE portal_monthly_simple_jobs
SET state = ?, lease_until = ?, updated_at = ?
WHERE (group_id, month) = (
  SELECT group_id, month FROM portal_monthly_simple_jobs
  WHERE (state IN (?, ?) AND next_at <= ?) OR (state = ? AND lease_until <= ?)
  ORDER BY next_at, created_at, group_id, month LIMIT 1)
RETURNING *`, simpleJobRunning, now+15*60, now,
		simpleJobQueued, simpleJobRetry, now, simpleJobRunning, now).Scan(&jobs).Error
	if err != nil {
		slog.Warn("月度账单任务领取失败", "err", err)
		return PortalMonthlySimpleJob{}, false
	}
	if len(jobs) != 1 {
		return PortalMonthlySimpleJob{}, false
	}
	return jobs[0], true
}

func (g *SimpleMonthlyGeneration) finish(ctx context.Context, job PortalMonthlySimpleJob, err error) {
	now := g.now().Unix()
	updates := map[string]any{"updated_at": now, "lease_until": 0}
	var retry errSimpleRetry
	switch {
	case err == nil:
		updates["state"], updates["last_error"] = simpleJobDone, ""
	case ctx.Err() != nil:
		// The service is stopping (e.g. a release): whatever failed was caused
		// by the shutdown, not by the bill. Resume first thing after restart,
		// without counting an attempt; a bill already issued is detected then.
		updates["state"], updates["next_at"], updates["last_error"] = simpleJobQueued, now, "interrupted by shutdown"
	case errors.As(err, new(errSimplePublication)):
		attempts, published := job.Attempts+1, job.PublishAttempts+1
		updates["attempts"], updates["publish_attempts"] = attempts, published
		updates["last_error"] = simpleTrim(err.Error())
		if published >= simplePublishAttempts || attempts >= g.maxAttempts {
			updates["state"] = simpleJobReview
		} else {
			delay := time.Duration(30<<min(attempts, 5)) * time.Second
			updates["state"], updates["next_at"] = simpleJobRetry, now+int64(delay/time.Second)
		}
	case errors.As(err, &retry) || !errors.As(err, new(errSimpleReview)):
		attempts := job.Attempts + 1
		updates["attempts"] = attempts
		updates["last_error"] = simpleTrim(err.Error())
		if attempts >= g.maxAttempts {
			updates["state"] = simpleJobReview
		} else {
			delay := time.Duration(30<<min(attempts, 5)) * time.Second // 1m .. 16m
			updates["state"], updates["next_at"] = simpleJobRetry, now+int64(delay/time.Second)
		}
	default:
		updates["state"], updates["last_error"] = simpleJobReview, simpleTrim(err.Error())
	}
	// A lost status write would leave the job leased (shown as processing) for
	// up to 15 minutes, so retry briefly before giving up to lease expiry.
	for try := 0; ; try++ {
		e := g.m.storeDB.WithContext(context.WithoutCancel(ctx)).Model(&PortalMonthlySimpleJob{}).
			Where("group_id = ? AND month = ?", job.GroupID, job.Month).Updates(updates).Error
		if e == nil {
			break
		}
		if try == 4 {
			slog.Warn("月度账单任务状态保存失败", "group", job.GroupID, "month", job.Month, "err", e)
			break
		}
		time.Sleep(time.Duration(200<<try) * time.Millisecond)
	}
	if err != nil {
		slog.Warn("月度账单生成未完成", "group", job.GroupID, "month", job.Month, "state", updates["state"], "err", simpleTrim(err.Error()))
	} else {
		slog.Info("月度账单已生成", "group", job.GroupID, "month", job.Month)
	}
}

func simpleTrim(s string) string {
	if len(s) > 500 {
		return s[:500]
	}
	return s
}

var errSimpleBillAlreadyIssued = errors.New("monthly bill already issued")

// generate builds and publishes one bill.
func (g *SimpleMonthlyGeneration) generate(ctx context.Context, job PortalMonthlySimpleJob) error {
	scope, err := LoadMonthlyGenerationScope(ctx, g.m.storeDB, job.GroupID, job.AuthVersion)
	if errors.Is(err, ErrMonthlyGenerationUnauthorized) {
		// A newer request for the changed organization arrived while this
		// attempt held the old version: run again with it, not review.
		var current int64
		if e := g.m.storeDB.WithContext(ctx).Model(&PortalMonthlySimpleJob{}).Where("group_id = ? AND month = ?", job.GroupID, job.Month).
			Pluck("auth_version", &current).Error; e == nil && current != job.AuthVersion {
			return errSimpleRetry("organization changed; a newer request is pending")
		}
		return errSimpleReview("organization authorization changed since the request")
	}
	if err != nil {
		return errSimpleRetry("authorization store: " + err.Error())
	}
	// A failed lookup must not be read as "not issued": regenerating would
	// rewrite the subscription part that an issued bill is verified against.
	saved, err := g.m.monthlyGenerationSavedBill(ctx, scope, job.Month)
	if err != nil {
		return errSimpleRetry("saved bill lookup: " + err.Error())
	}
	if saved != nil {
		return nil // already issued
	}
	ids := scope.UserIDs
	through, floors, err := g.facts.VerifiedThrough(ctx, ids)
	if err != nil {
		return err
	}
	// Facts are daily; use the last verified CST midnight as the tail boundary.
	t := time.Unix(through, 0).In(usageCST)
	tailFrom := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, usageCST).Unix()
	reg, err := g.source.Registrations(ctx, ids)
	if err != nil {
		return errSimpleRetry("registrations: " + err.Error())
	}
	fundingFrom := int64(0)
	for _, uid := range ids {
		start, _ := simpleMonthStart(simpleMonthOf(reg[uid]))
		if fundingFrom == 0 || start.Unix() < fundingFrom {
			fundingFrom = start.Unix()
		}
	}
	// Two snapshots some seconds apart must agree on every implied balance;
	// otherwise a request was in flight (charged but not yet logged).
	readStarted := time.Now()
	first, err := g.source.Snapshot(ctx, ids, fundingFrom, tailFrom)
	if err != nil {
		return errSimpleRetry("snapshot: " + err.Error())
	}
	firstRead := time.Since(readStarted)
	select {
	case <-ctx.Done():
		return errSimpleRetry("cancelled")
	case <-time.After(g.settle):
	}
	readStarted = time.Now()
	snap, err := g.source.Snapshot(ctx, ids, fundingFrom, tailFrom)
	if err != nil {
		return errSimpleRetry("snapshot: " + err.Error())
	}
	slog.Info("月度账单来源读取完成", "group", job.GroupID, "month", job.Month, "members", len(ids),
		"first_read_ms", firstRead.Milliseconds(), "second_read_ms", time.Since(readStarted).Milliseconds(), "funding_rows", len(snap.Events))
	for _, uid := range ids {
		if first.BalanceAtTail(uid) != snap.BalanceAtTail(uid) {
			return errSimpleRetry(fmt.Sprintf("account %d balance moved between snapshots", uid))
		}
	}
	days, err := g.facts.Days(ctx, ids, fundingFrom, tailFrom)
	if err != nil {
		return errSimpleRetry("usage facts: " + err.Error())
	}
	var boundaries []PortalMonthlySimpleBoundary
	if err := g.m.storeDB.WithContext(ctx).Where("user_id IN ?", ids).Find(&boundaries).Error; err != nil {
		return errSimpleRetry("boundaries: " + err.Error())
	}
	frozen := map[simpleBoundaryKey]simpleFrozen{}
	for _, b := range boundaries {
		frozen[simpleBoundaryKey{b.UserID, b.Month}] = simpleFrozen{Raw: b.Raw, Cents: b.Cents}
	}
	ref := fmt.Sprintf("newapi-replica-snapshot@%d;usage-daily-facts<%d", snap.CapturedAt, tailFrom)
	out, err := simpleBuildOrgMonth(simpleOrgInput{GroupID: job.GroupID, Month: job.Month, Members: ids, Snapshot: snap, Days: days,
		SourceFloor: floors, Frozen: frozen, ResidualLimit: g.residualLimit, Now: g.now(), SourceRef: ref,
		Plans: g.plans, SubOverrides: g.subOverrides})
	if err != nil {
		return err
	}
	var subRow *PortalMonthlySimpleSubscription
	if out.Sub != nil {
		data, err := json.Marshal(out.Sub)
		if err != nil {
			return errSimpleReview("subscription encoding")
		}
		sum := sha256.Sum256(data)
		digest := hex.EncodeToString(sum[:])
		out.Evidence.SourceReference += simpleSubscriptionRefPrefix + digest
		subRow = &PortalMonthlySimpleSubscription{GroupID: job.GroupID, Month: job.Month, Data: string(data), Digest: digest, CreatedAt: g.now().Unix()}
	}
	raw, err := json.Marshal(out.Evidence)
	if err != nil {
		return errSimpleReview("evidence encoding")
	}
	// Validate exactly as publication will, before freezing anything.
	if _, _, err := buildPublishedMonthlyBill(out.Evidence, g.now()); err != nil {
		return errSimpleReview("bill validation: " + err.Error())
	}
	if err := g.freeze(ctx, job.GroupID, out); err != nil {
		return errSimpleRetry("freeze: " + err.Error())
	}
	// Stored before publication (a reader verifies it against the digest in
	// the published evidence); replaced freely until the bill is issued, and
	// never after: the check and the write share one transaction.
	if subRow != nil {
		err := g.m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var issued int64
			if err := tx.Model(&PortalMonthlyBill{}).Where("group_id = ? AND month = ?", job.GroupID, job.Month).Count(&issued).Error; err != nil {
				return err
			}
			if issued > 0 {
				return errSimpleBillAlreadyIssued
			}
			return tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(subRow).Error
		})
		if errors.Is(err, errSimpleBillAlreadyIssued) {
			return errSimpleRetry("bill already issued; re-reading it")
		}
		if err != nil {
			return errSimpleRetry("subscription store: " + err.Error())
		}
	}
	expected := MonthlyBillExpectedAuthorization{GroupID: scope.GroupID, AuthVersion: scope.AuthVersion, MemberFingerprint: scope.MemberFingerprint, MemberManifest: scope.MemberManifest}
	if _, err := g.m.PublishMonthlyBillForAuthorization(ctx, raw, expected); err != nil {
		// Membership changed during generation, a busy store, ...: try again
		// twice; a failure that persists (e.g. a conflicting saved bill) is held.
		return errSimplePublication("publication: " + err.Error())
	}
	g.saveAudit(ctx, job, out, ref)
	return nil
}

// freeze writes newly decided boundaries; existing rows are never changed.
func (g *SimpleMonthlyGeneration) freeze(ctx context.Context, gid int64, out simpleOrgOutput) error {
	keys := make([]simpleBoundaryKey, 0, len(out.Freeze))
	for k := range out.Freeze {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].UserID != keys[j].UserID {
			return keys[i].UserID < keys[j].UserID
		}
		return keys[i].Month < keys[j].Month
	})
	now := g.now().Unix()
	return g.m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, k := range keys {
			v := out.Freeze[k]
			row := PortalMonthlySimpleBoundary{UserID: k.UserID, Month: k.Month, GroupID: gid, Raw: v.Raw, Cents: v.Cents, FrozenAt: now}
			res := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected == 0 {
				var cur PortalMonthlySimpleBoundary
				if err := tx.First(&cur, "user_id = ? AND month = ?", k.UserID, k.Month).Error; err != nil {
					return err
				}
				if cur.Raw != v.Raw || cur.Cents != v.Cents {
					return errors.New("boundary frozen concurrently with a different value")
				}
			}
		}
		return nil
	})
}

func (g *SimpleMonthlyGeneration) saveAudit(ctx context.Context, job PortalMonthlySimpleJob, out simpleOrgOutput, ref string) {
	now := g.now().Unix()
	rows := make([]PortalMonthlySimpleAudit, 0, len(out.Audit))
	for _, a := range out.Audit {
		rows = append(rows, PortalMonthlySimpleAudit{GroupID: job.GroupID, Month: job.Month, UserID: a.UserID,
			OpeningRaw: a.OpeningRaw, ClosingRaw: a.ClosingRaw, CreditRaw: a.CreditRaw, LogConsume: a.LogConsume, LogRefund: a.LogRefund,
			Consume: a.Consume, WalletResidual: a.WalletResidual, OpeningCents: a.OpeningCents, ClosingCents: a.ClosingCents, ConsumeCents: a.ConsumeCents,
			RefundCents: a.RefundCents, CreditCents: a.CreditCents, Source: simpleTrim(ref), CreatedAt: now})
		if a.WalletResidual != 0 {
			slog.Info("月度账单内部对账：钱包与日志推算余额存在差额(不影响账单)", "group", job.GroupID, "month", job.Month, "user", a.UserID, "residual_quota", a.WalletResidual)
		}
	}
	if len(rows) == 0 {
		return
	}
	if err := g.m.storeDB.WithContext(context.WithoutCancel(ctx)).Clauses(clause.OnConflict{UpdateAll: true}).Create(&rows).Error; err != nil {
		slog.Warn("月度账单核对记录保存失败", "group", job.GroupID, "month", job.Month, "err", err)
	}
}
