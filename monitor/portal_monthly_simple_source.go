package monitor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"gorm.io/gorm"
)

// Read-only inputs of the simple monthly bill generator.
//
//   - NewAPI (read replica, consistent snapshot): current wallet quota,
//     registration time, every wallet funding row of the members, and the
//     consumption tail after the local facts' verified boundary.
//   - Monitor usage facts (local, verified against NewAPI): daily consumption,
//     refunds, requests and tokens per account and API token.

type simpleEvent struct {
	ID     int64
	UserID int64
	At     int64
	Delta  int64 // quota, signed
	Kind   string
	Method string
}

type simpleSnapshot struct {
	CapturedAt int64
	Quota      map[int64]int64
	Registered map[int64]int64
	Events     []simpleEvent // wallet movements of members, any time >= fundingFrom
	Review     []int64       // unrecognized funding log IDs affecting members
	TailFrom   int64
	TailNet    map[int64]int64 // wallet consumption - refund since TailFrom
	// NewAPI subscriptions of the members, and the part of each day's logged
	// consumption that a subscription (not the wallet) paid, before TailFrom.
	Subscriptions []simpleSubscription
	SubDays       []simpleSubDay
}

type simpleSubscription struct {
	ID, UserID, PlanID int64
	Start, End         int64
	Status, Source     string
}

// simpleSubDay is subscription-paid usage of one token on one CST day.
type simpleSubDay struct {
	UserID, TokenID, DateTS int64
	Consume, Refund         int64 // quota paid by / returned to the subscription
}

// BalanceAt returns the wallet balance implied at TailFrom.
func (s simpleSnapshot) BalanceAtTail(uid int64) int64 {
	b := s.Quota[uid] + s.TailNet[uid]
	for _, e := range s.Events {
		if e.UserID == uid && e.At >= s.TailFrom {
			b -= e.Delta
		}
	}
	return b
}

type simpleBillSource interface {
	Registrations(ctx context.Context, ids []int64) (map[int64]int64, error)
	Snapshot(ctx context.Context, ids []int64, fundingFrom, tailFrom int64) (simpleSnapshot, error)
}

type simpleReplicaSource struct {
	db *sql.DB

	// Site-wide admin quota rows of closed weeks never change (logs are
	// append-only), and every organization's bill needs the same weeks. They
	// are read from the replica once per process instead of twice per bill.
	adminMu    sync.Mutex
	adminWeeks map[int64][]monthlysource.FundingLog // aligned week start -> rows
}

func newSimpleReplicaSource(db *sql.DB) *simpleReplicaSource {
	return &simpleReplicaSource{db: db, adminWeeks: map[int64][]monthlysource.FundingLog{}}
}

const simpleWeek = 7 * 86400

// simpleWeekStart aligns t to a fixed weekly grid (Mondays 00:00 CST) so all
// organizations share the same cached windows.
func simpleWeekStart(t int64) int64 {
	epoch := time.Date(2026, 1, 5, 0, 0, 0, 0, usageCST).Unix()
	d := (t - epoch) % simpleWeek
	if d < 0 {
		d += simpleWeek
	}
	return t - d
}

func simpleInList(ids []int64) (string, []any) {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return strings.TrimSuffix(strings.Repeat("?,", len(ids)), ","), args
}

func (s *simpleReplicaSource) Registrations(ctx context.Context, ids []int64) (map[int64]int64, error) {
	if s.db == nil || len(ids) == 0 {
		return nil, errors.New("simple bill source unavailable")
	}
	in, args := simpleInList(ids)
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(8000) */ id, created_at FROM users WHERE id IN ("+in+")", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var id int64
		var created sql.NullInt64
		if err := rows.Scan(&id, &created); err != nil {
			return nil, err
		}
		if created.Valid && created.Int64 > 0 {
			out[id] = created.Int64
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) != len(ids) {
		return nil, errors.New("member registration time missing")
	}
	return out, nil
}

// Snapshot reads everything in ONE read-only REPEATABLE READ transaction so the
// balance, the funding rows and the consumption tail share one InnoDB view.
func (s *simpleReplicaSource) Snapshot(ctx context.Context, ids []int64, fundingFrom, tailFrom int64) (simpleSnapshot, error) {
	out := simpleSnapshot{Quota: map[int64]int64{}, Registered: map[int64]int64{}, TailNet: map[int64]int64{}, TailFrom: tailFrom}
	if s.db == nil || len(ids) == 0 || fundingFrom <= 0 || tailFrom <= fundingFrom {
		return out, errors.New("simple bill snapshot input invalid")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer func() { _ = tx.Rollback() }()
	in, args := simpleInList(ids)
	rows, err := tx.QueryContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(8000) */ id, quota, created_at FROM users WHERE id IN ("+in+")", args...)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var id, quota int64
		var created sql.NullInt64
		if err := rows.Scan(&id, &quota, &created); err != nil {
			rows.Close()
			return out, err
		}
		out.Quota[id] = quota
		if created.Valid {
			out.Registered[id] = created.Int64
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}
	out.CapturedAt = time.Now().Unix()
	if len(out.Quota) != len(ids) {
		return out, errors.New("member wallet missing from source")
	}
	member := map[int64]bool{}
	for _, id := range ids {
		member[id] = true
	}
	seen := map[int64]bool{}
	review := map[int64]bool{}
	fetch := func(q string, qargs ...any) ([]monthlysource.FundingLog, error) {
		rows, err := tx.QueryContext(ctx, q, qargs...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []monthlysource.FundingLog
		for rows.Next() {
			var r monthlysource.FundingLog
			if err := rows.Scan(&r.ID, &r.UserID, &r.CreatedAt, &r.Type, &r.Content, &r.Other); err != nil {
				return nil, err
			}
			out = append(out, r)
		}
		return out, rows.Err()
	}
	collect := func(list []monthlysource.FundingLog) {
		for _, r := range list {
			if seen[r.ID] || r.CreatedAt < fundingFrom {
				continue
			}
			res := simpleClassifyFunding(r)
			target := res.TargetUserID
			if target == 0 && member[r.UserID] && r.Type != 3 {
				target = r.UserID // owner-recorded topup/system rows
			}
			if target == 0 && member[r.UserID] && r.Type == 3 && res.Status == monthlysource.FundingReview {
				target = r.UserID // cannot tell the beneficiary: block conservatively
			}
			if !member[target] {
				continue
			}
			seen[r.ID] = true
			switch res.Status {
			case monthlysource.FundingMovement:
				if res.DeltaQuota == nil || *res.DeltaQuota == 0 {
					review[r.ID] = true
					continue
				}
				out.Events = append(out.Events, simpleEvent{ID: r.ID, UserID: target, At: r.CreatedAt, Delta: *res.DeltaQuota, Kind: res.Kind, Method: res.Method})
			case monthlysource.FundingReview:
				review[r.ID] = true
			}
		}
	}
	for _, id := range ids {
		list, err := fetch("SELECT /*+ MAX_EXECUTION_TIME(15000) */ id, user_id, created_at, type, COALESCE(content,''), COALESCE(other,'') FROM logs FORCE INDEX (idx_user_created_type) WHERE user_id = ? AND created_at >= ? AND type IN (1,3,4) ORDER BY id", id, fundingFrom)
		if err != nil {
			return out, fmt.Errorf("funding rows: %w", err)
		}
		collect(list)
	}
	// RC4/RC26 admin quota operations may be recorded under the operator's
	// user_id with the beneficiary in other.op. Scan type=3 site-wide in
	// aligned weekly windows (rc48 no longer writes these to logs). Weeks that
	// closed more than a day before this snapshot are cached for the process.
	cacheBefore := out.CapturedAt - 86400
	for from := simpleWeekStart(fundingFrom); from < out.CapturedAt; from += simpleWeek {
		to := from + simpleWeek
		s.adminMu.Lock()
		list, cached := s.adminWeeks[from]
		s.adminMu.Unlock()
		if !cached {
			var err error
			list, err = fetch("SELECT /*+ MAX_EXECUTION_TIME(15000) */ id, user_id, created_at, type, COALESCE(content,''), COALESCE(other,'') FROM logs FORCE INDEX (idx_created_at_type) WHERE created_at >= ? AND created_at < ? AND type = 3 ORDER BY id", from, to)
			if err != nil {
				return out, fmt.Errorf("admin funding rows: %w", err)
			}
			if to <= cacheBefore && s.adminWeeks != nil {
				s.adminMu.Lock()
				s.adminWeeks[from] = list
				s.adminMu.Unlock()
			}
		}
		collect(list)
	}
	rows, err = tx.QueryContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(15000) */ user_id, type, COALESCE(SUM(quota),0) FROM logs FORCE INDEX (idx_user_created_type) WHERE user_id IN ("+in+") AND created_at >= ? AND type IN (2,6) GROUP BY user_id, type", append(args, tailFrom)...)
	if err != nil {
		return out, fmt.Errorf("consumption tail: %w", err)
	}
	for rows.Next() {
		var uid, q int64
		var typ int
		if err := rows.Scan(&uid, &typ, &q); err != nil {
			rows.Close()
			return out, err
		}
		if typ == 2 {
			out.TailNet[uid] += q
		} else {
			out.TailNet[uid] -= q
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}
	if err := s.subscriptionUsage(ctx, tx, ids, in, args, tailFrom, &out); err != nil {
		return out, err
	}
	for id := range review {
		out.Review = append(out.Review, id)
	}
	sort.Slice(out.Review, func(i, j int) bool { return out.Review[i] < out.Review[j] })
	sort.Slice(out.Events, func(i, j int) bool { return out.Events[i].ID < out.Events[j].ID })
	return out, nil
}

// subscriptionUsage reads the members' subscriptions and, for members that
// have one, the subscription-paid part of their logged usage (log
// other.billing_source = "subscription"; a request that overflowed to the
// wallet keeps its wallet part in other.wallet_quota_deducted). Only those
// members' logs inside their subscription periods are scanned. Usage at or
// after tailFrom is removed from the wallet tail; earlier usage is returned per
// token and day so it can be split out of the local daily facts.
func (s *simpleReplicaSource) subscriptionUsage(ctx context.Context, tx *sql.Tx, ids []int64, in string, args []any, tailFrom int64, out *simpleSnapshot) error {
	rows, err := tx.QueryContext(ctx, "SELECT /*+ MAX_EXECUTION_TIME(8000) */ id, user_id, plan_id, start_time, end_time, COALESCE(status,''), COALESCE(source,'') FROM user_subscriptions WHERE user_id IN ("+in+") ORDER BY id", args...)
	if err != nil {
		if strings.Contains(err.Error(), "1146") { // NewAPI without subscriptions
			return nil
		}
		return fmt.Errorf("subscriptions: %w", err)
	}
	from := map[int64]int64{}
	for rows.Next() {
		var sub simpleSubscription
		if err := rows.Scan(&sub.ID, &sub.UserID, &sub.PlanID, &sub.Start, &sub.End, &sub.Status, &sub.Source); err != nil {
			rows.Close()
			return err
		}
		out.Subscriptions = append(out.Subscriptions, sub)
		if f, ok := from[sub.UserID]; !ok || sub.Start < f {
			from[sub.UserID] = sub.Start
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	const window = 3 * 86400
	for _, uid := range ids {
		start, ok := from[uid]
		if !ok {
			continue
		}
		for t := start; t < out.CapturedAt+60; t += window {
			rows, err := tx.QueryContext(ctx, `SELECT /*+ MAX_EXECUTION_TIME(15000) */ FLOOR((created_at+28800)/86400)*86400-28800, token_id, type,
COALESCE(SUM(quota),0), COALESCE(SUM(CAST(COALESCE(JSON_EXTRACT(other,'$.wallet_quota_deducted'),0) AS SIGNED)),0)
FROM logs FORCE INDEX (idx_user_created_type) WHERE user_id = ? AND created_at >= ? AND created_at < ? AND type IN (2,6)
AND JSON_UNQUOTE(JSON_EXTRACT(other,'$.billing_source')) = 'subscription' GROUP BY 1, 2, 3`, uid, t, t+window)
			if err != nil {
				return fmt.Errorf("subscription usage: %w", err)
			}
			for rows.Next() {
				var day, token, quota, wallet int64
				var typ int
				if err := rows.Scan(&day, &token, &typ, &quota, &wallet); err != nil {
					rows.Close()
					return err
				}
				part := quota - wallet
				if part < 0 || wallet < 0 {
					rows.Close()
					return errSimpleReview(fmt.Sprintf("account %d subscription split out of range", uid))
				}
				if day >= tailFrom {
					if typ == 2 {
						out.TailNet[uid] -= part
					} else {
						out.TailNet[uid] += part
					}
					continue
				}
				d := simpleSubDay{UserID: uid, TokenID: token, DateTS: day}
				if typ == 2 {
					d.Consume = part
				} else {
					d.Refund = part
				}
				out.SubDays = append(out.SubDays, d)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- local usage facts ----

type simpleFactDay struct {
	UserID, TokenID int64
	TokenName       string
	DateTS          int64
	Requests        int64
	Tokens          int64
	Consume, Refund int64
}

type simpleFactsSource interface {
	// VerifiedThrough returns the earliest verified boundary (unix seconds)
	// over the members, and each member's source floor (first logged hour).
	VerifiedThrough(ctx context.Context, ids []int64) (int64, map[int64]int64, error)
	Days(ctx context.Context, ids []int64, from, to int64) ([]simpleFactDay, error)
}

type simpleLocalFacts struct{ db *gorm.DB }

func (f simpleLocalFacts) VerifiedThrough(ctx context.Context, ids []int64) (int64, map[int64]int64, error) {
	if f.db == nil {
		return 0, nil, errors.New("usage facts unavailable")
	}
	type row struct {
		UserID              int64
		CoverageStatus      string
		VerificationStatus  string
		VerifiedThroughHour int64
		SourceFloorHour     int64
	}
	var rows []row
	err := f.db.WithContext(ctx).Raw("SELECT user_id, coverage_status, verification_status, verified_through_hour, source_floor_hour FROM usage_fact_member_states WHERE user_id IN ?", ids).Scan(&rows).Error
	if err != nil {
		return 0, nil, err
	}
	if len(rows) != len(ids) {
		return 0, nil, errSimpleRetry("usage facts not tracked for every member")
	}
	through := int64(0)
	floors := map[int64]int64{}
	for i, r := range rows {
		if r.CoverageStatus != "ready" || r.VerificationStatus != "complete" || r.VerifiedThroughHour <= 0 {
			return 0, nil, errSimpleRetry("usage facts not verified for every member")
		}
		if i == 0 || r.VerifiedThroughHour < through {
			through = r.VerifiedThroughHour
		}
		floors[r.UserID] = r.SourceFloorHour
	}
	return through, floors, nil
}

func (f simpleLocalFacts) Days(ctx context.Context, ids []int64, from, to int64) ([]simpleFactDay, error) {
	var out []simpleFactDay
	err := f.db.WithContext(ctx).Raw(`SELECT user_id, token_id, MAX(token_name) AS token_name, date_ts,
SUM(requests) AS requests, SUM(prompt_tokens + completion_tokens) AS tokens, SUM(consume_quota) AS consume, SUM(refund_quota) AS refund
FROM usage_daily_facts WHERE user_id IN ? AND date_ts >= ? AND date_ts < ? GROUP BY user_id, token_id, date_ts`, ids, from, to).Scan(&out).Error
	return out, err
}
