package monitor

// Local scheduling metadata only. This table owns neither billing facts nor
// attribution. Every write is still performed by the existing independently
// fenced binding/cost writers. There is no upstream I/O in this pipeline.

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ChannelUpstreamTokenWorkState struct {
	Domain        string `gorm:"primaryKey;size:253"`
	AccountEpoch  string `gorm:"primaryKey;size:64"`
	Kind          string `gorm:"primaryKey;size:24"`
	DayTs         int64  `gorm:"primaryKey"`
	ChannelID     int    `gorm:"primaryKey"`
	Revision      int64
	InputRevision int64
	Status        string `gorm:"size:32"`
	Attempts      int
	NextAttemptAt int64
	LeaseUntil    int64
	CreatedAt     int64
	UpdatedAt     int64
}

func (ChannelUpstreamTokenWorkState) TableName() string { return "channel_upstream_token_work_states" }

const (
	upstreamTokenPrepareBudget = 2 * time.Second
	upstreamTokenWorkLease     = 60
	upstreamTokenCostRecheck   = 6 * 3600
)

func upstreamTokenWorkScope(db *gorm.DB, key ChannelUpstreamTokenWorkState) *gorm.DB {
	return db.Where("domain=? AND account_epoch=? AND kind=? AND day_ts=? AND channel_id=?", key.Domain, key.AccountEpoch, key.Kind, key.DayTs, key.ChannelID)
}

// Claim before heavy reads. A restart reclaims an expired lease; a stale
// completion cannot acknowledge a newer claim. The source-epoch runtime already
// allows only one active Monitor writer, but CAS also protects local overlap.
func (m *Monitor) claimUpstreamTokenWork(ctx context.Context, key ChannelUpstreamTokenWorkState, inputRevision, now int64) (ChannelUpstreamTokenWorkState, bool, error) {
	var result ChannelUpstreamTokenWorkState
	claimed := false
	if m.cfg.LocalSnapshotOnly {
		return result, false, errUpstreamTokenReadOnly
	}
	if now <= 0 || now > math.MaxInt64-upstreamTokenCostRecheck || inputRevision < 0 || !validSHA256Hex(key.AccountEpoch) ||
		!m.cfg.ChannelCostClosureEnabled || !channelCostDomainAllowed(m.cfg.ChannelCostClosureDomains, key.Domain) ||
		!(key.Kind == "binding" && key.DayTs == 0 && key.ChannelID == 0 || (key.Kind == "cost_day" && key.ChannelID > 0 || key.Kind == "scan_day" && key.ChannelID == 0) && key.DayTs > 0 && key.DayTs <= now-86400 && key.DayTs == cstDayStart(key.DayTs)) {
		return result, false, errors.New("令牌后台任务范围无效")
	}
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row ChannelUpstreamTokenWorkState
		q := upstreamTokenWorkScope(tx, key).Limit(1).Find(&row)
		if q.Error != nil {
			return q.Error
		}
		if q.RowsAffected == 0 {
			row = key
			row.CreatedAt, row.UpdatedAt = now, now
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
				return err
			}
			if err := upstreamTokenWorkScope(tx, key).Take(&row).Error; err != nil {
				return err
			}
		}
		changedProof := key.Kind == "cost_day" && row.Status == "prepared" && row.InputRevision != inputRevision
		// An explicit append-only configuration change is a new operator
		// intent; let it wake binding verification even during error cooldown.
		// Repeated upstream scans alone cannot do so.
		changedConfig := key.Kind == "binding" && row.InputRevision != inputRevision
		changedCredential := key.Kind == "scan_day" && row.Status == "reconnect" && row.InputRevision != inputRevision
		if row.LeaseUntil > now || row.NextAttemptAt > now && !changedProof && !changedConfig && !changedCredential {
			return nil
		}
		if row.Revision < 0 || row.Revision == math.MaxInt64 {
			return errors.New("令牌后台任务版本无效")
		}
		u := upstreamTokenWorkScope(tx.Model(&ChannelUpstreamTokenWorkState{}), key).Where("revision=? AND lease_until<=?", row.Revision, now).
			UpdateColumns(map[string]any{"revision": row.Revision + 1, "lease_until": now + upstreamTokenWorkLease, "updated_at": now})
		if u.Error != nil {
			return u.Error
		}
		if u.RowsAffected == 1 {
			row.Revision++
			row.LeaseUntil, row.UpdatedAt = now+upstreamTokenWorkLease, now
			result, claimed = row, true
		}
		return nil
	})
	return result, claimed, err
}

func (m *Monitor) finishUpstreamTokenWork(ctx context.Context, claim ChannelUpstreamTokenWorkState, inputRevision, now int64, status string, retry bool) error {
	return m.finishUpstreamTokenWorkWithRetryAt(ctx, claim, inputRevision, now, status, retry, 0)
}

func (m *Monitor) finishUpstreamTokenWorkWithRetryAt(ctx context.Context, claim ChannelUpstreamTokenWorkState, inputRevision, now int64, status string, retry bool, minimumNext int64) error {
	if m.cfg.LocalSnapshotOnly {
		return errUpstreamTokenReadOnly
	}
	attempts := 0
	next := now + 60
	if claim.Kind == "cost_day" || claim.Kind == "scan_day" && status == "observed" {
		next = now + upstreamTokenCostRecheck
	}
	if retry {
		attempts = claim.Attempts
		if attempts < 0 {
			attempts = 0
		}
		if attempts < 7 {
			attempts++
		} else {
			attempts = 7
		}
		next = now + int64(60*(1<<(attempts-1)))
	}
	if minimumNext > next {
		next = minimumNext
	}
	// Cancellation must not reset retries to a hot loop. This cleanup only
	// acknowledges this claim's tiny metadata row, with a separate short cap.
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 250*time.Millisecond)
	defer cancel()
	u := upstreamTokenWorkScope(m.storeDB.WithContext(finishCtx).Model(&ChannelUpstreamTokenWorkState{}), claim).
		Where("revision=? AND lease_until=?", claim.Revision, claim.LeaseUntil).
		UpdateColumns(map[string]any{"lease_until": 0, "input_revision": inputRevision, "status": status, "attempts": attempts, "next_attempt_at": next, "updated_at": now})
	if u.Error != nil {
		return u.Error
	}
	if u.RowsAffected != 1 {
		return errors.New("令牌后台任务已由更新的处理轮次接管")
	}
	return nil
}

func (m *Monitor) runUpstreamTokenWork(ctx context.Context, key ChannelUpstreamTokenWorkState, inputRevision, now int64, run func() (string, bool, error)) (bool, error) {
	claim, ok, err := m.claimUpstreamTokenWork(ctx, key, inputRevision, now)
	if err != nil || !ok {
		return false, err
	}
	status, retry, runErr := run()
	if runErr != nil {
		status, retry = "retry", true // never persist SQL, raw proof or credentials
	}
	finishErr := m.finishUpstreamTokenWork(ctx, claim, inputRevision, now, status, retry)
	return true, errors.Join(runErr, finishErr)
}

type upstreamTokenCostWorkCandidate struct {
	DayTs, CursorRevision int64
	ChannelID             int
}

// Select at most one configured historical channel-day with a complete shadow.
// Selecting a candidate is not authorization: the historical writer requires
// valid-time confirmed binding, current signed proof and whole-day reconciliation.
func (m *Monitor) nextUpstreamTokenCostWork(ctx context.Context, account ChannelUpstreamAccount, now int64) (upstreamTokenCostWorkCandidate, error) {
	var out upstreamTokenCostWorkCandidate
	err := m.storeDB.WithContext(ctx).Raw(`SELECT p.day_ts, p.revision AS cursor_revision, c.channel_id
	 FROM channel_upstream_token_scan_checkpoints p
	 JOIN channel_upstream_token_configs c ON c.domain=p.domain AND c.account_epoch=p.account_epoch
	 LEFT JOIN channel_upstream_token_work_states w ON w.domain=p.domain AND w.account_epoch=p.account_epoch
	  AND w.kind='cost_day' AND w.day_ts=p.day_ts AND w.channel_id=c.channel_id
	 WHERE p.domain=? AND p.account_epoch=? AND p.format=? AND p.day_ts>0 AND p.day_ts<=?
	  AND p.success_revision=p.revision AND p.stable_scans>=2 AND p.success_hash<>''
	  AND c.token_id<>'' AND c.created_at>0 AND c.created_at<p.day_ts+82800
	  AND (c.created_at>=p.day_ts OR NOT EXISTS (SELECT 1 FROM channel_upstream_token_configs n
	   WHERE n.domain=c.domain AND n.account_epoch=c.account_epoch AND n.channel_id=c.channel_id AND n.revision>c.revision AND n.created_at<p.day_ts))
	  AND COALESCE(w.lease_until,0)<=?
	  AND (COALESCE(w.next_attempt_at,0)<=? OR (w.status='prepared' AND w.input_revision<>p.revision))
	 GROUP BY p.day_ts,p.revision,c.channel_id
	 ORDER BY COALESCE(w.next_attempt_at,0),p.day_ts,c.channel_id LIMIT 1`,
		account.Domain, newAPIUpstreamAccountEpoch(account), sub2TokenCheckpointFormat, now-86400, now, now).Scan(&out).Error
	return out, err
}

// One account, at most one activation and one channel-day preparation per tick.
// No new goroutine, no provider requests, no scan of evidence payloads merely
// to find a job. Durable cooldowns are respected even if new bad scans arrive.
func (m *Monitor) syncDueUpstreamTokenWorkAt(ctx context.Context, now int64) {
	if m == nil || m.storeDB == nil || m.cfg.LocalSnapshotOnly || !m.cfg.ChannelCostClosureEnabled || len(m.cfg.ChannelCostClosureDomains) == 0 || now <= 86400 || now > math.MaxInt64-upstreamTokenCostRecheck {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, upstreamTokenPrepareBudget)
	defer cancel()
	domains := m.cfg.ChannelCostClosureDomains
	start := int(now / 60 % int64(len(domains)))
	for i := range domains {
		if ctx.Err() != nil {
			return
		}
		domain := strings.ToLower(strings.TrimSpace(domains[(start+i)%len(domains)]))
		var account ChannelUpstreamAccount
		if err := m.storeDB.WithContext(ctx).Select("domain", "provider", "base_url", "user_id", "account", "usage_adapter").Where("domain=?", domain).Take(&account).Error; err != nil {
			continue
		}
		if _, err := upstreamTokenBindingKind(account); err != nil {
			continue
		}
		epoch := newAPIUpstreamAccountEpoch(account)
		var anchor int
		if err := m.storeDB.WithContext(ctx).Raw(`SELECT c.channel_id FROM channel_upstream_token_configs c
		 JOIN channel_snaps s ON s.id=c.channel_id AND s.base_domain=c.domain
		 WHERE c.domain=? AND c.account_epoch=? ORDER BY c.channel_id LIMIT 1`, domain, epoch).Scan(&anchor).Error; err != nil || anchor <= 0 {
			continue
		}
		var configRevision int64
		if err := m.storeDB.WithContext(ctx).Raw(`SELECT COALESCE(MAX(rowid),0) FROM channel_upstream_token_configs WHERE domain=? AND account_epoch=?`, domain, epoch).Scan(&configRevision).Error; err != nil || configRevision <= 0 {
			continue
		}
		worked, err := m.runUpstreamTokenWork(ctx, ChannelUpstreamTokenWorkState{Domain: domain, AccountEpoch: epoch, Kind: "binding"}, configRevision, now, func() (string, bool, error) {
			result, err := m.advanceUpstreamTokenBindings(ctx, domain, anchor, now)
			return result.Status, result.Status == "waiting_evidence", err
		})
		if err != nil {
			slog.Warn("令牌归属后台核验待重试", "domain", domain) // details stay private
		}
		if account.Provider == upstreamProviderSub2API && ctx.Err() == nil {
			candidate, err := m.nextUpstreamTokenCostWork(ctx, account, now)
			if err != nil {
				slog.Warn("令牌历史成本任务暂不可读", "domain", domain)
			} else if candidate.ChannelID > 0 {
				key := ChannelUpstreamTokenWorkState{Domain: domain, AccountEpoch: epoch, Kind: "cost_day", DayTs: candidate.DayTs, ChannelID: candidate.ChannelID}
				did, err := m.runUpstreamTokenWork(ctx, key, candidate.CursorRevision, now, func() (string, bool, error) {
					result, err := m.prepareSub2TokenHistoricalCostDay(ctx, domain, candidate.ChannelID, candidate.DayTs, now)
					if err != nil {
						return "retry", true, err
					}
					if result.UpdatedHours+result.UnchangedHours == 0 {
						return "not_applicable", false, nil
					}
					return "prepared", false, nil
				})
				worked = worked || did
				if err != nil {
					slog.Warn("令牌历史成本后台核验待重试", "domain", domain, "day", candidate.DayTs)
				}
			}
		}
		if worked {
			return
		}
	}
}

func (m *Monitor) syncDueChannelCostPipeline(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, channelEconomicsTickTimeout)
	defer cancel()
	now := time.Now().Unix()
	m.syncDueUpstreamTokenWorkAt(ctx, now)
	m.syncDueChannelEconomicsAt(ctx, now)
}
