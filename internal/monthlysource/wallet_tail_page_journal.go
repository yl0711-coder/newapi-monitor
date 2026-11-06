package monthlysource

// Private, explicit capture-receiver setup only. These checkpoints are NOT
// complete-tail evidence and are not installed in the generation runtime.
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"
)

const walletTailJournalPages = 4096

type WalletTailPageJournalProgress struct {
	Pages           int                  `json:"pages"`
	After           WalletTailPageCursor `json:"after"`
	LastSHA         string               `json:"last_sha256"`
	Traversed       bool                 `json:"traversed"` // NOT financial completeness
	Captured        bool                 `json:"captured"`
	LastCompletedMS int64                `json:"last_completed_at_ms"`
	RegisteredAt    int64                `json:"registered_at"`
}

type walletTailPageSeries struct {
	JobKey, PlanSHA, ScopeSHA string
	Request                   WalletTailPageRequest
	Pass                      int
}

func (s walletTailPageSeries) id() string {
	// Wall-clock rollover must hit the existing series and stop on a scope
	// mismatch, not allocate a fresh chain under the same job/pass.
	s.Request.Wallet.Boundary = 0
	raw, _ := json.Marshal(s)
	return digest(raw)
}

// No source DDL, startup hook, customer endpoint, replacement or eviction.
// Each immutable row commits the original page and its cursor together.
func InitWalletTailPageJournal(parent context.Context, db *sql.DB) error {
	if parent == nil || db == nil {
		return ErrWalletMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	if err := registeredWalletSQLite(ctx, db); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_tail_page_journal (
series TEXT NOT NULL CHECK(length(series)=64),seq INTEGER NOT NULL CHECK(seq>=0 AND seq<4096),
id TEXT NOT NULL CHECK(length(id)=64),previous_id TEXT NOT NULL,
started_at_ms INTEGER NOT NULL,completed_at_ms INTEGER NOT NULL CHECK(completed_at_ms>=started_at_ms),
payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=2097152),
PRIMARY KEY(series,seq))`,
		`CREATE TRIGGER IF NOT EXISTS monthly_tail_page_no_update BEFORE UPDATE ON portal_monthly_tail_page_journal BEGIN SELECT RAISE(ABORT,'tail pages are immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_tail_page_no_delete BEFORE DELETE ON portal_monthly_tail_page_journal BEGIN SELECT RAISE(ABORT,'tail pages are immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_tail_page_append BEFORE INSERT ON portal_monthly_tail_page_journal
WHEN EXISTS(SELECT 1 FROM portal_monthly_tail_page_journal WHERE series=NEW.series AND seq=NEW.seq) OR
NEW.seq<>(SELECT count(*) FROM portal_monthly_tail_page_journal WHERE series=NEW.series) OR
(NEW.seq=0 AND NEW.previous_id<>'') OR
(NEW.seq>0 AND NOT EXISTS(SELECT 1 FROM portal_monthly_tail_page_journal WHERE series=NEW.series AND seq=NEW.seq-1 AND id=NEW.previous_id AND completed_at_ms<=NEW.started_at_ms))
BEGIN SELECT RAISE(ABORT,'tail page chain cannot be replaced or skipped'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_tail_page_capacity BEFORE INSERT ON portal_monthly_tail_page_journal
WHEN (SELECT count(*) FROM portal_monthly_tail_page_journal)>=4096 OR
(SELECT coalesce(sum(length(payload)),0) FROM portal_monthly_tail_page_journal)+length(NEW.payload)>67108864
BEGIN SELECT RAISE(ABORT,'tail page capacity reached'); END`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func validateTailPageJournal(ctx context.Context, db *sql.DB) error {
	var n int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM main.sqlite_master WHERE
(type='table' AND name='portal_monthly_tail_page_journal') OR
(type='trigger' AND name IN ('monthly_tail_page_no_update','monthly_tail_page_no_delete','monthly_tail_page_append','monthly_tail_page_capacity'))`).Scan(&n)
	if err != nil || n != 5 {
		return errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	return nil
}

// Validate transport shape when reloading a checkpoint. A digest or this
// structural check never upgrades separate page views into a wallet proof.
func walletTailPageBytes(o WalletTailPageObservation) ([]byte, error) {
	if o.Version != 1 || o.Basis != walletTailPageBasis || !validTailPageRequest(o.Request, o.StartedAtMS) ||
		o.CompletedAtMS < o.StartedAtMS || o.CompletedAtMS-o.StartedAtMS > usageWindowTimeout.Milliseconds() ||
		walletTailMonthStart(o.CompletedAtMS) != o.Request.Wallet.Boundary || o.User.UserID != o.Request.Wallet.UserID ||
		o.User.RegisteredAt <= 0 || o.User.RegisteredAt > o.CompletedAtMS/1000 || o.User.BalanceQuota == nil || o.User.UsedQuota == nil || *o.User.UsedQuota < 0 ||
		o.Index.Records < 0 || o.Index.Records > walletTailPageRows || !validDigest(o.Index.SHA256) || o.Index.Kinds == nil || len(o.Index.Kinds) > 128 ||
		o.Usage == nil || len(o.Usage) > usageWindowRowsLimit || o.Funding == nil || len(o.Funding) > FundingPageLimit || o.FundingKeys == nil || len(o.FundingKeys) > FundingPageLimit || o.OtherUserFundingRecords < 0 {
		return nil, ErrWalletMonthCaptureIntegrity
	}
	counts, total := map[int64]int64{}, 0
	for i, k := range o.Index.Kinds {
		if k.Type < 0 || k.Records <= 0 || k.Records > walletTailPageRows || i > 0 && o.Index.Kinds[i-1].Type >= k.Type {
			return nil, ErrWalletMonthCaptureIntegrity
		}
		counts[k.Type], total = int64(k.Records), total+k.Records
	}
	if total != o.Index.Records || o.Index.Records == 0 && (!o.ViewEndReached || o.Index.First != (WalletTailPageCursor{}) || o.Index.Next != o.Request.After) ||
		o.Index.Records > 0 && (o.Index.First.ID <= 0 || o.Index.First.Type < 0 || o.Index.First.At < o.Request.Wallet.Boundary || !tailPageCursorBefore(o.Request.After, o.Index.First) ||
			o.Index.Next.ID <= 0 || o.Index.Next.Type < 0 || tailPageCursorBefore(o.Index.Next, o.Index.First) || o.Index.Next.At > o.CompletedAtMS/1000) {
		return nil, ErrWalletMonthCaptureIntegrity
	}
	if o.Request.Kind == "user_usage" {
		if len(o.Funding) != 0 || len(o.FundingKeys) != 0 || o.OtherUserFundingRecords != 0 {
			return nil, ErrWalletMonthCaptureIntegrity
		}
		r := UsageWindowRequest{UserID: o.Request.Wallet.UserID, From: o.Request.Wallet.Boundary, To: math.MaxInt64}
		for i, row := range o.Usage {
			if !validUsageWindowRow(row, r) || !validUsageWindowAmount(row) || row.Records > counts[int64(row.Type)] || row.LastAt > o.CompletedAtMS/1000 ||
				row.FirstAt < o.User.RegisteredAt || row.FirstAt < o.Index.First.At || row.LastAt > o.Index.Next.At || i > 0 && !usageRowBefore(o.Usage[i-1], row) {
				return nil, ErrWalletMonthCaptureIntegrity
			}
			counts[int64(row.Type)] -= row.Records
		}
		if counts[2] != 0 || counts[6] != 0 {
			return nil, ErrWalletMonthCaptureIntegrity
		}
	} else {
		if len(o.Usage) != 0 || len(o.Funding)+o.OtherUserFundingRecords != len(o.FundingKeys) || int64(len(o.FundingKeys)) != counts[1]+counts[3]+counts[4] {
			return nil, ErrWalletMonthCaptureIntegrity
		}
		keys, after := map[int64]WalletTailPageCursor{}, o.Request.After
		for _, k := range o.FundingKeys {
			if k.ID <= 0 || !tailPageRelevant("global_funding", k.Type) || keys[k.ID].ID != 0 || !tailPageCursorBefore(after, k) ||
				tailPageCursorBefore(k, o.Index.First) || tailPageCursorBefore(o.Index.Next, k) || counts[k.Type] <= 0 {
				return nil, ErrWalletMonthCaptureIntegrity
			}
			keys[k.ID], after = k, k
			counts[k.Type]--
		}
		scan := FundingScan{UserIDs: []int64{o.Request.Wallet.UserID}, From: o.Request.Wallet.Boundary, To: math.MaxInt64}
		cursor := FundingCursor{}
		for _, row := range o.Funding {
			k, exists := keys[row.ID]
			if !exists || k.At != row.CreatedAt || k.Type != int64(row.Type) || !validFundingRow(row, scan, math.MaxInt64, cursor) {
				return nil, ErrWalletMonthCaptureIntegrity
			}
			delete(keys, row.ID)
			cursor = FundingCursor{At: row.CreatedAt, ID: row.ID}
		}
		for _, k := range keys {
			if k.Type == 3 {
				return nil, ErrWalletMonthCaptureIntegrity
			}
		}
	}
	raw, err := json.Marshal(o)
	if err != nil || len(raw) > walletTailBytesLimit {
		return nil, errors.Join(ErrWalletTailCaptureCapacity, err)
	}
	return raw, nil
}

type tailPageJournalReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Only first/last payloads are decoded per resume, not every previous page
// on every step. Full retained-original audit remains a separate requirement.
func loadTailPageJournal(ctx context.Context, db tailPageJournalReader, s walletTailPageSeries, now time.Time) (WalletTailPageJournalProgress, error) {
	var out WalletTailPageJournalProgress
	var low, high int
	var first int64
	if err := db.QueryRowContext(ctx, `SELECT count(*),coalesce(min(seq),0),coalesce(max(seq),0),coalesce(min(started_at_ms),0) FROM portal_monthly_tail_page_journal WHERE series=?`, s.id()).Scan(&out.Pages, &low, &high, &first); err != nil {
		return out, err
	}
	if out.Pages == 0 {
		return out, nil
	}
	if low != 0 || high != out.Pages-1 || first <= 0 || first > now.UnixMilli() {
		return WalletTailPageJournalProgress{}, ErrWalletMonthCaptureIntegrity
	}
	if out.Pages > walletTailJournalPages || now.UnixMilli()-first > (24*time.Hour).Milliseconds() {
		return WalletTailPageJournalProgress{}, ErrWalletTailCaptureCapacity
	}
	var last WalletTailPageObservation
	var registeredAt int64
	for _, seq := range []int{0, high} {
		var id, previous, parent string
		var started, completed int64
		var raw []byte
		err := db.QueryRowContext(ctx, `SELECT p.id,p.previous_id,coalesce(q.id,''),p.started_at_ms,p.completed_at_ms,
CASE WHEN typeof(p.payload)='blob' AND length(p.payload)<=2097152 THEN p.payload END
FROM portal_monthly_tail_page_journal p LEFT JOIN portal_monthly_tail_page_journal q ON q.series=p.series AND q.seq=p.seq-1 WHERE p.series=? AND p.seq=?`, s.id(), seq).Scan(&id, &previous, &parent, &started, &completed, &raw)
		if err != nil || !validDigest(id) || digest(raw) != id || previous != parent || (seq == 0 && previous != "") || (seq > 0 && !validDigest(previous)) || completed > now.UnixMilli() || json.Unmarshal(raw, &last) != nil {
			return WalletTailPageJournalProgress{}, errors.Join(ErrWalletMonthCaptureIntegrity, err)
		}
		canonical, err := walletTailPageBytes(last)
		r := last.Request
		r.After = WalletTailPageCursor{}
		if err != nil || !bytes.Equal(raw, canonical) || r != s.Request || started != last.StartedAtMS || completed != last.CompletedAtMS || seq == 0 && last.Request.After != (WalletTailPageCursor{}) {
			return WalletTailPageJournalProgress{}, errors.Join(ErrWalletMonthCaptureIntegrity, err)
		}
		if registeredAt != 0 && registeredAt != last.User.RegisteredAt {
			return WalletTailPageJournalProgress{}, ErrWalletMonthCaptureIntegrity
		}
		registeredAt = last.User.RegisteredAt
		out.LastSHA = id
	}
	out.After, out.Traversed = last.Index.Next, last.ViewEndReached
	out.LastCompletedMS = last.CompletedAtMS
	out.RegisteredAt = registeredAt
	return out, nil
}

func tailPageJournalCapacity(ctx context.Context, db tailPageJournalReader, added int) error {
	var count int
	var size int64
	if err := db.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(length(payload)),0) FROM portal_monthly_tail_page_journal`).Scan(&count, &size); err != nil {
		return err
	}
	if count >= walletTailJournalPages || size < 0 || size+int64(added) > walletTailStoreBytesLimit {
		return ErrWalletTailCaptureCapacity
	}
	return nil
}

func saveTailPageJournal(ctx context.Context, db *sql.DB, s walletTailPageSeries, before WalletTailPageJournalProgress, o WalletTailPageObservation, now time.Time) error {
	raw, err := walletTailPageBytes(o)
	r := o.Request
	r.After = WalletTailPageCursor{}
	if err != nil || before.Traversed || o.Request.After != before.After || r != s.Request || o.CompletedAtMS > now.UnixMilli() || before.Pages > 0 && before.RegisteredAt != o.User.RegisteredAt {
		return errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// Acquire the SQLite writer before rechecking the chain/CAS and budgets.
	if _, err := tx.ExecContext(ctx, `UPDATE portal_monthly_tail_page_journal SET seq=seq WHERE 0`); err != nil {
		return err
	}
	current, err := loadTailPageJournal(ctx, tx, s, now)
	if err != nil || current != before {
		return errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	if err := tailPageJournalCapacity(ctx, tx, len(raw)); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO portal_monthly_tail_page_journal(series,seq,id,previous_id,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?,?,?)`, s.id(), before.Pages, digest(raw), before.LastSHA, o.StartedAtMS, o.CompletedAtMS, raw)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// One explicit step; shares the existing wallet month lane, never retries or
// starts a worker. Current authority must still be checked by the caller.
// Not yet installed: whole-spool accounting and monetary re-audit are required.
func CaptureNextWalletTailPage(parent context.Context, source, receiver *sql.DB, key string, scope OrganizationMonthScope, userID int64, kind string, pass int) (WalletTailPageJournalProgress, error) {
	return captureNextWalletTailPage(parent, source, receiver, key, scope, userID, kind, pass, time.Now)
}

func captureNextWalletTailPage(parent context.Context, source, receiver *sql.DB, key string, scope OrganizationMonthScope, userID int64, kind string, pass int, clock func() time.Time) (WalletTailPageJournalProgress, error) {
	var empty WalletTailPageJournalProgress
	if parent == nil || source == nil || receiver == nil || source == receiver || clock == nil || pass < 0 || pass > 1 || !fundingScopeHas(scope.Funding.UserIDs, userID) {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	select {
	case walletTailMonthSlots <- struct{}{}:
		defer func() { <-walletTailMonthSlots }()
	default:
		return empty, ErrWalletCaptureBusy
	}
	ctx, cancel := context.WithTimeout(parent, 12*time.Second)
	defer cancel()
	readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
	defer readCancel()
	now := clock()
	if err := validateTailPageJournal(readCtx, receiver); err != nil {
		return empty, err
	}
	plan, err := loadFundingMonthCapturePlan(readCtx, receiver, key, scope, now)
	if err != nil {
		return empty, err
	}
	s := walletTailPageSeries{JobKey: key, PlanSHA: registeredCapturePlanSHA(plan), ScopeSHA: registeredCaptureScopeSHA(scope), Pass: pass,
		Request: WalletTailPageRequest{Wallet: WalletTailRequest{SourceIdentity: scope.Funding.SourceIdentity, Rules: scope.Funding.Rules, UserID: userID, Boundary: walletTailMonthStart(now.UnixMilli())}, Kind: kind}}
	if !validTailPageRequest(s.Request, now.UnixMilli()) {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	before, err := loadTailPageJournal(readCtx, receiver, s, now)
	if err != nil || before.Traversed {
		return before, err
	}
	if pass == 1 {
		first := s
		first.Pass = 0
		prior, err := loadTailPageJournal(readCtx, receiver, first, now)
		if err != nil || !prior.Traversed {
			return empty, errors.Join(ErrWalletMonthCaptureIntegrity, err)
		}
		if now.UnixMilli()-prior.LastCompletedMS < (5 * time.Minute).Milliseconds() {
			return empty, ErrWalletCaptureBusy
		}
	}
	if err := tailPageJournalCapacity(readCtx, receiver, 1); err != nil {
		return empty, err
	}
	readCancel()
	if err := waitFundingInterval(ctx, time.Second); err != nil {
		return empty, err
	}
	request := s.Request
	request.After = before.After
	o, err := captureWalletTailPage(ctx, source, request, clock)
	if err != nil {
		return empty, err
	}
	writeCtx, writeCancel := context.WithTimeout(ctx, 2*time.Second)
	defer writeCancel()
	if err := saveTailPageJournal(writeCtx, receiver, s, before, o, clock()); err != nil {
		return empty, err
	}
	after, err := loadTailPageJournal(writeCtx, receiver, s, clock())
	if err != nil {
		return empty, err
	}
	after.Captured = true
	return after, nil
}
