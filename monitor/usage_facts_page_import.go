package monitor

// This file contains the bounded raw-event import primitive used by the next
// generation of the facts worker.  It deliberately does not call the source
// database itself: production wiring must first prove the source query plan.
// Keeping the page contract small makes that plan replaceable without changing
// the exactly-once local commit protocol.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const usageFactRawPageSize = 1000
const usageFactRawPagesPerTurn = 3
const usageFactRawShardDefaultHours = 24
const usageFactRawShardWideHours = 6
const usageFactRawShardMediumHours = 3
const usageFactRawPageLegacyDayProofError = "日终 24 小时当前来源 epoch 证明尚未完整"

var (
	errUsageFactRawPageOrder   = errors.New("原始日志分页游标顺序错误")
	errUsageFactRawPageControl = errors.New("原始日志分页控制数不一致")
	errUsageFactRawShardDense  = errors.New("原始日志分片密度过高，需要缩小时间范围")
)

// usageFactRawEvent is the minimal non-sensitive projection needed to build a
// usage fact.  Source readers must return only type 2/6 events ordered by the
// stable (created_at, id) tuple, and never aggregate them in MySQL.
type usageFactRawEvent struct {
	ID               int64
	CreatedAt        int64
	UserID           int64
	ChannelID        int64
	Grp              string
	ModelName        string
	TokenID          int64
	TokenName        string
	Type             int
	PromptTokens     int64
	CompletionTokens int64
	Quota            int64
}

// usageFactRawPageSource exposes exactly one bounded source operation. A shard
// is accepted only after two independent cursor passes produce identical
// counts and semantic hashes. Keeping unbounded COUNT/SUM out of this interface
// makes it impossible for a high-volume member to reintroduce the old source
// query bottleneck through a future caller.
type usageFactRawPageSource interface {
	FetchUsageFactRawPage(ctx context.Context, userID, fromTs, throughTs, afterCreatedAt int64, afterType int, afterID int64, limit int) ([]usageFactRawEvent, error)
}

// usageFactMonitorRawPageSource binds the generic import protocol to Monitor's
// read-only source connection.  Keeping this adapter separate means the
// scheduler can be tested with an in-memory source while production gets the
// exact same cursor and commit semantics.
type usageFactMonitorRawPageSource struct {
	m           *Monitor
	lowPriority bool
}

// usageFactPrefetchedRawPageSource reuses the no-history activity probe as the
// importer's first page. Empty no-history hours therefore reuse the activity
// probe and then need only the independent bounded verification pass.
type usageFactPrefetchedRawPageSource struct {
	base      usageFactRawPageSource
	userID    int64
	fromTs    int64
	throughTs int64
	page      []usageFactRawEvent
	consumed  bool
}

func (s *usageFactPrefetchedRawPageSource) FetchUsageFactRawPage(ctx context.Context, userID, fromTs, throughTs, afterCreatedAt int64, afterType int, afterID int64, limit int) ([]usageFactRawEvent, error) {
	if !s.consumed {
		if userID != s.userID || fromTs != s.fromTs || throughTs != s.throughTs ||
			afterCreatedAt != 0 || afterType != 0 || afterID != 0 || limit != usageFactRawPageSize {
			return nil, errors.New("无历史分页预取游标不一致")
		}
		s.consumed = true
		return append([]usageFactRawEvent(nil), s.page...), nil
	}
	return s.base.FetchUsageFactRawPage(ctx, userID, fromTs, throughTs, afterCreatedAt, afterType, afterID, limit)
}

func (s usageFactMonitorRawPageSource) FetchUsageFactRawPage(ctx context.Context, userID, fromTs, throughTs, afterCreatedAt int64, afterType int, afterID int64, limit int) ([]usageFactRawEvent, error) {
	if s.m == nil {
		return nil, errors.New("原始日志分页来源未初始化")
	}
	return s.m.fetchUsageFactRawPage(ctx, userID, fromTs, throughTs, afterCreatedAt, afterType, afterID, limit, s.lowPriority)
}

func (m *Monitor) usageFactRawPageSource(lowPriority bool) usageFactRawPageSource {
	return usageFactMonitorRawPageSource{m: m, lowPriority: lowPriority}
}

func (m *Monitor) usageFactsRawPageImportEnabled() bool {
	return m != nil && m.usageFactsFullHistoryEnabled() && m.cfg.UsageFactsRawPageImportEnabled
}

// fetchUsageFactRawPage is intentionally a plain index-range SELECT.  Source
// MySQL never groups this workload: group/model/token aggregation happens only
// after the bounded page reaches SQLite.  The cursor order follows the source
// index (user_id, created_at, type) plus InnoDB's primary-key suffix (id).
func (m *Monitor) fetchUsageFactRawPage(
	ctx context.Context,
	userID, fromTs, throughTs, afterCreatedAt int64,
	afterType int,
	afterID int64,
	limit int,
	lowPriority bool,
) ([]usageFactRawEvent, error) {
	if m == nil || m.prodDB == nil || userID <= 0 || fromTs <= 0 || throughTs <= fromTs || limit < 1 || limit > usageFactRawPageSize {
		return nil, errors.New("原始日志分页读取参数无效")
	}
	hint := "/*+ MAX_EXECUTION_TIME(5000) */ "
	fromClause := "logs FORCE INDEX (idx_user_created_type)"
	if m.usageDayExpr != "" { // local SQLite fake source
		hint = ""
		fromClause = "logs"
	}
	query := `SELECT ` + hint + `
  id, created_at, type, COALESCE(user_id,0), COALESCE(channel_id,0),
  COALESCE(` + "`group`" + `,''), COALESCE(model_name,''), COALESCE(token_id,0),
  COALESCE(token_name,''), COALESCE(prompt_tokens,0), COALESCE(completion_tokens,0), COALESCE(quota,0)
 FROM ` + fromClause + `
 WHERE user_id = ? AND created_at >= ? AND created_at < ? AND type IN (2,6)
   AND NOT (` + m.channelTestSourcePredicateSQL() + `)
   AND (created_at > ?
        OR (created_at = ? AND type > ?)
        OR (created_at = ? AND type = ? AND id > ?))
 ORDER BY created_at, type, id
 LIMIT ?`
	args := []any{userID, fromTs, throughTs, afterCreatedAt, afterCreatedAt, afterType, afterCreatedAt, afterType, afterID, limit}
	out := make([]usageFactRawEvent, 0, limit)
	run := m.withUsageFactSourceQuery
	if lowPriority {
		run = m.withUsageFactHistorySourceQuery
	}
	err := run(ctx, func(qctx context.Context) error {
		rows, err := m.prodDB.QueryContext(qctx, query, args...)
		if err != nil {
			return fmt.Errorf("读取原始用量日志分页失败: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var event usageFactRawEvent
			if err := rows.Scan(&event.ID, &event.CreatedAt, &event.Type, &event.UserID, &event.ChannelID,
				&event.Grp, &event.ModelName, &event.TokenID, &event.TokenName,
				&event.PromptTokens, &event.CompletionTokens, &event.Quota); err != nil {
				return err
			}
			out = append(out, event)
			if len(out) > limit {
				return errUsageFactRawPageOrder
			}
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func usageFactRawEventAfter(a, b usageFactRawEvent) bool {
	return a.CreatedAt > b.CreatedAt ||
		(a.CreatedAt == b.CreatedAt && (a.Type > b.Type || (a.Type == b.Type && a.ID > b.ID)))
}

func usageFactRawEventMetrics(events []usageFactRawEvent) usageFactMetrics {
	var out usageFactMetrics
	for _, event := range events {
		out.Rows++
		switch event.Type {
		case 2:
			out.Requests++
			out.PromptTokens += event.PromptTokens
			out.CompletionTokens += event.CompletionTokens
			out.ConsumeQuota += event.Quota
		case 6:
			out.RefundRecords++
			out.RefundQuota += event.Quota
		}
	}
	return out
}

// usageFactRawRollingHash is restartable: every committed page folds the
// previous digest and every source field that affects fact semantics into the
// next digest. A second bounded pass must produce the same final digest before
// publication. This replaces the old unbounded COUNT/SUM control query.
func usageFactRawRollingHash(previous string, events []usageFactRawEvent) string {
	h := sha256.New()
	writeString := func(value string) {
		_ = binary.Write(h, binary.LittleEndian, uint64(len(value)))
		_, _ = h.Write([]byte(value))
	}
	writeString(previous)
	for _, event := range events {
		for _, value := range []int64{
			event.ID, event.CreatedAt, event.UserID, event.ChannelID, event.TokenID,
			int64(event.Type), event.PromptTokens, event.CompletionTokens, event.Quota,
		} {
			_ = binary.Write(h, binary.LittleEndian, value)
		}
		writeString(event.Grp)
		writeString(event.ModelName)
		writeString(event.TokenName)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// usageFactRawRecordCount is the source-event cardinality represented by an
// aggregated fact set. factsMetrics.Rows is the number of dimension rows and
// is intentionally unrelated: millions of events may collapse into one
// channel/group/model/token row.
func usageFactRawRecordCount(metrics usageFactMetrics) int64 {
	return metrics.Requests + metrics.RefundRecords
}

func usageFactRawEventsToRangeFacts(fromTs, throughTs int64, userID int64, events []usageFactRawEvent) ([]UsageHourFact, error) {
	type key struct {
		hourTs    int64
		channelID int64
		grp       string
		modelName string
		tokenID   int64
	}
	byDimension := make(map[key]UsageHourFact)
	for _, event := range events {
		if event.UserID != userID || event.CreatedAt < fromTs || event.CreatedAt >= throughTs {
			return nil, fmt.Errorf("%w: event id=%d is outside its member-shard", errUsageFactRawPageOrder, event.ID)
		}
		if event.Type != 2 && event.Type != 6 {
			return nil, fmt.Errorf("%w: event id=%d has unsupported type=%d", errUsageFactRawPageOrder, event.ID, event.Type)
		}
		hourTs := event.CreatedAt - event.CreatedAt%usageFactHourSeconds
		k := key{hourTs, event.ChannelID, event.Grp, event.ModelName, event.TokenID}
		row := byDimension[k]
		if row.UserID == 0 {
			row = UsageHourFact{HourTs: hourTs, DayTs: usageFactDayStart(hourTs), UserID: userID, ChannelID: event.ChannelID, Grp: event.Grp, ModelName: event.ModelName, TokenID: event.TokenID}
		}
		if event.TokenName > row.TokenName { // same deterministic MAX(token_name) semantics as the legacy query
			row.TokenName = event.TokenName
		}
		if event.Type == 2 {
			row.Requests++
			row.PromptTokens += event.PromptTokens
			row.CompletionTokens += event.CompletionTokens
			row.ConsumeQuota += event.Quota
		} else {
			row.RefundRecords++
			row.RefundQuota += event.Quota
		}
		byDimension[k] = row
	}
	out := make([]UsageHourFact, 0, len(byDimension))
	for _, row := range byDimension {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.HourTs != b.HourTs {
			return a.HourTs < b.HourTs
		}
		if a.ChannelID != b.ChannelID {
			return a.ChannelID < b.ChannelID
		}
		if a.Grp != b.Grp {
			return a.Grp < b.Grp
		}
		if a.ModelName != b.ModelName {
			return a.ModelName < b.ModelName
		}
		return a.TokenID < b.TokenID
	})
	return out, nil
}

// upsertUsageFactRawPageFacts merges one bounded page with a small number of
// batched SQLite statements. The previous row-by-row SELECT+UPDATE loop was
// correct but turned a 1000-event page into as many as 2000 local statements,
// which made high-cardinality members needlessly slow even after source reads
// were bounded. SQLite applies this UPSERT inside the same transaction as the
// cursor and rolling hash, preserving exactly-once restart semantics.
func upsertUsageFactRawPageFacts(tx *gorm.DB, facts []UsageHourFact) error {
	if len(facts) == 0 {
		return nil
	}
	return tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "hour_ts"}, {Name: "user_id"}, {Name: "channel_id"},
			{Name: "grp"}, {Name: "model_name"}, {Name: "token_id"},
		},
		DoUpdates: clause.Assignments(map[string]any{
			"token_name":        gorm.Expr("MAX(token_name, excluded.token_name)"),
			"requests":          gorm.Expr("requests + excluded.requests"),
			"refund_records":    gorm.Expr("refund_records + excluded.refund_records"),
			"prompt_tokens":     gorm.Expr("prompt_tokens + excluded.prompt_tokens"),
			"completion_tokens": gorm.Expr("completion_tokens + excluded.completion_tokens"),
			"consume_quota":     gorm.Expr("consume_quota + excluded.consume_quota"),
			"refund_quota":      gorm.Expr("refund_quota + excluded.refund_quota"),
		}),
	}).CreateInBatches(facts, 200).Error
}

func addUsageFactRawMetrics(state *UsageFactPageIngestState, metrics usageFactMetrics) {
	state.SourceRows += metrics.Rows
	state.Requests += metrics.Requests
	state.RefundRecords += metrics.RefundRecords
	state.PromptTokens += metrics.PromptTokens
	state.CompletionTokens += metrics.CompletionTokens
	state.ConsumeQuota += metrics.ConsumeQuota
	state.RefundQuota += metrics.RefundQuota
}

func usageFactRawPageStateThrough(state UsageFactPageIngestState) int64 {
	if state.ThroughTs > state.HourTs {
		return state.ThroughTs
	}
	return state.HourTs + usageFactHourSeconds
}

func loadUsageFactRange(db *gorm.DB, fromTs, throughTs, userID int64) ([]UsageHourFact, error) {
	var rows []UsageHourFact
	err := db.Where("hour_ts >= ? AND hour_ts < ? AND user_id = ?", fromTs, throughTs, userID).
		Order("hour_ts, channel_id, grp, model_name, token_id").Find(&rows).Error
	return rows, err
}

// importUsageFactRawPages imports at most maxPages short source reads.  A
// caller schedules the next turn later, so a high-volume member is fair to
// other members and cannot monopolize the source gate. An empty first-pass page
// starts an independent bounded verification pass; only exact equality across
// both passes and the locally aggregated facts can complete the shard.
func importUsageFactRawPages(
	ctx context.Context,
	db *gorm.DB,
	source usageFactRawPageSource,
	userID, hourTs int64,
	sourceEpoch string,
	maxPages int,
) (complete bool, err error) {
	return importUsageFactRawShardPages(ctx, db, source, userID, hourTs, hourTs+usageFactHourSeconds, sourceEpoch, maxPages)
}

// importUsageFactRawShardPages is the common bounded importer. The range may
// cover several complete hours, but every source page is still capped and the
// caller must yield after maxPages. A dense shard asks the scheduler to shrink
// before the first page is committed, so no staged rows need to be rolled back.
func importUsageFactRawShardPages(
	ctx context.Context,
	db *gorm.DB,
	source usageFactRawPageSource,
	userID, fromTs, throughTs int64,
	sourceEpoch string,
	maxPages int,
) (complete bool, err error) {
	if db == nil || source == nil || userID <= 0 || fromTs <= 0 || fromTs%usageFactHourSeconds != 0 ||
		throughTs <= fromTs || throughTs%usageFactHourSeconds != 0 || maxPages <= 0 {
		return false, errors.New("原始日志分页导入参数无效")
	}
	if throughTs-fromTs > usageFactRawShardDefaultHours*usageFactHourSeconds {
		return false, errors.New("原始日志分页分片超过最大范围")
	}
	if sourceEpoch == "" {
		return false, errors.New("原始日志分页导入缺少来源 epoch")
	}
	var initial UsageFactPageIngestState
	err = db.Transaction(func(tx *gorm.DB) error {
		lookupErr := tx.First(&initial, "user_id = ? AND hour_ts = ?", userID, fromTs).Error
		if errors.Is(lookupErr, gorm.ErrRecordNotFound) {
			// This is a narrowly scoped replacement, not a history reset.  A
			// page cursor is only created for the exact member-hour it is about
			// to re-prove.  Keeping stale staged rows would make a resumed v5
			// importer add onto an old GROUP BY result and silently double-count.
			if err := tx.Where("hour_ts >= ? AND hour_ts < ? AND user_id = ?", fromTs, throughTs, userID).Delete(&UsageHourFact{}).Error; err != nil {
				return err
			}
			if err := tx.Where("hour_ts >= ? AND hour_ts < ? AND user_id = ?", fromTs, throughTs, userID).Delete(&UsageFactMemberHourState{}).Error; err != nil {
				return err
			}
			initial = UsageFactPageIngestState{UserID: userID, HourTs: fromTs, ThroughTs: throughTs, SourceEpoch: sourceEpoch, Status: "running", UpdatedAt: time.Now().Unix()}
			return tx.Create(&initial).Error
		}
		return lookupErr
	})
	if err != nil {
		return false, err
	}
	if initial.SourceEpoch != sourceEpoch {
		return false, fmt.Errorf("原始日志分页导入来源 epoch 已变化: have=%s want=%s", initial.SourceEpoch, sourceEpoch)
	}
	if usageFactRawPageStateThrough(initial) != throughTs {
		return false, fmt.Errorf("原始日志分页分片范围已变化: have=[%d,%d) want=[%d,%d)", initial.HourTs, usageFactRawPageStateThrough(initial), fromTs, throughTs)
	}
	if initial.Status == "complete" {
		// A completed cursor is trustworthy only together with the rows it
		// fingerprints. Repair cleanup intentionally removes hour staging after
		// a signed daily replacement; a later repair of that same hour must not
		// mistake the old cursor for still-present local facts.
		rows, loadErr := loadUsageFactRange(db, fromTs, throughTs, userID)
		if loadErr != nil {
			return false, loadErr
		}
		metrics := factsMetrics(rows)
		if usageFactRawRecordCount(metrics) == initial.SourceRows && metrics.Requests == initial.Requests &&
			metrics.RefundRecords == initial.RefundRecords && metrics.PromptTokens == initial.PromptTokens &&
			metrics.CompletionTokens == initial.CompletionTokens && metrics.ConsumeQuota == initial.ConsumeQuota &&
			metrics.RefundQuota == initial.RefundQuota && usageFactContentHash(rows) == initial.ContentHash {
			return true, nil
		}
		if err := db.Model(&UsageFactPageIngestState{}).Where("user_id = ? AND hour_ts = ? AND status = ?", userID, fromTs, "complete").Updates(map[string]any{
			"status": "repair", "last_error": "completed page proof no longer matches local facts", "updated_at": time.Now().Unix(),
		}).Error; err != nil {
			return false, err
		}
		initial.Status = "repair"
	}
	if initial.Status == "repair" {
		// A two-pass/source-to-local mismatch is an integrity hold, not a terminal state.
		// Start that one hour over from its immutable source window; pages and
		// cursor are reset with its local staging rows in one transaction.
		if err := db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Where("hour_ts >= ? AND hour_ts < ? AND user_id = ?", fromTs, throughTs, userID).Delete(&UsageHourFact{}).Error; err != nil {
				return err
			}
			if err := tx.Where("hour_ts >= ? AND hour_ts < ? AND user_id = ?", fromTs, throughTs, userID).Delete(&UsageFactMemberHourState{}).Error; err != nil {
				return err
			}
			return tx.Model(&UsageFactPageIngestState{}).Where("user_id = ? AND hour_ts = ? AND status = ?", userID, fromTs, "repair").Updates(map[string]any{
				"cursor_created_at": 0, "cursor_type": 0, "cursor_id": 0, "pages": 0,
				"source_rows": 0, "requests": 0, "refund_records": 0, "prompt_tokens": 0,
				"completion_tokens": 0, "consume_quota": 0, "refund_quota": 0,
				"raw_hash": "", "verify_cursor_created_at": 0, "verify_cursor_type": 0, "verify_cursor_id": 0,
				"verify_pages": 0, "verify_source_rows": 0, "verify_requests": 0,
				"verify_refund_records": 0, "verify_prompt_tokens": 0, "verify_completion_tokens": 0,
				"verify_consume_quota": 0, "verify_refund_quota": 0, "verify_raw_hash": "",
				"content_hash": "", "last_error": "", "status": "running", "completed_at": 0, "updated_at": time.Now().Unix(),
			}).Error
		}); err != nil {
			return false, err
		}
	}
	var phase UsageFactPageIngestState
	if err := db.First(&phase, "user_id = ? AND hour_ts = ?", userID, fromTs).Error; err != nil {
		return false, err
	}
	if phase.Status == "verifying" {
		return verifyUsageFactRawShardPages(ctx, db, source, phase, maxPages)
	}

	for pageNo := 0; pageNo < maxPages; pageNo++ {
		var state UsageFactPageIngestState
		if err := db.First(&state, "user_id = ? AND hour_ts = ?", userID, fromTs).Error; err != nil {
			return false, err
		}
		if state.Status == "complete" {
			return true, nil
		}
		page, fetchErr := source.FetchUsageFactRawPage(ctx, userID, fromTs, throughTs, state.CursorCreatedAt, state.CursorType, state.CursorID, usageFactRawPageSize)
		if fetchErr != nil {
			_ = db.Model(&UsageFactPageIngestState{}).Where("user_id = ? AND hour_ts = ?", userID, fromTs).
				Updates(map[string]any{"last_error": truncateUsageFactError(fetchErr.Error()), "updated_at": time.Now().Unix()}).Error
			return false, fetchErr
		}
		if len(page) > usageFactRawPageSize {
			return false, fmt.Errorf("%w: source returned %d rows over page limit", errUsageFactRawPageOrder, len(page))
		}
		if len(page) == 0 {
			if err := db.Model(&UsageFactPageIngestState{}).
				Where("user_id = ? AND hour_ts = ? AND source_epoch = ?", userID, fromTs, sourceEpoch).
				Updates(map[string]any{
					"status": "verifying", "verify_cursor_created_at": 0, "verify_cursor_type": 0,
					"verify_cursor_id": 0, "verify_pages": 0, "verify_source_rows": 0,
					"verify_requests": 0, "verify_refund_records": 0, "verify_prompt_tokens": 0,
					"verify_completion_tokens": 0, "verify_consume_quota": 0, "verify_refund_quota": 0,
					"verify_raw_hash": "", "last_error": "", "updated_at": time.Now().Unix(),
				}).Error; err != nil {
				return false, err
			}
			// Yield between passes. The scheduler gives every other member a turn
			// before this shard performs its independent bounded verification.
			return false, nil
		}
		if state.Pages == 0 && state.CursorCreatedAt == 0 && state.CursorType == 0 && state.CursorID == 0 &&
			throughTs-fromTs > usageFactHourSeconds && len(page) == usageFactRawPageSize {
			if deleteErr := db.Where("user_id = ? AND hour_ts = ? AND pages = 0 AND cursor_id = 0", userID, fromTs).
				Delete(&UsageFactPageIngestState{}).Error; deleteErr != nil {
				return false, deleteErr
			}
			return false, errUsageFactRawShardDense
		}
		previous := usageFactRawEvent{CreatedAt: state.CursorCreatedAt, Type: state.CursorType, ID: state.CursorID}
		for _, event := range page {
			if !usageFactRawEventAfter(event, previous) {
				return false, fmt.Errorf("%w: page did not advance after (%d,%d,%d)", errUsageFactRawPageOrder, previous.CreatedAt, previous.Type, previous.ID)
			}
			previous = event
		}
		pageFacts, convertErr := usageFactRawEventsToRangeFacts(fromTs, throughTs, userID, page)
		if convertErr != nil {
			return false, convertErr
		}
		metrics := usageFactRawEventMetrics(page)
		last := page[len(page)-1]
		pageProvesEOF := len(page) < usageFactRawPageSize
		if err := db.Transaction(func(tx *gorm.DB) error {
			var current UsageFactPageIngestState
			if err := tx.First(&current, "user_id = ? AND hour_ts = ?", userID, fromTs).Error; err != nil {
				return err
			}
			if current.CursorCreatedAt != state.CursorCreatedAt || current.CursorType != state.CursorType || current.CursorID != state.CursorID || current.Status == "complete" {
				return errors.New("原始日志分页导入游标并发变化")
			}
			if err := upsertUsageFactRawPageFacts(tx, pageFacts); err != nil {
				return fmt.Errorf("批量写入本地分页事实失败 user=%d range=[%d,%d): %w", userID, fromTs, throughTs, err)
			}
			addUsageFactRawMetrics(&current, metrics)
			current.RawHash = usageFactRawRollingHash(current.RawHash, page)
			current.CursorCreatedAt, current.CursorType, current.CursorID = last.CreatedAt, last.Type, last.ID
			current.Pages++
			current.Status, current.LastError, current.UpdatedAt = "running", "", time.Now().Unix()
			if pageProvesEOF {
				// LIMIT returned fewer than the requested rows, which is already a
				// deterministic end-of-range proof. Persist the first-pass result
				// and yield directly to the independent verification pass instead
				// of issuing a redundant empty source query.
				current.Status = "verifying"
				current.VerifyCursorCreatedAt, current.VerifyCursorType, current.VerifyCursorID = 0, 0, 0
				current.VerifyPages, current.VerifySourceRows = 0, 0
				current.VerifyRequests, current.VerifyRefundRecords = 0, 0
				current.VerifyPromptTokens, current.VerifyCompletionTokens = 0, 0
				current.VerifyConsumeQuota, current.VerifyRefundQuota = 0, 0
				current.VerifyRawHash = ""
			}
			return tx.Save(&current).Error
		}); err != nil {
			return false, err
		}
		if pageProvesEOF {
			return false, nil
		}
	}
	return false, nil
}

func verifyUsageFactRawShardPages(
	ctx context.Context,
	db *gorm.DB,
	source usageFactRawPageSource,
	initial UsageFactPageIngestState,
	maxPages int,
) (bool, error) {
	for pageNo := 0; pageNo < maxPages; pageNo++ {
		var state UsageFactPageIngestState
		if err := db.First(&state, "user_id = ? AND hour_ts = ?", initial.UserID, initial.HourTs).Error; err != nil {
			return false, err
		}
		if state.Status == "complete" {
			return true, nil
		}
		if state.Status != "verifying" || state.SourceEpoch != initial.SourceEpoch || usageFactRawPageStateThrough(state) != usageFactRawPageStateThrough(initial) {
			return false, errors.New("原始日志分页复核状态已变化")
		}
		page, err := source.FetchUsageFactRawPage(ctx, state.UserID, state.HourTs, usageFactRawPageStateThrough(state),
			state.VerifyCursorCreatedAt, state.VerifyCursorType, state.VerifyCursorID, usageFactRawPageSize)
		if err != nil {
			_ = db.Model(&UsageFactPageIngestState{}).Where("user_id = ? AND hour_ts = ?", state.UserID, state.HourTs).
				Updates(map[string]any{"last_error": truncateUsageFactError(err.Error()), "updated_at": time.Now().Unix()}).Error
			return false, err
		}
		if len(page) > usageFactRawPageSize {
			return false, fmt.Errorf("%w: verification source returned %d rows over page limit", errUsageFactRawPageOrder, len(page))
		}
		if len(page) == 0 {
			return finalizeUsageFactRawPageVerification(db, state)
		}
		previous := usageFactRawEvent{CreatedAt: state.VerifyCursorCreatedAt, Type: state.VerifyCursorType, ID: state.VerifyCursorID}
		for _, event := range page {
			if !usageFactRawEventAfter(event, previous) {
				return false, fmt.Errorf("%w: verification page did not advance after (%d,%d,%d)", errUsageFactRawPageOrder, previous.CreatedAt, previous.Type, previous.ID)
			}
			previous = event
		}
		metrics := usageFactRawEventMetrics(page)
		last := page[len(page)-1]
		pageProvesEOF := len(page) < usageFactRawPageSize
		if err := db.Transaction(func(tx *gorm.DB) error {
			var current UsageFactPageIngestState
			if err := tx.First(&current, "user_id = ? AND hour_ts = ?", state.UserID, state.HourTs).Error; err != nil {
				return err
			}
			if current.Status != "verifying" || current.VerifyCursorCreatedAt != state.VerifyCursorCreatedAt ||
				current.VerifyCursorType != state.VerifyCursorType || current.VerifyCursorID != state.VerifyCursorID {
				return errors.New("原始日志分页复核游标并发变化")
			}
			current.VerifySourceRows += metrics.Rows
			current.VerifyRequests += metrics.Requests
			current.VerifyRefundRecords += metrics.RefundRecords
			current.VerifyPromptTokens += metrics.PromptTokens
			current.VerifyCompletionTokens += metrics.CompletionTokens
			current.VerifyConsumeQuota += metrics.ConsumeQuota
			current.VerifyRefundQuota += metrics.RefundQuota
			current.VerifyRawHash = usageFactRawRollingHash(current.VerifyRawHash, page)
			current.VerifyCursorCreatedAt, current.VerifyCursorType, current.VerifyCursorID = last.CreatedAt, last.Type, last.ID
			current.VerifyPages++
			current.LastError, current.UpdatedAt = "", time.Now().Unix()
			return tx.Save(&current).Error
		}); err != nil {
			return false, err
		}
		if pageProvesEOF {
			// As in the first pass, a short page proves EOF. Finalize against
			// the just-committed verification cursor without another empty read.
			return finalizeUsageFactRawPageVerification(db, state)
		}
	}
	return false, nil
}

func finalizeUsageFactRawPageVerification(db *gorm.DB, observed UsageFactPageIngestState) (bool, error) {
	var complete bool
	err := db.Transaction(func(tx *gorm.DB) error {
		var state UsageFactPageIngestState
		if err := tx.First(&state, "user_id = ? AND hour_ts = ?", observed.UserID, observed.HourTs).Error; err != nil {
			return err
		}
		hashMatches := (state.SourceRows == 0 && state.VerifySourceRows == 0) ||
			(state.RawHash != "" && state.RawHash == state.VerifyRawHash)
		matches := state.Status == "verifying" && state.SourceEpoch == observed.SourceEpoch &&
			state.SourceRows == state.VerifySourceRows && state.Requests == state.VerifyRequests &&
			state.RefundRecords == state.VerifyRefundRecords && state.PromptTokens == state.VerifyPromptTokens &&
			state.CompletionTokens == state.VerifyCompletionTokens && state.ConsumeQuota == state.VerifyConsumeQuota &&
			state.RefundQuota == state.VerifyRefundQuota && hashMatches
		if !matches {
			state.Status = "repair"
			state.LastError = errUsageFactRawPageControl.Error()
			state.UpdatedAt = time.Now().Unix()
			return tx.Save(&state).Error
		}
		rows, err := loadUsageFactRange(tx, state.HourTs, usageFactRawPageStateThrough(state), state.UserID)
		if err != nil {
			return err
		}
		metrics := factsMetrics(rows)
		if usageFactRawRecordCount(metrics) != state.SourceRows || metrics.Requests != state.Requests ||
			metrics.RefundRecords != state.RefundRecords || metrics.PromptTokens != state.PromptTokens ||
			metrics.CompletionTokens != state.CompletionTokens || metrics.ConsumeQuota != state.ConsumeQuota ||
			metrics.RefundQuota != state.RefundQuota {
			state.Status = "repair"
			state.LastError = errUsageFactRawPageControl.Error()
			state.UpdatedAt = time.Now().Unix()
			return tx.Save(&state).Error
		}
		state.Status = "complete"
		state.ContentHash = usageFactContentHash(rows)
		state.LastError = ""
		state.UpdatedAt, state.CompletedAt = time.Now().Unix(), time.Now().Unix()
		if err := tx.Save(&state).Error; err != nil {
			return err
		}
		complete = true
		return nil
	})
	return complete, err
}

// commitUsageFactRawPageHourProof turns a completed page checkpoint into the
// normal member-hour proof consumed by the existing day finalizer.  The raw
// checkpoint is already source-controlled; this transaction only accepts it
// when its persisted metrics and hash still match the actual local rows.
func (m *Monitor) commitUsageFactRawPageHourProof(ctx context.Context, userID, hourTs int64, revision int64) error {
	return m.commitUsageFactRawPageShardProof(ctx, userID, hourTs, hourTs+usageFactHourSeconds, revision)
}

func adaptiveUsageFactRawSpanHours(current int, sourceRows, pages int64) int {
	if current != usageFactRawShardMediumHours && current != usageFactRawShardWideHours && current != usageFactRawShardDefaultHours {
		current = 1
	}
	if sourceRows >= usageFactRawPageSize || pages >= usageFactRawPagesPerTurn {
		return 1
	}
	if sourceRows < usageFactRawPageSize/4 && pages <= 1 {
		if current <= 1 {
			return usageFactRawShardMediumHours
		}
		if current <= usageFactRawShardMediumHours {
			return usageFactRawShardWideHours
		}
		return usageFactRawShardDefaultHours
	}
	return current
}

// commitUsageFactRawPageShardProof converts one source-controlled shard into
// the existing per-hour proof contract. The in-progress wide cursor is
// expanded into one completed page proof per hour, preserving compatibility
// with day finalization, local audits and restart realignment.
func (m *Monitor) commitUsageFactRawPageShardProof(ctx context.Context, userID, fromTs, throughTs int64, revision int64) error {
	if m == nil || userID <= 0 || revision < 1 {
		return errors.New("原始日志分页小时证明参数无效")
	}
	if fromTs <= 0 || throughTs <= fromTs || fromTs%usageFactHourSeconds != 0 || throughTs%usageFactHourSeconds != 0 ||
		throughTs-fromTs > usageFactRawShardDefaultHours*usageFactHourSeconds {
		return errors.New("原始日志分页分片证明参数无效")
	}
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	if epoch == "" {
		return errors.New("原始日志分页小时证明缺少来源 epoch")
	}
	nowUnix := time.Now().Unix()
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	return m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var page UsageFactPageIngestState
		if err := tx.First(&page, "user_id = ? AND hour_ts = ?", userID, fromTs).Error; err != nil {
			return err
		}
		if page.SourceEpoch != epoch || usageFactRawPageStateThrough(page) != throughTs || page.Status != "complete" || page.ContentHash == "" {
			return errors.New("原始日志分页小时尚未完成独立控制")
		}
		rows, err := loadUsageFactRange(tx, fromTs, throughTs, userID)
		if err != nil {
			return err
		}
		metrics := factsMetrics(rows)
		if usageFactRawRecordCount(metrics) != page.SourceRows || metrics.Requests != page.Requests ||
			metrics.RefundRecords != page.RefundRecords || metrics.PromptTokens != page.PromptTokens ||
			metrics.CompletionTokens != page.CompletionTokens || metrics.ConsumeQuota != page.ConsumeQuota ||
			metrics.RefundQuota != page.RefundQuota || usageFactContentHash(rows) != page.ContentHash {
			return errors.New("原始日志分页小时本地事实证明不一致")
		}
		byHour := make(map[int64][]UsageHourFact, int((throughTs-fromTs)/usageFactHourSeconds))
		for _, row := range rows {
			byHour[row.HourTs] = append(byHour[row.HourTs], row)
		}
		if err := tx.Where("user_id = ? AND hour_ts = ?", userID, fromTs).Delete(&UsageFactPageIngestState{}).Error; err != nil {
			return err
		}
		for hour := fromTs; hour < throughTs; hour += usageFactHourSeconds {
			hourRows := byHour[hour]
			hourMetrics := factsMetrics(hourRows)
			hourHash := usageFactContentHash(hourRows)
			var previous UsageFactMemberHourState
			lookupErr := tx.First(&previous, "user_id = ? AND hour_ts = ?", userID, hour).Error
			if lookupErr != nil && !errors.Is(lookupErr, gorm.ErrRecordNotFound) {
				return lookupErr
			}
			proof := UsageFactMemberHourState{
				UserID: userID, HourTs: hour, Status: "complete", Rows: int(hourMetrics.Rows),
				Requests: hourMetrics.Requests, Tokens: hourMetrics.tokens(), ContentHash: hourHash,
				SourceEpoch: epoch, UpdatedAt: nowUnix, CompletedAt: nowUnix, Attempts: 1,
			}
			if lookupErr == nil {
				proof.Attempts = previous.Attempts + 1
			}
			if err := tx.Save(&proof).Error; err != nil {
				return err
			}
			hourPage := UsageFactPageIngestState{
				UserID: userID, HourTs: hour, ThroughTs: hour + usageFactHourSeconds,
				SourceEpoch: epoch, Status: "complete", SourceRows: usageFactRawRecordCount(hourMetrics),
				Requests: hourMetrics.Requests, RefundRecords: hourMetrics.RefundRecords,
				PromptTokens: hourMetrics.PromptTokens, CompletionTokens: hourMetrics.CompletionTokens,
				ConsumeQuota: hourMetrics.ConsumeQuota, RefundQuota: hourMetrics.RefundQuota,
				ContentHash: hourHash, UpdatedAt: nowUnix, CompletedAt: nowUnix,
			}
			if hour == fromTs {
				hourPage.Pages = page.Pages
			}
			if err := tx.Save(&hourPage).Error; err != nil {
				return err
			}
			if err := m.saveAggregateUsageHourState(tx, hour, []int64{userID}, nowUnix); err != nil {
				return err
			}
		}
		spanHours := int((throughTs - fromTs) / usageFactHourSeconds)
		nextSpan := adaptiveUsageFactRawSpanHours(spanHours, page.SourceRows, page.Pages)
		result := tx.Model(&UsageFactMemberState{}).
			Where("user_id = ? AND active = ? AND tracked_revision = ?", userID, true, revision).
			Updates(map[string]any{"last_sync_at": nowUnix, "last_failure_at": int64(0), "last_error": "", "raw_page_span_hours": nextSpan, "updated_at": nowUnix})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("%w: raw shard revision changed user_id=%d", errUsageMemberControlIntegrity, userID)
		}
		return nil
	})
}

// usageFactRawPageHourReady verifies the whole local proof triplet for one
// hour: page cursor/control, the normal member-hour proof, and the staged
// dimensional rows.  Counting checkpoint rows alone is insufficient because
// a damaged local row must also cause a bounded one-hour rebuild.
func usageFactRawPageHourReady(db *gorm.DB, userID, hourTs int64, sourceEpoch string) (bool, error) {
	var page UsageFactPageIngestState
	if err := db.First(&page, "user_id = ? AND hour_ts = ?", userID, hourTs).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	if page.Status != "complete" || page.SourceEpoch != sourceEpoch || page.ContentHash == "" {
		return false, nil
	}
	var proof UsageFactMemberHourState
	if err := db.First(&proof, "user_id = ? AND hour_ts = ?", userID, hourTs).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	if proof.Status != "complete" || proof.SourceEpoch != sourceEpoch || proof.ContentHash == "" {
		return false, nil
	}
	rows, err := loadUsageFactHour(db, hourTs, []int64{userID})
	if err != nil {
		return false, err
	}
	metrics := factsMetrics(rows)
	ready := usageFactRawRecordCount(metrics) == page.SourceRows && metrics.Requests == page.Requests &&
		metrics.RefundRecords == page.RefundRecords && metrics.PromptTokens == page.PromptTokens &&
		metrics.CompletionTokens == page.CompletionTokens && metrics.ConsumeQuota == page.ConsumeQuota &&
		metrics.RefundQuota == page.RefundQuota && usageFactContentHash(rows) == page.ContentHash &&
		usageFactMemberMetricsMatchState(metrics, proof) && usageFactContentHash(rows) == proof.ContentHash
	return ready, nil
}

// realignUsageFactRawPageClaim upgrades a durable job created by the legacy
// day/hour GROUP BY worker.  Such a job can be parked in the middle of a CST
// day without page controls for its already-advanced prefix.  Letting it reach
// 23:00 would make the raw day finalizer retry forever.  Rewind only that open
// day (at most 23 hours); completed prior days remain untouched.
func (m *Monitor) realignUsageFactRawPageClaim(ctx context.Context, claim usageFactHistoryClaim, now time.Time) (bool, error) {
	if len(claim.Jobs) != 1 || claim.From <= 0 {
		return false, errors.New("原始日志分页任务对齐范围无效")
	}
	job := claim.Jobs[0]
	if job.UserID == nil || *job.UserID <= 0 || job.SourceEpoch == "" {
		return false, errors.New("原始日志分页任务对齐缺少成员或来源 epoch")
	}
	dayFrom := usageFactDayStart(claim.From)
	rewindFrom := max(dayFrom, job.FromTs)
	if rewindFrom >= claim.From {
		return false, nil
	}

	realigned := false
	err := m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current UsageFactJob
		if err := tx.First(&current, "id = ? AND lease_owner = ? AND status = ?", job.ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; err != nil {
			return err
		}
		if current.NextHour != claim.From || current.TrackedRevision != job.TrackedRevision || current.SourceEpoch != job.SourceEpoch {
			return errors.New("原始日志分页任务对齐时游标已变化")
		}
		for hour := rewindFrom; hour < claim.From; hour += usageFactHourSeconds {
			ready, err := usageFactRawPageHourReady(tx, *job.UserID, hour, job.SourceEpoch)
			if err != nil {
				return err
			}
			if ready {
				continue
			}
			current.NextHour = rewindFrom
			current.CompletedHours = max(current.NextHour-current.FromTs, 0) / usageFactHourSeconds
			current.Status, current.LeaseOwner, current.LeaseUntil = usageFactHistoryJobQueued, "", 0
			current.Attempts, current.NextRetryAt, current.LastError = 0, 0, ""
			current.HeartbeatAt, current.UpdatedAt = now.Unix(), now.Unix()
			if err := tx.Save(&current).Error; err != nil {
				return err
			}
			var member UsageFactMemberState
			if err := tx.First(&member, "user_id = ?", *job.UserID).Error; err != nil {
				return err
			}
			updates := map[string]any{"coverage_status": "backfilling", "last_error": "", "updated_at": now.Unix()}
			if member.CoverageThroughHour != nil && *member.CoverageThroughHour > rewindFrom {
				updates["coverage_through_hour"] = rewindFrom
			}
			if member.TailThroughHour != nil && *member.TailThroughHour > rewindFrom {
				updates["tail_through_hour"] = rewindFrom
			}
			if err := tx.Model(&UsageFactMemberState{}).Where("user_id = ? AND active = ? AND tracked_revision = ?", *job.UserID, true, job.TrackedRevision).Updates(updates).Error; err != nil {
				return err
			}
			realigned = true
			break
		}
		return nil
	})
	return realigned, err
}

// invalidateNoHistoryAfterRawActivity preserves the existing publication
// fence for a member previously proven to have no source history. The first
// bounded page proves that new activity exists; this transaction revokes the
// old no-history signature and returns the durable job to discovery before any
// new dimensional rows are staged.
func (m *Monitor) invalidateNoHistoryAfterRawActivity(ctx context.Context, job UsageFactJob, now time.Time) error {
	if job.UserID == nil || *job.UserID <= 0 || job.TrackedRevision < 1 {
		return errors.New("无历史分页撤权缺少成员")
	}
	nowUnix := now.Unix()
	var global UsageFactSyncState
	var revoked bool
	m.usageFactsSyncMu.Lock()
	err := m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&global, 1).Error; err != nil {
			return err
		}
		invalidated, oneRevoked, err := m.invalidateNoHistoryAfterHourTx(tx, []int64{*job.UserID}, &global, nowUnix)
		if err != nil {
			return err
		}
		if len(invalidated) != 1 || invalidated[0] != *job.UserID {
			return fmt.Errorf("%w: no-history raw activity user_id=%d", errUsageMemberControlIntegrity, *job.UserID)
		}
		global.Generation++
		revoked = oneRevoked
		return tx.Save(&global).Error
	})
	m.usageFactsSyncMu.Unlock()
	if err != nil {
		return err
	}
	m.publishUsageFactGenerations(global.Generation, 0)
	if revoked {
		m.publishUsageFactGenerations(0, global.ServingGeneration)
		m.publishUsageFactReadBoundsAfterMutation(global)
	}
	return nil
}

func normalizedUsageFactRawSpanHours(value int) int {
	switch value {
	case 1, usageFactRawShardMediumHours, usageFactRawShardWideHours, usageFactRawShardDefaultHours:
		return value
	default:
		// Existing databases predate this field. Start their first resumed shard
		// at one hour, then widen from observed low density after it completes.
		return 1
	}
}

func smallerUsageFactRawSpanHours(value int) int {
	switch normalizedUsageFactRawSpanHours(value) {
	case usageFactRawShardDefaultHours:
		return usageFactRawShardWideHours
	case usageFactRawShardWideHours:
		return usageFactRawShardMediumHours
	default:
		return 1
	}
}

func usageFactRawColdShardRange(db *gorm.DB, member UsageFactMemberState, job UsageFactJob, fromTs int64) (int64, int, error) {
	var existing UsageFactPageIngestState
	err := db.First(&existing, "user_id = ? AND hour_ts = ?", member.UserID, fromTs).Error
	if err == nil {
		through := usageFactRawPageStateThrough(existing)
		if through <= fromTs || through > job.ThroughTs || through > usageFactDayStart(fromTs)+usageFactDaySeconds {
			return 0, 0, errors.New("原始日志分页持久分片范围无效")
		}
		return through, int((through - fromTs) / usageFactHourSeconds), nil
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, 0, err
	}
	spanHours := normalizedUsageFactRawSpanHours(member.RawPageSpanHours)
	through := fromTs + int64(spanHours)*usageFactHourSeconds
	dayThrough := usageFactDayStart(fromTs) + usageFactDaySeconds
	if through > dayThrough {
		through = dayThrough
	}
	if through > job.ThroughTs {
		through = job.ThroughTs
	}
	if through <= fromTs {
		return 0, 0, errors.New("原始日志分页全历史没有可执行范围")
	}
	return through, int((through - fromTs) / usageFactHourSeconds), nil
}

// executeUsageFactHistoryBackfillRawPages is the cold-worker bridge for the
// adaptive raw-page protocol. Low-density history covers up to one CST day per
// shard; a full first page shrinks 24h -> 6h -> 3h -> 1h before any local fact write.
// Every turn still yields after the bounded page budget, so a high-volume
// member cannot monopolize the shared source lane.
func (m *Monitor) executeUsageFactHistoryBackfillRawPages(ctx context.Context, claim usageFactHistoryClaim, now time.Time) error {
	if !m.usageFactsRawPageImportEnabled() || len(claim.Jobs) != 1 || claim.From <= 0 || claim.Through <= claim.From {
		err := errors.New("原始日志分页全历史任务范围无效")
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	job := claim.Jobs[0]
	if job.UserID == nil || *job.UserID <= 0 || claim.From%usageFactHourSeconds != 0 {
		err := errors.New("原始日志分页全历史任务缺少成员小时")
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	if err := m.usageFactJobRevisionCurrent(ctx, job); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	if realigned, err := m.realignUsageFactRawPageClaim(ctx, claim, now); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	} else if realigned {
		return nil
	}
	var member UsageFactMemberState
	if err := m.usageFactsStore().WithContext(ctx).First(&member, "user_id = ? AND active = ? AND tracked_revision = ?", *job.UserID, true, job.TrackedRevision).Error; err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	throughTs, spanHours, err := usageFactRawColdShardRange(m.usageFactsStore().WithContext(ctx), member, job, claim.From)
	if err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	complete, err := importUsageFactRawShardPages(ctx, m.usageFactsStore(), m.usageFactRawPageSource(true), *job.UserID, claim.From, throughTs, job.SourceEpoch, usageFactRawPagesPerTurn)
	if errors.Is(err, errUsageFactRawShardDense) {
		nextSpan := smallerUsageFactRawSpanHours(spanHours)
		updateErr := m.usageFactsStore().WithContext(context.Background()).Model(&UsageFactMemberState{}).
			Where("user_id = ? AND active = ? AND tracked_revision = ?", *job.UserID, true, job.TrackedRevision).
			Updates(map[string]any{"raw_page_span_hours": nextSpan, "updated_at": now.Unix()}).Error
		if updateErr != nil {
			_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, updateErr, now, false)
			return updateErr
		}
		return m.releaseUsageFactHistoryClaim(context.Background(), claim, errUsageFactAdaptiveBudget, now, true)
	}
	if err != nil {
		immediate := errors.Is(err, errUsageFactSourceBusy) || errors.Is(err, context.Canceled)
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, immediate)
		return err
	}
	if !complete {
		// Yield without charging an attempt.  The durable raw-page cursor has
		// already advanced in the same SQLite transaction as its local facts.
		return m.releaseUsageFactHistoryClaim(context.Background(), claim, errUsageFactAdaptiveBudget, now, true)
	}
	if err := m.commitUsageFactRawPageShardProof(ctx, *job.UserID, claim.From, throughTs, job.TrackedRevision); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	if err := m.maybeFinalizeUsageFactHistoryDayFromHours(ctx, throughTs-usageFactHourSeconds, []int64{*job.UserID}, true); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	nextHour := throughTs
	nowUnix := now.Unix()
	return m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current UsageFactJob
		if err := tx.First(&current, "id = ? AND lease_owner = ? AND status = ?", job.ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; err != nil {
			return err
		}
		if current.NextHour != claim.From || current.TrackedRevision != job.TrackedRevision {
			return errors.New("原始日志分页全历史游标并发变化")
		}
		current.NextHour = nextHour
		current.CompletedHours = max(current.NextHour-current.FromTs, 0) / usageFactHourSeconds
		current.Status, current.Kind = usageFactHistoryJobQueued, usageFactHistoryKindBackfill
		current.Attempts = 0
		if current.NextHour >= current.ThroughTs {
			current.Kind = usageFactHistoryKindVerify
			current.CompletedAt = 0
		} else if current.NextHour >= usageFactDayStart(current.ThroughTs) {
			current.Kind = usageFactHistoryKindTail
		}
		current.LeaseOwner, current.LeaseUntil, current.NextRetryAt = "", 0, 0
		current.LastError, current.HeartbeatAt, current.UpdatedAt = "", nowUnix, nowUnix
		if err := tx.Save(&current).Error; err != nil {
			return err
		}
		return tx.Model(&UsageFactMemberState{}).
			Where("user_id = ? AND active = ? AND tracked_revision = ? AND source_epoch = ?", *job.UserID, true, job.TrackedRevision, job.SourceEpoch).
			Updates(map[string]any{"coverage_through_hour": nextHour, "coverage_status": "backfilling", "last_success_at": nowUnix, "last_failure_at": int64(0), "last_error": "", "updated_at": nowUnix}).Error
	})
}

// executeUsageFactHistoryTailRawPages keeps the live waterline on the same
// bounded page protocol as cold history. A no-history member is also paged:
// its first page doubles as an activity probe, and any row atomically revokes
// the prior no-history publication before rediscovery.
func (m *Monitor) executeUsageFactHistoryTailRawPages(ctx context.Context, claim usageFactHistoryClaim, now time.Time) error {
	if !m.usageFactsRawPageImportEnabled() || len(claim.Jobs) != 1 || claim.From <= 0 || claim.Through != claim.From+usageFactHourSeconds {
		err := errors.New("原始日志分页 Tail 任务范围无效")
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	job := claim.Jobs[0]
	if job.UserID == nil || *job.UserID <= 0 {
		err := errors.New("原始日志分页 Tail 缺少成员")
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	var member UsageFactMemberState
	if err := m.usageFactsStore().WithContext(ctx).First(&member, "user_id = ?", *job.UserID).Error; err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	if err := m.usageFactJobRevisionCurrent(ctx, job); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	if realigned, err := m.realignUsageFactRawPageClaim(ctx, claim, now); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	} else if realigned {
		return nil
	}
	source := m.usageFactRawPageSource(false)
	var pageSource usageFactRawPageSource = source
	if member.SourceHistoryStatus == "no_history" {
		var checkpoint UsageFactPageIngestState
		checkpointErr := m.usageFactsStore().WithContext(ctx).First(&checkpoint, "user_id = ? AND hour_ts = ?", *job.UserID, claim.From).Error
		if checkpointErr != nil && !errors.Is(checkpointErr, gorm.ErrRecordNotFound) {
			_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, checkpointErr, now, false)
			return checkpointErr
		}
		if checkpointErr == nil && (checkpoint.SourceRows > 0 || checkpoint.CursorID > 0) {
			if err := m.invalidateNoHistoryAfterRawActivity(ctx, job, now); err != nil {
				_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
				return err
			}
			return nil
		}
		first, err := source.FetchUsageFactRawPage(ctx, *job.UserID, claim.From, claim.From+usageFactHourSeconds, 0, 0, 0, usageFactRawPageSize)
		if err != nil {
			immediate := errors.Is(err, errUsageFactSourceBusy) || errors.Is(err, context.Canceled)
			_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, immediate)
			return err
		}
		if len(first) > 0 {
			if err := m.invalidateNoHistoryAfterRawActivity(ctx, job, now); err != nil {
				_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
				return err
			}
			return nil
		}
		pageSource = &usageFactPrefetchedRawPageSource{base: source, userID: *job.UserID, fromTs: claim.From, throughTs: claim.From + usageFactHourSeconds, page: first}
	}
	complete, err := importUsageFactRawPages(ctx, m.usageFactsStore(), pageSource, *job.UserID, claim.From, job.SourceEpoch, usageFactRawPagesPerTurn)
	if err != nil {
		immediate := errors.Is(err, errUsageFactSourceBusy) || errors.Is(err, context.Canceled)
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, immediate)
		return err
	}
	if !complete {
		return m.releaseUsageFactHistoryClaim(context.Background(), claim, errUsageFactAdaptiveBudget, now, true)
	}
	if err := m.commitUsageFactRawPageHourProof(ctx, *job.UserID, claim.From, job.TrackedRevision); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	if err := m.maybeFinalizeUsageFactHistoryDayFromHours(ctx, claim.From, []int64{*job.UserID}, true); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	nextHour := min(claim.From+usageFactHourSeconds, job.ThroughTs)
	nowUnix := now.Unix()
	return m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current UsageFactJob
		if err := tx.First(&current, "id = ? AND lease_owner = ? AND status = ?", job.ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; err != nil {
			return err
		}
		if current.NextHour != claim.From || current.TrackedRevision != job.TrackedRevision {
			return errors.New("原始日志分页 Tail 游标并发变化")
		}
		var currentMember UsageFactMemberState
		if err := tx.First(&currentMember, "user_id = ?", *job.UserID).Error; err != nil {
			return err
		}
		current.NextHour = nextHour
		current.CompletedHours = max(current.NextHour-current.FromTs, 0) / usageFactHourSeconds
		current.Status, current.LeaseOwner, current.LeaseUntil, current.NextRetryAt = usageFactHistoryJobQueued, "", 0, 0
		current.Attempts = 0
		current.LastError, current.HeartbeatAt, current.UpdatedAt = "", nowUnix, nowUnix
		memberUpdates := map[string]any{"tail_through_hour": current.NextHour, "last_success_at": nowUnix, "last_failure_at": int64(0), "last_error": "", "updated_at": nowUnix}
		if usageFactDayStart(current.NextHour) == current.NextHour && current.NextHour > claim.From {
			memberUpdates["coverage_through_hour"] = current.NextHour
			verifiedThrough := current.NextHour
			if currentMember.VerifiedThroughHour != nil && *currentMember.VerifiedThroughHour > verifiedThrough {
				verifiedThrough = *currentMember.VerifiedThroughHour
			}
			memberUpdates["verified_through_hour"] = verifiedThrough
			memberUpdates["verify_next_hour"] = verifiedThrough
			current.VerifyNextHour = verifiedThrough
			current.VerifiedHours = max(verifiedThrough-current.FromTs, 0) / usageFactHourSeconds
			if current.NextHour < usageFactDayStart(current.ThroughTs) {
				current.Kind = usageFactHistoryKindBackfill
			}
		}
		if current.NextHour >= current.ThroughTs {
			dayTarget := usageFactDayStart(current.ThroughTs)
			alreadyVerified := currentMember.VerificationStatus == "complete" && currentMember.VerifiedThroughHour != nil &&
				*currentMember.VerifiedThroughHour >= dayTarget
			current.Kind = usageFactHistoryKindVerify
			if alreadyVerified {
				// Intraday Tail adds hours inside a natural day whose durable day
				// checkpoint is unchanged. Keep the signed serving state ready and
				// avoid a redundant verify turn that would transiently close reads.
				current.Status, current.CompletedAt = usageFactHistoryJobComplete, nowUnix
				memberUpdates["coverage_status"] = "ready"
				memberUpdates["verification_status"] = "complete"
			} else {
				current.Status, current.CompletedAt = usageFactHistoryJobQueued, 0
				memberUpdates["coverage_status"] = "verifying"
				memberUpdates["verification_status"] = "running"
			}
		}
		if err := tx.Save(&current).Error; err != nil {
			return err
		}
		return tx.Model(&UsageFactMemberState{}).
			Where("user_id = ? AND active = ? AND tracked_revision = ?", *job.UserID, true, job.TrackedRevision).
			Updates(memberUpdates).Error
	})
}

// executeUsageFactHistoryRepairHourRawPages applies the exact same bounded
// import protocol to a repair hold.  Repair remains fail-closed for serving,
// but a high-volume member is repaired hour by hour instead of being subjected
// to a source-side dimensional day aggregation.
func (m *Monitor) executeUsageFactHistoryRepairHourRawPages(ctx context.Context, claim usageFactHistoryClaim, now time.Time) error {
	if !m.usageFactsRawPageImportEnabled() || len(claim.Jobs) != 1 || claim.From <= 0 || claim.Through != claim.From+usageFactHourSeconds {
		err := errors.New("原始日志分页修复小时范围无效")
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	job := claim.Jobs[0]
	if job.UserID == nil || *job.UserID <= 0 || job.Kind != usageFactHistoryKindRepairHour {
		err := errors.New("原始日志分页修复缺少成员")
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	if err := m.usageFactJobRevisionCurrent(ctx, job); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	if realigned, err := m.realignUsageFactRawPageClaim(ctx, claim, now); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	} else if realigned {
		return nil
	}
	complete, err := importUsageFactRawPages(ctx, m.usageFactsStore(), m.usageFactRawPageSource(true), *job.UserID, claim.From, job.SourceEpoch, usageFactRawPagesPerTurn)
	if err != nil {
		immediate := errors.Is(err, errUsageFactSourceBusy) || errors.Is(err, context.Canceled)
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, immediate)
		return err
	}
	if !complete {
		return m.releaseUsageFactHistoryClaim(context.Background(), claim, errUsageFactAdaptiveBudget, now, true)
	}
	if err := m.commitUsageFactRawPageHourProof(ctx, *job.UserID, claim.From, job.TrackedRevision); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	if err := m.maybeFinalizeUsageFactHistoryDayFromHours(ctx, claim.From, []int64{*job.UserID}, true); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	nextHour := min(claim.From+usageFactHourSeconds, job.ThroughTs)
	nowUnix := now.Unix()
	completed := false
	var completedJob UsageFactJob
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current UsageFactJob
		if err := tx.First(&current, "id = ? AND lease_owner = ? AND status = ?", job.ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; err != nil {
			return err
		}
		if current.Kind != usageFactHistoryKindRepairHour || current.NextHour != claim.From || current.TrackedRevision != job.TrackedRevision {
			return errors.New("原始日志分页修复游标并发变化")
		}
		current.NextHour = nextHour
		current.CompletedHours = max(current.NextHour-current.FromTs, 0) / usageFactHourSeconds
		current.LeaseOwner, current.LeaseUntil, current.NextRetryAt = "", 0, 0
		current.Attempts, current.LastError, current.HeartbeatAt, current.UpdatedAt = 0, "", nowUnix, nowUnix
		current.Status = usageFactHistoryJobQueued
		if current.NextHour >= current.ThroughTs {
			current.Status, current.CompletedAt = usageFactHistoryJobComplete, nowUnix
			completed, completedJob = true, current
		}
		return tx.Save(&current).Error
	})
	if err != nil {
		return err
	}
	if completed {
		return m.afterUsageFactRepairsCompleted(ctx, []UsageFactJob{completedJob}, now)
	}
	return nil
}

// executeUsageFactHistorySourceAuditHourRawPages re-verifies a served member
// one bounded hour at a time.  Until the 24th hour signs the replacement daily
// fact, the old published day remains authoritative; this is verification, not
// a premature customer-visible rewrite.
func (m *Monitor) executeUsageFactHistorySourceAuditHourRawPages(ctx context.Context, claim usageFactHistoryClaim, now time.Time) error {
	if !m.usageFactsRawPageImportEnabled() || len(claim.Jobs) != 1 || claim.From <= 0 || claim.Through != claim.From+usageFactHourSeconds {
		err := errors.New("原始日志分页来源审计小时范围无效")
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	job := claim.Jobs[0]
	if job.UserID == nil || *job.UserID <= 0 || job.Kind != usageFactHistoryKindAuditHour || job.VerifyNextHour <= job.NextHour {
		err := errors.New("原始日志分页来源审计检查点无效")
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	if err := m.usageFactJobRevisionCurrent(ctx, job); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	if realigned, err := m.realignUsageFactRawPageClaim(ctx, claim, now); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	} else if realigned {
		return nil
	}
	complete, err := importUsageFactRawPages(ctx, m.usageFactsStore(), m.usageFactRawPageSource(true), *job.UserID, claim.From, job.SourceEpoch, usageFactRawPagesPerTurn)
	if err != nil {
		immediate := errors.Is(err, errUsageFactSourceBusy) || errors.Is(err, context.Canceled)
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, immediate)
		return err
	}
	if !complete {
		return m.releaseUsageFactHistoryClaim(context.Background(), claim, errUsageFactAdaptiveBudget, now, true)
	}
	if err := m.commitUsageFactRawPageHourProof(ctx, *job.UserID, claim.From, job.TrackedRevision); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	if err := m.maybeFinalizeUsageFactHistoryDayFromHours(ctx, claim.From, []int64{*job.UserID}, true); err != nil {
		_ = m.releaseUsageFactHistoryClaim(context.Background(), claim, err, now, false)
		return err
	}
	nextHour := min(claim.From+usageFactHourSeconds, job.VerifyNextHour)
	nowUnix := now.Unix()
	return m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var current UsageFactJob
		if err := tx.First(&current, "id = ? AND lease_owner = ? AND status = ?", job.ID, claim.LeaseOwner, usageFactHistoryJobRunning).Error; err != nil {
			return err
		}
		if current.Kind != usageFactHistoryKindAuditHour || current.NextHour != claim.From || current.VerifyNextHour != job.VerifyNextHour {
			return errors.New("原始日志分页来源审计游标并发变化")
		}
		current.NextHour = nextHour
		current.CompletedHours = max(current.NextHour-current.FromTs, 0) / usageFactHourSeconds
		current.LeaseOwner, current.LeaseUntil, current.NextRetryAt = "", 0, 0
		current.Attempts, current.LastError, current.HeartbeatAt, current.UpdatedAt = 0, "", nowUnix, nowUnix
		current.Status = usageFactHistoryJobQueued
		if current.NextHour >= current.VerifyNextHour {
			dayFrom := current.VerifyNextHour - usageFactDaySeconds
			if err := tx.Where("user_id = ? AND hour_ts >= ? AND hour_ts < ?", *job.UserID, dayFrom, current.VerifyNextHour).Delete(&UsageHourFact{}).Error; err != nil {
				return err
			}
			if err := tx.Where("user_id = ? AND hour_ts >= ? AND hour_ts < ?", *job.UserID, dayFrom, current.VerifyNextHour).Delete(&UsageFactMemberHourState{}).Error; err != nil {
				return err
			}
			if err := tx.Where("user_id = ? AND hour_ts >= ? AND hour_ts < ?", *job.UserID, dayFrom, current.VerifyNextHour).Delete(&UsageFactPageIngestState{}).Error; err != nil {
				return err
			}
			baseKind, baseErr := usageFactSourceAuditBaseKind(current)
			if baseErr != nil {
				return baseErr
			}
			current.Kind, current.VerifyNextHour = baseKind, 0
			if current.NextHour >= current.ThroughTs {
				current.Status, current.CompletedAt = usageFactHistoryJobComplete, nowUnix
			}
		}
		return tx.Save(&current).Error
	})
}
