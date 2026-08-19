package monitor

// usage_facts_history.go implements the cold, day-grained source read used by
// all-history backfill.  It is intentionally separate from the durable job
// runner: this file only returns a fully controlled in-memory range.  Nothing
// is written to SQLite unless both the dimensional query and an independent
// per-member/day control query agree exactly.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

const (
	usageFactHistoryInitialMaxDays = 1
	usageFactHistoryMaxDays        = 7
	usageFactHistoryMaxMembers     = 50
	usageFactHistoryMaxRows        = 20_000
)

var (
	errUsageFactHistoryRangeTooLarge = errors.New("历史日聚合结果超过安全上限")
	errUsageFactHistoryControl       = errors.New("历史日聚合独立控制对账不一致")
)

// usageFactHistoryMemberControlError 保留已定位的坏成员。调度器可以用
// 同一条批量控制查询继续签收其他成员，而不是把 50 人全部单拆回源。
type usageFactHistoryMemberControlError struct {
	UserID int64
	DayTs  int64
	Detail string
}

func (e *usageFactHistoryMemberControlError) Error() string {
	if e == nil {
		return errUsageFactHistoryControl.Error()
	}
	return fmt.Sprintf("%s: user=%d day=%d %s", errUsageFactHistoryControl, e.UserID, e.DayTs, strings.TrimSpace(e.Detail))
}

func (e *usageFactHistoryMemberControlError) Unwrap() error { return errUsageFactHistoryControl }

// usageFactHistoryFinalizeError 只表示已精确定位的成员级失败。未列入
// Failures 的成员已完成日终签收，可以独立推进持久游标。
type usageFactHistoryFinalizeError struct {
	Failures map[int64]error
}

func (e *usageFactHistoryFinalizeError) Error() string {
	if e == nil || len(e.Failures) == 0 {
		return ""
	}
	ids := make([]int64, 0, len(e.Failures))
	for id := range e.Failures {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return fmt.Sprintf("日终签收存在 %d 个成员级失败: user_ids=%v", len(ids), ids)
}

type usageFactHistoryControl struct {
	UserID           int64
	DateTs           int64
	SourceRows       int64
	Requests         int64
	RefundRecords    int64
	PromptTokens     int64
	CompletionTokens int64
	ConsumeQuota     int64
	RefundQuota      int64
}

func (c usageFactHistoryControl) metrics() usageFactMetrics {
	return usageFactMetrics{
		Rows:             c.SourceRows,
		Requests:         c.Requests,
		RefundRecords:    c.RefundRecords,
		PromptTokens:     c.PromptTokens,
		CompletionTokens: c.CompletionTokens,
		ConsumeQuota:     c.ConsumeQuota,
		RefundQuota:      c.RefundQuota,
	}
}

type usageFactHistoryRange struct {
	FromTs        int64
	ThroughTs     int64
	UserIDs       []int64
	Facts         map[usageFactMemberDayKey][]UsageDailyFact
	Controls      map[usageFactMemberDayKey]usageFactHistoryControl
	Rows          int
	SourceQueries int
	QueryDuration time.Duration
}

func usageFactHistoryDayIndexToTs(dayIndex int64) int64 {
	return dayIndex*usageFactDaySeconds - usageTZOffsetSec
}

func validateUsageFactHistoryRange(fromTs, throughTs int64, ids []int64) ([]int64, error) {
	ordered, err := normalizedUsageFactHistoryIDs(ids, usageFactHistoryMaxMembers)
	if err != nil {
		return nil, err
	}
	if fromTs <= 0 || throughTs <= fromTs || usageFactDayStart(fromTs) != fromTs || usageFactDayStart(throughTs) != throughTs {
		return nil, errors.New("历史补全范围必须是非空 CST 自然日")
	}
	days := (throughTs - fromTs) / usageFactDaySeconds
	if days < 1 || days > usageFactHistoryMaxDays {
		return nil, fmt.Errorf("历史补全单段必须为 1～%d 个自然日", usageFactHistoryMaxDays)
	}
	return ordered, nil
}

func usageFactHistoryDetailSQL(dayExpr, memberSQL, testPredicate string) string {
	return `SELECT /*+ MAX_EXECUTION_TIME(5000) */
  ` + dayExpr + ` AS day_idx,
  COALESCE(user_id,0), COALESCE(channel_id,0), COALESCE(` + "`group`" + `,''),
  COALESCE(model_name,''), COALESCE(token_id,0), COALESCE(MAX(token_name),''),
  CAST(COALESCE(SUM(CASE WHEN type = 2 THEN 1 ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 6 THEN 1 ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 2 THEN COALESCE(prompt_tokens,0) ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 2 THEN COALESCE(completion_tokens,0) ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 2 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 6 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED)
 FROM logs
 WHERE created_at >= ? AND created_at < ? AND type IN (2,6) AND ` + memberSQL + `
   AND NOT (` + testPredicate + `)
 GROUP BY day_idx, user_id, channel_id, ` + "`group`" + `, model_name, token_id
 ORDER BY day_idx, user_id, channel_id, ` + "`group`" + `, model_name, token_id
 LIMIT ?`
}

func usageFactHistoryControlSQL(dayExpr, memberSQL, testPredicate string) string {
	return `SELECT /*+ MAX_EXECUTION_TIME(5000) */
  ` + dayExpr + ` AS day_idx, COALESCE(user_id,0), COUNT(*),
  CAST(COALESCE(SUM(CASE WHEN type = 2 THEN 1 ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 6 THEN 1 ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 2 THEN COALESCE(prompt_tokens,0) ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 2 THEN COALESCE(completion_tokens,0) ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 2 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED),
  CAST(COALESCE(SUM(CASE WHEN type = 6 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED)
 FROM logs
 WHERE created_at >= ? AND created_at < ? AND type IN (2,6) AND ` + memberSQL + `
   AND NOT (` + testPredicate + `)
 GROUP BY day_idx, user_id
 ORDER BY day_idx, user_id
 LIMIT ?`
}

func (m *Monitor) fetchUsageFactHistoryControls(
	ctx context.Context,
	fromTs, throughTs int64,
	ids []int64,
) (map[usageFactMemberDayKey]usageFactHistoryControl, time.Duration, error) {
	ordered, err := validateUsageFactHistoryRange(fromTs, throughTs, ids)
	if err != nil {
		return nil, 0, err
	}
	inSQL, inArgs := usageIn("user_id", ordered)
	query := usageFactHistoryControlSQL(m.dayExpr(), inSQL, m.channelTestSourcePredicateSQL())
	args := append([]any{fromTs, throughTs}, inArgs...)
	maxMemberDays := len(ordered) * int((throughTs-fromTs)/usageFactDaySeconds)
	args = append(args, maxMemberDays+1)
	controls := make(map[usageFactMemberDayKey]usageFactHistoryControl, maxMemberDays)
	var duration time.Duration
	err = m.withUsageFactHistorySourceQuery(ctx, func(qctx context.Context) error {
		started := time.Now()
		defer func() { duration = time.Since(started) }()
		rows, queryErr := m.prodDB.QueryContext(qctx, query, args...)
		if queryErr != nil {
			return fmt.Errorf("读取历史日独立控制数失败: %w", queryErr)
		}
		defer rows.Close()
		seen := 0
		for rows.Next() {
			var dayIndex int64
			var control usageFactHistoryControl
			if scanErr := rows.Scan(
				&dayIndex, &control.UserID, &control.SourceRows, &control.Requests, &control.RefundRecords,
				&control.PromptTokens, &control.CompletionTokens, &control.ConsumeQuota, &control.RefundQuota,
			); scanErr != nil {
				return scanErr
			}
			seen++
			if seen > maxMemberDays {
				return errUsageFactHistoryRangeTooLarge
			}
			control.DateTs = usageFactHistoryDayIndexToTs(dayIndex)
			if control.DateTs < fromTs || control.DateTs >= throughTs {
				return fmt.Errorf("历史控制数返回越界日期 %d", control.DateTs)
			}
			if control.SourceRows != control.Requests+control.RefundRecords {
				return &usageFactHistoryMemberControlError{UserID: control.UserID, DayTs: control.DateTs,
					Detail: fmt.Sprintf("source_rows=%d records=%d", control.SourceRows, control.Requests+control.RefundRecords)}
			}
			key := usageFactMemberDayKey{userID: control.UserID, dayTs: control.DateTs}
			if _, duplicate := controls[key]; duplicate {
				return &usageFactHistoryMemberControlError{UserID: control.UserID, DayTs: control.DateTs, Detail: "来源控制行重复"}
			}
			controls[key] = control
		}
		return rows.Err()
	})
	if err != nil {
		return nil, duration, err
	}
	for day := fromTs; day < throughTs; day += usageFactDaySeconds {
		for _, id := range ordered {
			key := usageFactMemberDayKey{userID: id, dayTs: day}
			if _, ok := controls[key]; !ok {
				controls[key] = usageFactHistoryControl{UserID: id, DateTs: day}
			}
		}
	}
	return controls, duration, nil
}

// usageFactRawPageDayControls folds 24 already source-controlled member-hours
// into a CST day control.  Each page state is produced only after its own
// independent second bounded cursor pass, so summing them preserves exact day
// arithmetic without asking MySQL to rescan with COUNT/SUM/GROUP BY.
func (m *Monitor) usageFactRawPageDayControls(ctx context.Context, dayTs int64, ids []int64) (map[usageFactMemberDayKey]usageFactHistoryControl, error) {
	ordered, err := normalizedUsageFactHistoryIDs(ids, usageFactHistoryMaxMembers)
	if err != nil {
		return nil, err
	}
	if dayTs <= 0 || usageFactDayStart(dayTs) != dayTs {
		return nil, errors.New("分页小时控制数必须归并到 CST 自然日")
	}
	epoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
	if epoch == "" {
		return nil, errors.New("分页小时控制数缺少来源 epoch")
	}
	inSQL, inArgs := usageIn("user_id", ordered)
	args := append([]any{dayTs, dayTs + usageFactDaySeconds}, inArgs...)
	args = append(args, "complete", epoch)
	var states []UsageFactPageIngestState
	if err := m.usageFactsStore().WithContext(ctx).
		Where("hour_ts >= ? AND hour_ts < ? AND "+inSQL+" AND status = ? AND source_epoch = ? AND content_hash <> ''", args...).
		Order("user_id, hour_ts").Find(&states).Error; err != nil {
		return nil, err
	}
	type rawMemberHourKey struct{ userID, hourTs int64 }
	byMemberHour := make(map[rawMemberHourKey]UsageFactPageIngestState, len(states))
	for _, state := range states {
		key := rawMemberHourKey{userID: state.UserID, hourTs: state.HourTs}
		if _, duplicate := byMemberHour[key]; duplicate {
			return nil, errors.New("分页小时控制数存在重复状态")
		}
		byMemberHour[key] = state
	}
	controls := make(map[usageFactMemberDayKey]usageFactHistoryControl, len(ordered))
	for _, id := range ordered {
		control := usageFactHistoryControl{UserID: id, DateTs: dayTs}
		for hour := dayTs; hour < dayTs+usageFactDaySeconds; hour += usageFactHourSeconds {
			state, ok := byMemberHour[rawMemberHourKey{userID: id, hourTs: hour}]
			if !ok {
				return nil, fmt.Errorf("分页小时控制数尚未完整 user_id=%d hour=%d", id, hour)
			}
			control.SourceRows += state.SourceRows
			control.Requests += state.Requests
			control.RefundRecords += state.RefundRecords
			control.PromptTokens += state.PromptTokens
			control.CompletionTokens += state.CompletionTokens
			control.ConsumeQuota += state.ConsumeQuota
			control.RefundQuota += state.RefundQuota
		}
		controls[usageFactMemberDayKey{userID: id, dayTs: dayTs}] = control
	}
	return controls, nil
}

// finalizeUsageFactHistoryDayFromHours turns 24 independently controlled hour
// reads into a source-signed day proof. The raw-page protocol sums its durable
// per-hour controls locally; the legacy path retains its source day control
// while older workers are still present.
func (m *Monitor) finalizeUsageFactHistoryDayFromHours(
	ctx context.Context,
	dayTs int64,
	ids []int64,
	revisions map[int64]int64,
	jobID string,
) error {
	ordered, err := normalizedUsageFactHistoryIDs(ids, usageFactHistoryMaxMembers)
	if err != nil {
		return err
	}
	if dayTs <= 0 || usageFactDayStart(dayTs) != dayTs {
		return errors.New("小时回填日终范围必须是 CST 自然日")
	}
	var controls map[usageFactMemberDayKey]usageFactHistoryControl
	if m.usageFactsRawPageImportEnabled() {
		controls, err = m.usageFactRawPageDayControls(ctx, dayTs, ordered)
	} else {
		controls, _, err = m.fetchUsageFactHistoryControls(ctx, dayTs, dayTs+usageFactDaySeconds, ordered)
	}
	if err != nil {
		return err
	}
	before, err := m.usageFactHistoryRevisionsCurrent(ctx, ordered, revisions)
	if err != nil {
		return err
	}
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		jobID = "tail-day-" + strconv.FormatInt(dayTs, 36)
	}
	if len(jobID) > 80 {
		return errors.New("日终证明 job_id 超过 80 字节")
	}
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	db := m.usageFactsStore().WithContext(ctx)
	var memberStates []UsageFactMemberState
	memberSQL, memberArgs := usageIn("user_id", ordered)
	if err := db.Where(memberSQL, memberArgs...).Find(&memberStates).Error; err != nil {
		return err
	}
	stateByID := make(map[int64]UsageFactMemberState, len(memberStates))
	for _, state := range memberStates {
		stateByID[state.UserID] = state
	}
	for _, id := range ordered {
		state, ok := stateByID[id]
		if !ok || !state.Active || state.TrackedRevision != revisions[id] {
			return fmt.Errorf("%w: day-finalize user_id=%d revision=%d", errUsageMemberControlIntegrity, id, revisions[id])
		}
	}
	var generation, servingGeneration int64
	var factsChanged, servingChanged bool
	err = db.Transaction(func(tx *gorm.DB) error {
		verified, verifyErr := verifyUsageFactDayHours(tx, dayTs, ordered)
		if verifyErr != nil {
			return verifyErr
		}
		if !verified {
			return errors.New("日终 24 小时事实尚未完整")
		}
		epoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
		var epochProofs int64
		if err := tx.Model(&UsageFactMemberHourState{}).
			Where("user_id IN ? AND hour_ts >= ? AND hour_ts < ? AND status = ? AND content_hash <> '' AND source_epoch = ?",
				ordered, dayTs, dayTs+usageFactDaySeconds, "complete", epoch).
			Count(&epochProofs).Error; err != nil {
			return err
		}
		if epoch == "" || epochProofs != int64(len(ordered))*24 {
			return errors.New("日终 24 小时当前来源 epoch 证明尚未完整")
		}
		priorRows, loadErr := loadUsageDailyFacts(tx, dayTs, dayTs+usageFactDaySeconds, ordered)
		if loadErr != nil {
			return loadErr
		}
		priorByDay := usageDailyFactsByMemberDay(priorRows)
		// Hour rows are staging material in full-history mode. Only after the
		// independent source proof succeeds do we replace daily facts and
		// the strict proof together in this one SQLite transaction.
		if rebuildErr := m.rebuildUsageDailyFact(tx, dayTs, ordered); rebuildErr != nil {
			return rebuildErr
		}
		rows, loadErr := loadUsageDailyFacts(tx, dayTs, dayTs+usageFactDaySeconds, ordered)
		if loadErr != nil {
			return loadErr
		}
		rowsByDay := usageDailyFactsByMemberDay(rows)
		nowUnix := time.Now().Unix()
		for _, id := range ordered {
			key := usageFactMemberDayKey{userID: id, dayTs: dayTs}
			memberRows := rowsByDay[key]
			metrics := dailyFactsMetrics(memberRows)
			control := controls[key]
			if !usageFactMetricsEqual(metrics, control.metrics()) {
				return &usageFactHistoryMemberControlError{UserID: id, DayTs: dayTs,
					Detail: fmt.Sprintf("detail=%+v control=%+v", metrics, control.metrics())}
			}
			var prior UsageFactMemberDayState
			_ = tx.First(&prior, "user_id = ? AND date_ts = ?", id, dayTs).Error
			factHash := usageDailyFactContentHash(memberRows)
			if usageDailyFactContentHash(priorByDay[key]) != factHash {
				factsChanged = true
			}
			proof := UsageFactMemberDayState{
				UserID: id, DateTs: dayTs, Status: "complete", Rows: len(memberRows), SourceRows: control.SourceRows,
				Requests: control.Requests, RefundRecords: control.RefundRecords,
				Tokens:       control.PromptTokens + control.CompletionTokens,
				PromptTokens: control.PromptTokens, CompletionTokens: control.CompletionTokens,
				ConsumeQuota: control.ConsumeQuota, RefundQuota: control.RefundQuota,
				ContentHash: factHash, SourceResultHash: usageFactHistoryControlHash(control), FactContentHash: factHash,
				ClassificationVersion: userTrafficClassificationVersion,
				QuerySemanticsVersion: usageFactQuerySemanticsVersion,
				SourceEpoch:           strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch),
				SourceCheckedAt:       nowUnix, CompletedAt: nowUnix, JobID: jobID,
				Attempts: prior.Attempts + 1, UpdatedAt: nowUnix,
			}
			if err := tx.Save(&proof).Error; err != nil {
				return err
			}
		}
		var global UsageFactSyncState
		if err := tx.First(&global, 1).Error; err != nil {
			return err
		}
		if factsChanged {
			global.Generation++
			if dayTs >= global.PublishedRangeStart && dayTs < global.PublishedThrough {
				var published int64
				if err := tx.Model(&UsageFactPublishedMember{}).Where("user_id IN ?", ordered).Count(&published).Error; err != nil {
					return err
				}
				if published > 0 {
					global.ServingGeneration++
					servingChanged = true
				}
			}
		}
		generation, servingGeneration = global.Generation, global.ServingGeneration
		if err := tx.Save(&global).Error; err != nil {
			return err
		}
		after, currentErr := m.loadUsageMemberControlSnapshot(ctx)
		if currentErr != nil {
			return currentErr
		}
		if !usageMemberControlSnapshotsEqual(before, after) {
			return fmt.Errorf("%w: member manifest changed during day finalize", errUsageMemberControlIntegrity)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if factsChanged {
		m.publishUsageFactGenerations(generation, 0)
	}
	if servingChanged {
		m.publishUsageFactGenerations(0, servingGeneration)
	}
	return nil
}

func (m *Monitor) maybeFinalizeUsageFactHistoryDayFromHours(ctx context.Context, hourTs int64, ids []int64, strict bool) error {
	if !m.usageFactsFullHistoryEnabled() || hourTs <= 0 || len(ids) == 0 {
		return nil
	}
	next := hourTs + usageFactHourSeconds
	if usageFactDayStart(next) != next { // only the 23:00 -> 00:00 CST boundary
		return nil
	}
	snapshot, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return err
	}
	revisions := make(map[int64]int64, len(ids))
	epoch := strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch)
	for _, id := range ids {
		control, ok := snapshot.Controls[id]
		if !ok || !control.Active || control.TrackedRevision < 1 {
			return fmt.Errorf("%w: day-finalize user_id=%d missing active control", errUsageMemberControlIntegrity, id)
		}
		revisions[id] = control.TrackedRevision
	}
	dayTs := usageFactDayStart(hourTs)
	memberFailures := make(map[int64]error)
	for start := 0; start < len(ids); start += usageFactHistoryMaxMembers {
		end := start + usageFactHistoryMaxMembers
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[start:end]
		readiness, verifyErr := usageFactDayHourReadiness(m.usageFactsStore().WithContext(ctx), dayTs, batch, epoch)
		if verifyErr != nil {
			return verifyErr
		}
		readyBatch := make([]int64, 0, len(batch))
		for _, id := range batch {
			if readiness[id] {
				readyBatch = append(readyBatch, id)
				continue
			}
			if strict {
				memberFailures[id] = errors.New("日终 24 小时当前来源 epoch 证明尚未完整")
			}
		}
		if len(readyBatch) == 0 {
			continue
		}
		// 一条批量 control 查询先尝试签收全部本地健康成员。若错误已
		// 精确携带 user_id，只剔除该成员并对余下集合重试；一个坏成员
		// 最多增加一条 control SQL，不会放大成 N 条。
		pending := append([]int64(nil), readyBatch...)
		for len(pending) > 0 {
			batchRevisions := make(map[int64]int64, len(pending))
			for _, id := range pending {
				batchRevisions[id] = revisions[id]
			}
			finalizeErr := m.finalizeUsageFactHistoryDayFromHours(
				ctx, dayTs, pending, batchRevisions, "tail-day-"+strconv.FormatInt(dayTs, 36),
			)
			if finalizeErr == nil {
				break
			}
			var memberErr *usageFactHistoryMemberControlError
			if !errors.As(finalizeErr, &memberErr) || memberErr.UserID <= 0 {
				return finalizeErr
			}
			found := false
			nextPending := make([]int64, 0, len(pending)-1)
			for _, id := range pending {
				if id == memberErr.UserID {
					found = true
					memberFailures[id] = finalizeErr
					continue
				}
				nextPending = append(nextPending, id)
			}
			if !found {
				return finalizeErr
			}
			pending = nextPending
		}
	}
	if len(memberFailures) > 0 {
		return &usageFactHistoryFinalizeError{Failures: memberFailures}
	}
	return nil
}

// fetchUsageFactHistoryRange always runs both queries.  The detail query is
// bounded by dimensional rows; the control query is bounded by member-days.
// Empty member-days are materialized in memory after the successful control
// read, making zero usage distinguishable from an unqueried day without
// storing millions of empty hour proofs.
func (m *Monitor) fetchUsageFactHistoryRange(ctx context.Context, fromTs, throughTs int64, ids []int64) (usageFactHistoryRange, error) {
	ordered, err := validateUsageFactHistoryRange(fromTs, throughTs, ids)
	if err != nil {
		return usageFactHistoryRange{}, err
	}
	result := usageFactHistoryRange{
		FromTs: fromTs, ThroughTs: throughTs, UserIDs: ordered,
		Facts:    make(map[usageFactMemberDayKey][]UsageDailyFact),
		Controls: make(map[usageFactMemberDayKey]usageFactHistoryControl),
	}
	inSQL, inArgs := usageIn("user_id", ordered)
	detailQuery := usageFactHistoryDetailSQL(m.dayExpr(), inSQL, m.channelTestSourcePredicateSQL())
	detailArgs := append([]any{fromTs, throughTs}, inArgs...)
	detailArgs = append(detailArgs, usageFactHistoryMaxRows+1)
	var detailDuration time.Duration
	err = m.withUsageFactHistorySourceQuery(ctx, func(qctx context.Context) error {
		queryStarted := time.Now()
		defer func() { detailDuration = time.Since(queryStarted) }()
		rows, queryErr := m.prodDB.QueryContext(qctx, detailQuery, detailArgs...)
		if queryErr != nil {
			return fmt.Errorf("读取历史日维度聚合失败: %w", queryErr)
		}
		defer rows.Close()
		for rows.Next() {
			var dayIndex int64
			var row UsageDailyFact
			if scanErr := rows.Scan(
				&dayIndex, &row.UserID, &row.ChannelID, &row.Grp, &row.ModelName, &row.TokenID, &row.TokenName,
				&row.Requests, &row.RefundRecords, &row.PromptTokens, &row.CompletionTokens, &row.ConsumeQuota, &row.RefundQuota,
			); scanErr != nil {
				return scanErr
			}
			result.Rows++
			if result.Rows > usageFactHistoryMaxRows {
				return errUsageFactHistoryRangeTooLarge
			}
			row.DateTs = usageFactHistoryDayIndexToTs(dayIndex)
			if row.DateTs < fromTs || row.DateTs >= throughTs {
				return fmt.Errorf("历史日维度返回越界日期 %d", row.DateTs)
			}
			key := usageFactMemberDayKey{userID: row.UserID, dayTs: row.DateTs}
			result.Facts[key] = append(result.Facts[key], row)
		}
		return rows.Err()
	})
	result.SourceQueries++
	result.QueryDuration += detailDuration
	if err != nil {
		return usageFactHistoryRange{}, err
	}

	controlQuery := usageFactHistoryControlSQL(m.dayExpr(), inSQL, m.channelTestSourcePredicateSQL())
	controlArgs := append([]any{fromTs, throughTs}, inArgs...)
	maxMemberDays := len(ordered) * int((throughTs-fromTs)/usageFactDaySeconds)
	controlArgs = append(controlArgs, maxMemberDays+1)
	var controlDuration time.Duration
	err = m.withUsageFactHistorySourceQuery(ctx, func(qctx context.Context) error {
		queryStarted := time.Now()
		defer func() { controlDuration = time.Since(queryStarted) }()
		rows, queryErr := m.prodDB.QueryContext(qctx, controlQuery, controlArgs...)
		if queryErr != nil {
			return fmt.Errorf("读取历史日独立控制数失败: %w", queryErr)
		}
		defer rows.Close()
		seen := 0
		for rows.Next() {
			var dayIndex int64
			var control usageFactHistoryControl
			if scanErr := rows.Scan(
				&dayIndex, &control.UserID, &control.SourceRows, &control.Requests, &control.RefundRecords,
				&control.PromptTokens, &control.CompletionTokens, &control.ConsumeQuota, &control.RefundQuota,
			); scanErr != nil {
				return scanErr
			}
			seen++
			if seen > maxMemberDays {
				return errUsageFactHistoryRangeTooLarge
			}
			control.DateTs = usageFactHistoryDayIndexToTs(dayIndex)
			if control.DateTs < fromTs || control.DateTs >= throughTs {
				return fmt.Errorf("历史控制数返回越界日期 %d", control.DateTs)
			}
			key := usageFactMemberDayKey{userID: control.UserID, dayTs: control.DateTs}
			if _, duplicate := result.Controls[key]; duplicate {
				return fmt.Errorf("历史控制数重复 user=%d day=%d", control.UserID, control.DateTs)
			}
			result.Controls[key] = control
		}
		return rows.Err()
	})
	result.SourceQueries++
	result.QueryDuration += controlDuration
	if err != nil {
		return usageFactHistoryRange{}, err
	}

	if err := validateUsageFactHistoryResult(&result); err != nil {
		return usageFactHistoryRange{}, err
	}
	return result, nil
}

func validateUsageFactHistoryResult(result *usageFactHistoryRange) error {
	if result == nil {
		return errors.New("历史日聚合结果不能为空")
	}
	ordered := result.UserIDs
	fromTs, throughTs := result.FromTs, result.ThroughTs
	allowed := make(map[int64]bool, len(ordered))
	for _, id := range ordered {
		allowed[id] = true
	}
	for key := range result.Facts {
		if !allowed[key.userID] {
			return fmt.Errorf("历史维度返回未知成员 %d", key.userID)
		}
	}
	for key := range result.Controls {
		if !allowed[key.userID] {
			return fmt.Errorf("历史控制数返回未知成员 %d", key.userID)
		}
	}
	for day := fromTs; day < throughTs; day += usageFactDaySeconds {
		for _, id := range ordered {
			key := usageFactMemberDayKey{userID: id, dayTs: day}
			control, exists := result.Controls[key]
			if !exists {
				control = usageFactHistoryControl{UserID: id, DateTs: day}
				result.Controls[key] = control
			}
			if control.SourceRows != control.Requests+control.RefundRecords {
				return fmt.Errorf("%w: user=%d day=%d source_rows=%d records=%d",
					errUsageFactHistoryControl, id, day, control.SourceRows, control.Requests+control.RefundRecords)
			}
			factRows := result.Facts[key]
			metrics := dailyFactsMetrics(factRows)
			if !usageFactMetricsEqual(metrics, control.metrics()) {
				return fmt.Errorf("%w: user=%d day=%d detail=%+v control=%+v",
					errUsageFactHistoryControl, id, day, metrics, control.metrics())
			}
		}
	}
	for key := range result.Facts {
		sort.Slice(result.Facts[key], func(i, j int) bool {
			a, b := result.Facts[key][i], result.Facts[key][j]
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
	}
	return nil
}

func usageFactHistoryControlHash(control usageFactHistoryControl) string {
	// Decimal separators make the input portable across architectures and easy
	// to reproduce in recovery tooling without depending on Go struct layout.
	parts := []int64{
		control.UserID, control.DateTs, control.SourceRows, control.Requests,
		control.RefundRecords, control.PromptTokens, control.CompletionTokens,
		control.ConsumeQuota, control.RefundQuota,
	}
	var b strings.Builder
	for _, value := range parts {
		b.WriteString(strconv.FormatInt(value, 10))
		b.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func (m *Monitor) usageFactHistoryRevisionsCurrent(ctx context.Context, ids []int64, revisions map[int64]int64) (usageMemberControlSnapshot, error) {
	snapshot, err := m.loadUsageMemberControlSnapshot(ctx)
	if err != nil {
		return usageMemberControlSnapshot{}, err
	}
	for _, id := range ids {
		control, ok := snapshot.Controls[id]
		if !ok || !control.Active || revisions[id] < 1 || control.TrackedRevision != revisions[id] {
			return usageMemberControlSnapshot{}, fmt.Errorf("%w: user_id=%d history revision=%d",
				errUsageMemberControlIntegrity, id, revisions[id])
		}
	}
	return snapshot, nil
}

// commitUsageFactHistoryRange atomically replaces every member-day in a
// controlled source range.  Main-store revisions are checked before and after
// the facts transaction: a concurrent remove/rejoin may leave harmless staged
// facts, but it can never advance coverage or publication for the stale job.
func (m *Monitor) commitUsageFactHistoryRange(
	ctx context.Context,
	result usageFactHistoryRange,
	jobID string,
	revisions map[int64]int64,
) error {
	if err := validateUsageFactHistoryResult(&result); err != nil {
		return err
	}
	jobID = strings.TrimSpace(jobID)
	if jobID == "" || len(jobID) > 80 {
		return errors.New("历史补全 job_id 必须为 1～80 字节")
	}
	before, err := m.usageFactHistoryRevisionsCurrent(ctx, result.UserIDs, revisions)
	if err != nil {
		return err
	}
	// Tail/reconcile/repair also rebuild the same member-day rows. Serialize only
	// the local validation+commit section; source SQL has already completed and
	// must never hold this mutex while waiting on MySQL.
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()

	inSQL, inArgs := usageIn("user_id", result.UserIDs)
	var memberStates []UsageFactMemberState
	if err := m.usageFactsStore().WithContext(ctx).Where(inSQL, inArgs...).Find(&memberStates).Error; err != nil {
		return err
	}
	stateByID := make(map[int64]UsageFactMemberState, len(memberStates))
	for _, state := range memberStates {
		stateByID[state.UserID] = state
	}
	for _, id := range result.UserIDs {
		state, ok := stateByID[id]
		if !ok || !state.Active || state.TrackedRevision != revisions[id] {
			return fmt.Errorf("%w: facts mirror user_id=%d revision=%d",
				errUsageMemberControlIntegrity, id, state.TrackedRevision)
		}
	}

	now := time.Now().Unix()
	var generation, servingGeneration int64
	var servingChanged bool
	err = m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var global UsageFactSyncState
		if err := tx.First(&global, 1).Error; err != nil {
			return err
		}
		var published []UsageFactPublishedMember
		if err := tx.Find(&published).Error; err != nil {
			return err
		}
		publishedByID := make(map[int64]UsageFactPublishedMember, len(published))
		for _, row := range published {
			publishedByID[row.UserID] = row
		}
		var prior []UsageFactMemberDayState
		priorArgs := append([]any{result.FromTs, result.ThroughTs}, inArgs...)
		if err := tx.Where("date_ts >= ? AND date_ts < ? AND "+inSQL, priorArgs...).Find(&prior).Error; err != nil {
			return err
		}
		attempts := make(map[usageFactMemberDayKey]int, len(prior))
		priorByKey := make(map[usageFactMemberDayKey]UsageFactMemberDayState, len(prior))
		for _, row := range prior {
			key := usageFactMemberDayKey{userID: row.UserID, dayTs: row.DateTs}
			attempts[key] = row.Attempts
			priorByKey[key] = row
		}
		deleteArgs := append([]any{result.FromTs, result.ThroughTs}, inArgs...)
		if err := tx.Where("date_ts >= ? AND date_ts < ? AND "+inSQL, deleteArgs...).Delete(&UsageDailyFact{}).Error; err != nil {
			return err
		}
		if err := tx.Where("date_ts >= ? AND date_ts < ? AND "+inSQL, deleteArgs...).Delete(&UsageFactMemberDayState{}).Error; err != nil {
			return err
		}

		facts := make([]UsageDailyFact, 0, result.Rows)
		proofs := make([]UsageFactMemberDayState, 0, len(result.Controls))
		for day := result.FromTs; day < result.ThroughTs; day += usageFactDaySeconds {
			for _, id := range result.UserIDs {
				key := usageFactMemberDayKey{userID: id, dayTs: day}
				rows := result.Facts[key]
				facts = append(facts, rows...)
				control := result.Controls[key]
				factHash := usageDailyFactContentHash(rows)
				if publishedRow, visible := publishedByID[id]; visible &&
					(publishedRow.TrackedRevision == revisions[id] || (publishedRow.TrackedRevision == 0 && revisions[id] == 1)) &&
					day >= global.PublishedRangeStart && day < global.PublishedThrough {
					old, existed := priorByKey[key]
					if !existed || old.ContentHash != factHash || !usageFactMemberDayMetricsMatchState(dailyFactsMetrics(rows), old) {
						servingChanged = true
					}
				}
				proofs = append(proofs, UsageFactMemberDayState{
					UserID: id, DateTs: day, Status: "complete",
					Rows: len(rows), SourceRows: control.SourceRows,
					Requests: control.Requests, RefundRecords: control.RefundRecords,
					Tokens:       control.PromptTokens + control.CompletionTokens,
					PromptTokens: control.PromptTokens, CompletionTokens: control.CompletionTokens,
					ConsumeQuota: control.ConsumeQuota, RefundQuota: control.RefundQuota,
					ContentHash: factHash, SourceResultHash: usageFactHistoryControlHash(control), FactContentHash: factHash,
					ClassificationVersion: userTrafficClassificationVersion,
					QuerySemanticsVersion: usageFactQuerySemanticsVersion,
					SourceEpoch:           strings.TrimSpace(m.cfg.UsageFactsHistorySourceEpoch),
					SourceCheckedAt:       now, CompletedAt: now, JobID: jobID,
					Attempts: attempts[key] + 1, UpdatedAt: now,
				})
			}
		}
		if len(facts) > 0 {
			if err := tx.CreateInBatches(facts, 500).Error; err != nil {
				return err
			}
		}
		if err := tx.CreateInBatches(proofs, 500).Error; err != nil {
			return err
		}
		global.Generation++
		if servingChanged {
			global.ServingGeneration++
		}
		generation, servingGeneration = global.Generation, global.ServingGeneration
		return tx.Save(&global).Error
	})
	if err != nil {
		return err
	}
	m.publishUsageFactGenerations(generation, 0)
	if servingChanged {
		m.publishUsageFactGenerations(0, servingGeneration)
	}

	after, err := m.usageFactHistoryRevisionsCurrent(ctx, result.UserIDs, revisions)
	if err != nil {
		return err
	}
	if !usageMemberControlSnapshotsEqual(before, after) {
		return fmt.Errorf("%w: member manifest changed during history commit", errUsageMemberControlIntegrity)
	}
	return nil
}
