package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/yl0711-coder/newapi-monitor/internal/financecredit"
	"gorm.io/gorm"
)

const (
	financeCreditFactSemanticsVersion = 1
	financeCreditHourMaxRows          = 1_000
)

// FinanceCreditEvent is a normalized audit fact. Raw management content and
// the structured producer payload are deliberately not copied into Monitor.
type FinanceCreditEvent struct {
	SourceEpoch    string `gorm:"primaryKey;size:64;column:source_epoch"`
	SourceLogID    int64  `gorm:"primaryKey;autoIncrement:false;column:source_log_id"`
	EventAt        int64  `gorm:"index:idx_finance_credit_event_at;column:event_at"`
	TargetUserID   int64  `gorm:"index:idx_finance_credit_user_at,priority:1;column:target_user_id"`
	UserCreatedAt  int64  `gorm:"column:user_created_at"`
	Action         string `gorm:"size:24;column:action"`
	Unit           string `gorm:"size:32;column:unit"`
	ValueMicro     int64  `gorm:"column:value_micro"`
	BeforeMicro    int64  `gorm:"column:before_micro"`
	AfterMicro     int64  `gorm:"column:after_micro"`
	NetChangeMicro int64  `gorm:"column:net_change_micro"`
	Cohort         string `gorm:"size:48;column:cohort"`
	EligibleTrial  bool   `gorm:"index;column:eligible_trial"`
	EvidenceHash   string `gorm:"size:64;column:evidence_hash"`
}

type FinanceCreditHourState struct {
	HourTs                  int64  `gorm:"primaryKey;autoIncrement:false;column:hour_ts"`
	SourceEpoch             string `gorm:"size:64;column:source_epoch"`
	SemanticsVersion        int    `gorm:"column:semantics_version"`
	Status                  string `gorm:"size:16;index;column:status"`
	SourceRows              int64  `gorm:"column:source_rows"`
	EventRows               int64  `gorm:"column:event_rows"`
	EligibleTrialRows       int64  `gorm:"column:eligible_trial_rows"`
	IgnoredRows             int64  `gorm:"column:ignored_rows"`
	UnknownRegistrationRows int64  `gorm:"column:unknown_registration_rows"`
	ContentHash             string `gorm:"size:64;column:content_hash"`
	CompletedAt             int64  `gorm:"column:completed_at"`
	UpdatedAt               int64  `gorm:"index;column:updated_at"`
}

type financeCreditHourFetch struct {
	Events                  []FinanceCreditEvent
	SourceRows              int64
	IgnoredRows             int64
	UnknownRegistrationRows int64
}

type financeCreditSourceRow struct {
	ID        int64
	UserID    int64
	CreatedAt int64
	Content   string
	Other     string
}

func financeCreditHourSQL() string {
	return fmt.Sprintf(`SELECT id,user_id,created_at,COALESCE(content,''),COALESCE(other,'')
FROM logs WHERE created_at>=? AND created_at<? AND type=3
ORDER BY created_at,id LIMIT %d`, financeCreditHourMaxRows+1)
}

func fetchFinanceCreditHour(ctx context.Context, source financeSourceQuerier, hourTs int64) (financeCreditHourFetch, error) {
	var result financeCreditHourFetch
	if source == nil || hourTs < 0 || hourTs%3600 != 0 {
		return result, errors.New("invalid finance credit source or hour")
	}
	rows, err := source.QueryContext(ctx, financeCreditHourSQL(), hourTs, hourTs+3600)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	sourceRows := make([]financeCreditSourceRow, 0, 32)
	for rows.Next() {
		if len(sourceRows) >= financeCreditHourMaxRows {
			return result, fmt.Errorf("finance credit hour exceeds %d rows", financeCreditHourMaxRows)
		}
		var row financeCreditSourceRow
		if err := rows.Scan(&row.ID, &row.UserID, &row.CreatedAt, &row.Content, &row.Other); err != nil {
			return result, err
		}
		if row.ID <= 0 || row.CreatedAt < hourTs || row.CreatedAt >= hourTs+3600 {
			return result, errors.New("invalid finance credit source identity")
		}
		sourceRows = append(sourceRows, row)
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	result.SourceRows = int64(len(sourceRows))

	targets := make(map[int64]struct{})
	type parsedRow struct {
		source     financeCreditSourceRow
		adjustment financecredit.Adjustment
		target     int64
	}
	parsed := make([]parsedRow, 0, len(sourceRows))
	for _, row := range sourceRows {
		structured, structuredOK := financecredit.ParseStructuredAdjustment(row.Other)
		adjustment, target, ok := structured.Adjustment, structured.TargetUserID, structuredOK
		if !ok {
			// A recognized structured quota action is authoritative. Falling
			// back to legacy content here could mistake the operator user_id for
			// the actual target after a producer schema change.
			if financecredit.PotentialAdjustment("", row.Other) {
				return result, fmt.Errorf("unparsed structured quota adjustment at source log %d", row.ID)
			}
			adjustment, ok = financecredit.ParseLegacyAdjustment(row.Content)
			target = row.UserID
		}
		if !ok {
			if financecredit.PotentialAdjustment(row.Content, row.Other) {
				return result, fmt.Errorf("unparsed potential quota adjustment at source log %d", row.ID)
			}
			result.IgnoredRows++
			continue
		}
		if target <= 0 {
			return result, fmt.Errorf("quota adjustment %d has no target user", row.ID)
		}
		targets[target] = struct{}{}
		parsed = append(parsed, parsedRow{source: row, adjustment: adjustment, target: target})
	}
	createdAt, err := financeCreditUserCreatedAt(ctx, source, targets)
	if err != nil {
		return result, err
	}
	result.Events = make([]FinanceCreditEvent, 0, len(parsed))
	for _, item := range parsed {
		created := createdAt[item.target]
		cohort := financecredit.Cohort(item.adjustment, created, item.source.CreatedAt)
		net := item.adjustment.NetChangeMicro()
		trialSized := item.adjustment.Unit == financecredit.UnitUSD && net >= 100_000_000 && net <= 200_000_000
		eligible := cohort == "trial_candidate_within_24h" || cohort == "trial_candidate_within_7d"
		if trialSized && created == 0 {
			result.UnknownRegistrationRows++
		}
		event := FinanceCreditEvent{
			SourceLogID: item.source.ID, EventAt: item.source.CreatedAt, TargetUserID: item.target, UserCreatedAt: created,
			Action: item.adjustment.Action, Unit: item.adjustment.Unit, ValueMicro: item.adjustment.ValueMicro,
			BeforeMicro: item.adjustment.BeforeMicro, AfterMicro: item.adjustment.AfterMicro, NetChangeMicro: net,
			Cohort: cohort, EligibleTrial: eligible,
		}
		event.EvidenceHash = financeCreditEventHash(event)
		result.Events = append(result.Events, event)
	}
	return result, nil
}

func financeCreditUserCreatedAt(ctx context.Context, source financeSourceQuerier, targets map[int64]struct{}) (map[int64]int64, error) {
	result := make(map[int64]int64, len(targets))
	if len(targets) == 0 {
		return result, nil
	}
	ids := make([]int64, 0, len(targets))
	for id := range targets {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := source.QueryContext(ctx, "SELECT id,created_at FROM users WHERE id IN ("+placeholders+")", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, createdAt int64
		if err := rows.Scan(&id, &createdAt); err != nil {
			return nil, err
		}
		if id <= 0 || createdAt <= 0 {
			continue
		}
		result[id] = createdAt
	}
	return result, rows.Err()
}

func financeCreditEventHash(event FinanceCreditEvent) string {
	values := []string{
		strconv.FormatInt(event.SourceLogID, 10), strconv.FormatInt(event.EventAt, 10), strconv.FormatInt(event.TargetUserID, 10),
		strconv.FormatInt(event.UserCreatedAt, 10), event.Action, event.Unit, strconv.FormatInt(event.ValueMicro, 10),
		strconv.FormatInt(event.BeforeMicro, 10), strconv.FormatInt(event.AfterMicro, 10), strconv.FormatInt(event.NetChangeMicro, 10),
		event.Cohort, strconv.FormatBool(event.EligibleTrial),
	}
	sum := sha256.Sum256([]byte(strings.Join(values, "\x00")))
	return hex.EncodeToString(sum[:])
}

func financeCreditHourContentHash(events []FinanceCreditEvent) string {
	ordered := append([]FinanceCreditEvent(nil), events...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].EventAt != ordered[j].EventAt {
			return ordered[i].EventAt < ordered[j].EventAt
		}
		return ordered[i].SourceLogID < ordered[j].SourceLogID
	})
	hash := sha256.New()
	for _, event := range ordered {
		_, _ = hash.Write([]byte(event.EvidenceHash))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func replaceFinanceCreditHour(ctx context.Context, db *gorm.DB, hourTs, now int64, sourceEpoch string, fetched financeCreditHourFetch) (FinanceCreditHourState, error) {
	var empty FinanceCreditHourState
	sourceEpoch = strings.TrimSpace(sourceEpoch)
	if db == nil || hourTs < 0 || hourTs%3600 != 0 || now <= 0 || sourceEpoch == "" || len(sourceEpoch) > 64 {
		return empty, errors.New("invalid finance credit publication")
	}
	if fetched.SourceRows != int64(len(fetched.Events))+fetched.IgnoredRows || fetched.UnknownRegistrationRows < 0 || fetched.UnknownRegistrationRows > int64(len(fetched.Events)) {
		return empty, errors.New("finance credit source counts do not reconcile")
	}
	events := append([]FinanceCreditEvent(nil), fetched.Events...)
	seen := make(map[int64]struct{}, len(events))
	var eligible int64
	for i := range events {
		if events[i].SourceLogID <= 0 || events[i].EventAt < hourTs || events[i].EventAt >= hourTs+3600 || events[i].TargetUserID <= 0 {
			return empty, errors.New("invalid finance credit event")
		}
		if _, exists := seen[events[i].SourceLogID]; exists {
			return empty, errors.New("duplicate finance credit source log")
		}
		seen[events[i].SourceLogID] = struct{}{}
		events[i].SourceEpoch = sourceEpoch
		if events[i].EvidenceHash != financeCreditEventHash(events[i]) {
			return empty, errors.New("finance credit event hash mismatch")
		}
		if events[i].EligibleTrial {
			eligible++
		}
	}
	status := "complete"
	if fetched.UnknownRegistrationRows > 0 {
		status = "incomplete"
	}
	state := FinanceCreditHourState{
		HourTs: hourTs, SourceEpoch: sourceEpoch, SemanticsVersion: financeCreditFactSemanticsVersion, Status: status,
		SourceRows: fetched.SourceRows, EventRows: int64(len(events)), EligibleTrialRows: eligible,
		IgnoredRows: fetched.IgnoredRows, UnknownRegistrationRows: fetched.UnknownRegistrationRows,
		ContentHash: financeCreditHourContentHash(events), CompletedAt: now, UpdatedAt: now,
	}
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("source_epoch = ? AND event_at >= ? AND event_at < ?", sourceEpoch, hourTs, hourTs+3600).Delete(&FinanceCreditEvent{}).Error; err != nil {
			return err
		}
		if len(events) > 0 {
			if err := tx.CreateInBatches(events, 100).Error; err != nil {
				return err
			}
		}
		var local struct {
			Rows     int64
			Eligible int64
		}
		if err := tx.Raw(`SELECT COUNT(*) rows,COALESCE(SUM(CASE WHEN eligible_trial THEN 1 ELSE 0 END),0) eligible
			FROM finance_credit_events WHERE source_epoch=? AND event_at>=? AND event_at<?`, sourceEpoch, hourTs, hourTs+3600).Scan(&local).Error; err != nil {
			return err
		}
		if local.Rows != state.EventRows || local.Eligible != state.EligibleTrialRows {
			return errors.New("finance credit local verification failed")
		}
		return tx.Save(&state).Error
	})
	if err != nil {
		return empty, err
	}
	return state, nil
}
