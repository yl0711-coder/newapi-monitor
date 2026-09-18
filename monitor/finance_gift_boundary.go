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

	"gorm.io/gorm"
)

const financeGiftBoundaryMaxRows = 50_000

// FinanceGiftBoundaryEvent keeps exact ordering for monetary activity by a
// user from the hour of their first eligible gift grant onward. It stores only
// monetary fields. Users who never received an eligible grant remain compact
// FinanceUserHourFact aggregates and never enter this detail table.
type FinanceGiftBoundaryEvent struct {
	SourceEpoch  string `gorm:"primaryKey;size:64;column:source_epoch"`
	SourceLogID  int64  `gorm:"primaryKey;autoIncrement:false;column:source_log_id"`
	HourTs       int64  `gorm:"index:idx_finance_gift_boundary_hour_user,priority:1;column:hour_ts"`
	UserID       int64  `gorm:"index:idx_finance_gift_boundary_hour_user,priority:2;column:user_id"`
	EventAt      int64  `gorm:"column:event_at"`
	Kind         string `gorm:"size:16;column:kind"`
	Quota        int64  `gorm:"column:quota"`
	EvidenceHash string `gorm:"size:64;column:evidence_hash"`
}

type FinanceGiftBoundaryState struct {
	SourceEpoch   string `gorm:"primaryKey;size:64;column:source_epoch"`
	HourTs        int64  `gorm:"primaryKey;autoIncrement:false;column:hour_ts"`
	UserID        int64  `gorm:"primaryKey;autoIncrement:false;column:user_id"`
	Status        string `gorm:"size:16;index;column:status"`
	Rows          int64  `gorm:"column:rows"`
	Requests      int64  `gorm:"column:requests"`
	RefundRecords int64  `gorm:"column:refund_records"`
	ConsumeQuota  int64  `gorm:"column:consume_quota"`
	RefundQuota   int64  `gorm:"column:refund_quota"`
	ContentHash   string `gorm:"size:64;column:content_hash"`
	CompletedAt   int64  `gorm:"column:completed_at"`
	UpdatedAt     int64  `gorm:"index;column:updated_at"`
}

func financeGiftBoundarySQL(userCount int) (string, error) {
	if userCount < 1 || userCount > financeCreditHourMaxRows {
		return "", errors.New("invalid gift boundary user count")
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", userCount), ",")
	return fmt.Sprintf(`SELECT id,user_id,created_at,type,quota
FROM logs WHERE created_at>=? AND created_at<? AND type IN (2,6)
  AND user_id IN (%s) AND NOT (%s)
ORDER BY created_at,id LIMIT %d`, placeholders, channelTestLogPredicateSQL(), financeGiftBoundaryMaxRows+1), nil
}

func fetchFinanceGiftBoundaryEvents(ctx context.Context, source financeSourceQuerier, hourTs int64, userIDs []int64) ([]FinanceGiftBoundaryEvent, error) {
	if source == nil || hourTs < 0 || hourTs%3600 != 0 {
		return nil, errors.New("invalid gift boundary source or hour")
	}
	ids := append([]int64(nil), userIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for index, id := range ids {
		if id <= 0 || (index > 0 && id == ids[index-1]) {
			return nil, errors.New("invalid gift boundary users")
		}
	}
	query, err := financeGiftBoundarySQL(len(ids))
	if err != nil {
		return nil, err
	}
	args := make([]any, 0, len(ids)+2)
	args = append(args, hourTs, hourTs+3600)
	allowed := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		args = append(args, id)
		allowed[id] = struct{}{}
	}
	rows, err := source.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := make([]FinanceGiftBoundaryEvent, 0, 64)
	for rows.Next() {
		if len(events) >= financeGiftBoundaryMaxRows {
			return nil, fmt.Errorf("gift boundary hour exceeds %d rows", financeGiftBoundaryMaxRows)
		}
		var event FinanceGiftBoundaryEvent
		var sourceType int
		if err := rows.Scan(&event.SourceLogID, &event.UserID, &event.EventAt, &sourceType, &event.Quota); err != nil {
			return nil, err
		}
		if event.SourceLogID <= 0 || event.EventAt < hourTs || event.EventAt >= hourTs+3600 || event.Quota < 0 {
			return nil, errors.New("invalid gift boundary source event")
		}
		if _, ok := allowed[event.UserID]; !ok {
			return nil, errors.New("gift boundary source returned another user")
		}
		event.HourTs = hourTs
		switch sourceType {
		case 2:
			event.Kind = "usage"
		case 6:
			event.Kind = "refund"
		default:
			return nil, errors.New("gift boundary source returned another event type")
		}
		event.EvidenceHash = financeGiftBoundaryEventHash(event)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

func financeGiftBoundaryEventHash(event FinanceGiftBoundaryEvent) string {
	values := []string{strconv.FormatInt(event.SourceLogID, 10), strconv.FormatInt(event.HourTs, 10),
		strconv.FormatInt(event.UserID, 10), strconv.FormatInt(event.EventAt, 10), event.Kind, strconv.FormatInt(event.Quota, 10)}
	sum := sha256.Sum256([]byte(strings.Join(values, "\x00")))
	return hex.EncodeToString(sum[:])
}

func financeGiftBoundaryContentHash(events []FinanceGiftBoundaryEvent) string {
	ordered := append([]FinanceGiftBoundaryEvent(nil), events...)
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

func replaceFinanceGiftBoundaryUserHour(ctx context.Context, db *gorm.DB, hourTs, userID, now int64, sourceEpoch string, input []FinanceGiftBoundaryEvent) (FinanceGiftBoundaryState, error) {
	var empty FinanceGiftBoundaryState
	sourceEpoch = strings.TrimSpace(sourceEpoch)
	if db == nil || hourTs < 0 || hourTs%3600 != 0 || userID <= 0 || now <= 0 || sourceEpoch == "" || len(sourceEpoch) > 64 {
		return empty, errors.New("invalid gift boundary publication")
	}
	events := append([]FinanceGiftBoundaryEvent(nil), input...)
	seen := make(map[int64]struct{}, len(events))
	state := FinanceGiftBoundaryState{SourceEpoch: sourceEpoch, HourTs: hourTs, UserID: userID, Status: "complete", CompletedAt: now, UpdatedAt: now}
	for i := range events {
		if events[i].SourceLogID <= 0 || events[i].HourTs != hourTs || events[i].UserID != userID || events[i].EventAt < hourTs || events[i].EventAt >= hourTs+3600 || events[i].Quota < 0 || (events[i].Kind != "usage" && events[i].Kind != "refund") {
			return empty, errors.New("invalid gift boundary event")
		}
		if _, exists := seen[events[i].SourceLogID]; exists {
			return empty, errors.New("duplicate gift boundary source log")
		}
		seen[events[i].SourceLogID] = struct{}{}
		events[i].SourceEpoch = sourceEpoch
		if events[i].EvidenceHash != financeGiftBoundaryEventHash(events[i]) {
			return empty, errors.New("gift boundary evidence hash mismatch")
		}
		state.Rows++
		if events[i].Kind == "usage" {
			state.Requests++
			if err := addEconomicsInt64(&state.ConsumeQuota, events[i].Quota); err != nil {
				return empty, err
			}
		} else {
			state.RefundRecords++
			if err := addEconomicsInt64(&state.RefundQuota, events[i].Quota); err != nil {
				return empty, err
			}
		}
	}
	state.ContentHash = financeGiftBoundaryContentHash(events)
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var aggregate FinanceUserHourFact
		query := tx.First(&aggregate, "hour_ts=? AND user_id=?", hourTs, userID)
		if errors.Is(query.Error, gorm.ErrRecordNotFound) {
			aggregate = FinanceUserHourFact{HourTs: hourTs, UserID: userID}
		} else if query.Error != nil {
			return query.Error
		}
		if aggregate.Requests != state.Requests || aggregate.RefundRecords != state.RefundRecords || aggregate.ConsumeQuota != state.ConsumeQuota || aggregate.RefundQuota != state.RefundQuota {
			return errors.New("gift boundary detail does not match finance user hour")
		}
		if err := tx.Where("source_epoch=? AND hour_ts=? AND user_id=?", sourceEpoch, hourTs, userID).Delete(&FinanceGiftBoundaryEvent{}).Error; err != nil {
			return err
		}
		if len(events) > 0 {
			if err := tx.CreateInBatches(events, 200).Error; err != nil {
				return err
			}
		}
		return tx.Save(&state).Error
	})
	if err != nil {
		return empty, err
	}
	return state, nil
}
