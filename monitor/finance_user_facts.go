package monitor

// This file defines the all-site, user-level finance fact boundary. It is
// intentionally separate from the customer portal's tracked-member facts:
// finance must not turn an authorization subset into a site-wide result.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

const (
	financeUserFactSemanticsVersion = 1
	financeUserHourMaxRows          = 10_000
)

// financeSourceQuerier is deliberately narrower than *sql.DB. It lets a
// complete finance hour be read through one read-only, repeatable-read
// transaction instead of combining independently timed source queries.
type financeSourceQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// FinanceUserHourFact stores only the monetary dimensions required to
// allocate registration gifts. It never stores prompts, request IDs, API
// keys, models, groups or channel details.
type FinanceUserHourFact struct {
	HourTs        int64 `gorm:"primaryKey;autoIncrement:false;index:idx_finance_user_hour,priority:1;column:hour_ts"`
	UserID        int64 `gorm:"primaryKey;autoIncrement:false;index:idx_finance_user_hour,priority:2;column:user_id"`
	Requests      int64 `gorm:"column:requests"`
	RefundRecords int64 `gorm:"column:refund_records"`
	ConsumeQuota  int64 `gorm:"column:consume_quota"`
	RefundQuota   int64 `gorm:"column:refund_quota"`
}

// FinanceUserHourState is the publication proof for one complete source
// hour. A fact row without the matching complete state is never publishable.
type FinanceUserHourState struct {
	HourTs              int64  `gorm:"primaryKey;autoIncrement:false;column:hour_ts"`
	SourceEpoch         string `gorm:"size:64;column:source_epoch"`
	SemanticsVersion    int    `gorm:"column:semantics_version"`
	Status              string `gorm:"size:16;index;column:status"`
	Rows                int    `gorm:"column:rows"`
	SourceRows          int64  `gorm:"column:source_rows"`
	Requests            int64  `gorm:"column:requests"`
	RefundRecords       int64  `gorm:"column:refund_records"`
	ConsumeQuota        int64  `gorm:"column:consume_quota"`
	RefundQuota         int64  `gorm:"column:refund_quota"`
	UnattributedRecords int64  `gorm:"column:unattributed_records"`
	ContentHash         string `gorm:"size:64;column:content_hash"`
	CompletedAt         int64  `gorm:"column:completed_at"`
	UpdatedAt           int64  `gorm:"index;column:updated_at"`
}

func financeUserHourSQL() string {
	testPredicate := channelTestLogPredicateSQL()
	return fmt.Sprintf(`
SELECT /*+ MAX_EXECUTION_TIME(8000) */ user_id,
  CAST(COALESCE(SUM(type=2),0) AS SIGNED) requests,
  CAST(COALESCE(SUM(type=6),0) AS SIGNED) refund_records,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN quota ELSE 0 END),0) AS SIGNED) consume_quota,
  CAST(COALESCE(SUM(CASE WHEN type=6 THEN quota ELSE 0 END),0) AS SIGNED) refund_quota,
  CAST(COUNT(*) AS SIGNED) source_rows
FROM logs
WHERE created_at >= ? AND created_at < ? AND type IN (2,6)
  AND NOT (%s)
GROUP BY user_id
LIMIT %d`, testPredicate, financeUserHourMaxRows+1)
}

// fetchFinanceUserHourFacts performs one bounded source query. A query error,
// row-cap sentinel or malformed aggregate fails the whole hour; callers must
// not publish a partial result.
func fetchFinanceUserHourFacts(ctx context.Context, source financeSourceQuerier, hourTs int64) ([]FinanceUserHourFact, int64, error) {
	if source == nil || hourTs < 0 || hourTs%3600 != 0 {
		return nil, 0, errors.New("invalid finance fact source or hour")
	}
	rows, err := source.QueryContext(ctx, financeUserHourSQL(), hourTs, hourTs+3600)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	facts := make([]FinanceUserHourFact, 0, 128)
	var sourceRows int64
	for rows.Next() {
		var fact FinanceUserHourFact
		var rowSourceRows int64
		if err := rows.Scan(&fact.UserID, &fact.Requests, &fact.RefundRecords, &fact.ConsumeQuota, &fact.RefundQuota, &rowSourceRows); err != nil {
			return nil, 0, err
		}
		if len(facts) >= financeUserHourMaxRows {
			return nil, 0, fmt.Errorf("finance user hour exceeds %d rows", financeUserHourMaxRows)
		}
		fact.HourTs = hourTs
		if err := validateFinanceUserHourFact(fact); err != nil {
			return nil, 0, err
		}
		if rowSourceRows != fact.Requests+fact.RefundRecords {
			return nil, 0, errors.New("finance source row count does not match classified records")
		}
		if err := addEconomicsInt64(&sourceRows, rowSourceRows); err != nil {
			return nil, 0, err
		}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return facts, sourceRows, nil
}

func validateFinanceUserHourFact(fact FinanceUserHourFact) error {
	if fact.HourTs < 0 || fact.HourTs%3600 != 0 || fact.UserID < 0 || fact.Requests < 0 || fact.RefundRecords < 0 || fact.ConsumeQuota < 0 || fact.RefundQuota < 0 {
		return errors.New("invalid finance user hour fact")
	}
	if fact.Requests == 0 && fact.ConsumeQuota != 0 {
		return errors.New("finance consumption exists without a request")
	}
	if fact.RefundRecords == 0 && fact.RefundQuota != 0 {
		return errors.New("finance refund exists without a refund record")
	}
	return nil
}

func financeUserHourMetrics(rows []FinanceUserHourFact) (state FinanceUserHourState, err error) {
	seen := make(map[int64]struct{}, len(rows))
	for _, row := range rows {
		if err := validateFinanceUserHourFact(row); err != nil {
			return state, err
		}
		if _, exists := seen[row.UserID]; exists {
			return state, fmt.Errorf("duplicate finance user %d in hour %d", row.UserID, row.HourTs)
		}
		seen[row.UserID] = struct{}{}
		state.Rows++
		if err := addEconomicsInt64(&state.Requests, row.Requests); err != nil {
			return state, err
		}
		if err := addEconomicsInt64(&state.RefundRecords, row.RefundRecords); err != nil {
			return state, err
		}
		if err := addEconomicsInt64(&state.ConsumeQuota, row.ConsumeQuota); err != nil {
			return state, err
		}
		if err := addEconomicsInt64(&state.RefundQuota, row.RefundQuota); err != nil {
			return state, err
		}
		if row.UserID == 0 {
			if err := addEconomicsInt64(&state.UnattributedRecords, row.Requests+row.RefundRecords); err != nil {
				return state, err
			}
		}
	}
	state.SourceRows = state.Requests + state.RefundRecords
	state.ContentHash = financeUserHourContentHash(rows)
	return state, nil
}

func financeUserHourContentHash(rows []FinanceUserHourFact) string {
	ordered := append([]FinanceUserHourFact(nil), rows...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].HourTs != ordered[j].HourTs {
			return ordered[i].HourTs < ordered[j].HourTs
		}
		return ordered[i].UserID < ordered[j].UserID
	})
	hash := sha256.New()
	buffer := make([]byte, 0, 128)
	for _, row := range ordered {
		buffer = buffer[:0]
		for _, value := range []int64{row.HourTs, row.UserID, row.Requests, row.RefundRecords, row.ConsumeQuota, row.RefundQuota} {
			buffer = strconv.AppendInt(buffer, value, 10)
			buffer = append(buffer, 0)
		}
		_, _ = hash.Write(buffer)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// replaceFinanceUserHourFacts atomically replaces an hour and publishes its
// proof. Replays are idempotent; changed source content replaces the complete
// hour instead of being added to it.
func replaceFinanceUserHourFacts(ctx context.Context, db *gorm.DB, hourTs int64, sourceEpoch string, rows []FinanceUserHourFact, now int64) (FinanceUserHourState, error) {
	var empty FinanceUserHourState
	sourceEpoch = strings.TrimSpace(sourceEpoch)
	if db == nil || hourTs < 0 || hourTs%3600 != 0 || sourceEpoch == "" || len(sourceEpoch) > 64 || now <= 0 {
		return empty, errors.New("invalid finance hour publication")
	}
	for i := range rows {
		if rows[i].HourTs != hourTs {
			return empty, errors.New("finance fact belongs to another hour")
		}
	}
	state, err := financeUserHourMetrics(rows)
	if err != nil {
		return empty, err
	}
	state.HourTs = hourTs
	state.SourceEpoch = sourceEpoch
	state.SemanticsVersion = financeUserFactSemanticsVersion
	state.Status = "complete"
	state.CompletedAt = now
	state.UpdatedAt = now
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("hour_ts = ?", hourTs).Delete(&FinanceUserHourFact{}).Error; err != nil {
			return err
		}
		if len(rows) > 0 {
			if err := tx.CreateInBatches(rows, 200).Error; err != nil {
				return err
			}
		}
		var local struct {
			Rows          int64
			Requests      int64
			RefundRecords int64
			ConsumeQuota  int64
			RefundQuota   int64
		}
		if err := tx.Raw(`SELECT COUNT(*) rows,COALESCE(SUM(requests),0) requests,
			COALESCE(SUM(refund_records),0) refund_records,COALESCE(SUM(consume_quota),0) consume_quota,
			COALESCE(SUM(refund_quota),0) refund_quota FROM finance_user_hour_facts WHERE hour_ts=?`, hourTs).Scan(&local).Error; err != nil {
			return err
		}
		if local.Rows != int64(state.Rows) || local.Requests != state.Requests || local.RefundRecords != state.RefundRecords || local.ConsumeQuota != state.ConsumeQuota || local.RefundQuota != state.RefundQuota {
			return errors.New("finance local fact verification failed")
		}
		return tx.Save(&state).Error
	})
	if err != nil {
		return empty, err
	}
	return state, nil
}
