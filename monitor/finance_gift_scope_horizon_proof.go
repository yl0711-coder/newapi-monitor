package monitor

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// A refund-free horizon must not rely on unverified aggregate rows. Verify
// compact user-hour facts against their complete content proofs, including
// empty hours and users other than gift recipients. This also detects a refund
// row deleted without updating its publication proof. No request details or
// writer transaction are read/held here.
func verifyFinanceGiftHorizonFacts(ctx context.Context, db *gorm.DB, from, to int64, states map[int64]FinanceUserHourState, selected []FinanceUserHourFact, users map[int64]int64) error {
	byKey := make(map[financeGiftUserHourKey]FinanceUserHourFact, len(selected))
	for _, fact := range selected {
		byKey[financeGiftUserHourKey{HourTs: fact.HourTs, UserID: fact.UserID}] = fact
	}
	rows, err := db.WithContext(ctx).Model(&FinanceUserHourFact{}).
		Select("hour_ts,user_id,requests,refund_records,consume_quota,refund_quota").
		Where("hour_ts>=? AND hour_ts<?", from, to).Order("hour_ts,user_id").Rows()
	if err != nil {
		return fmt.Errorf("read gift horizon user proofs: %w", err)
	}
	defer rows.Close()
	hour := from
	var facts []FinanceUserHourFact
	verify := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		actual, err := financeUserHourMetrics(facts)
		if err != nil {
			return err
		}
		expected, ok := states[hour]
		if !ok || actual.Rows != expected.Rows || actual.SourceRows != expected.SourceRows || actual.Requests != expected.Requests || actual.RefundRecords != expected.RefundRecords ||
			actual.ConsumeQuota != expected.ConsumeQuota || actual.RefundQuota != expected.RefundQuota || actual.UnattributedRecords != expected.UnattributedRecords || actual.ContentHash != expected.ContentHash {
			return financeFactChange("gift-horizon", "user-hour-proof")
		}
		facts = facts[:0]
		return nil
	}
	var matched int
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var fact FinanceUserHourFact
		if err := rows.Scan(&fact.HourTs, &fact.UserID, &fact.Requests, &fact.RefundRecords, &fact.ConsumeQuota, &fact.RefundQuota); err != nil {
			return err
		}
		for hour < fact.HourTs {
			if err := verify(); err != nil {
				return err
			}
			hour += 3600
		}
		if _, giftUser := users[fact.UserID]; giftUser {
			if prior, ok := byKey[financeGiftUserHourKey{HourTs: fact.HourTs, UserID: fact.UserID}]; !ok || prior != fact {
				return financeFactChange("gift-horizon", "selected-user-facts")
			}
			matched++
		}
		facts = append(facts, fact)
		if len(facts) > financeUserHourMaxRows {
			return financeFactChange("gift-horizon", "hour-row-limit")
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for hour < to {
		if err := verify(); err != nil {
			return err
		}
		hour += 3600
	}
	if matched != len(selected) {
		return financeFactChange("gift-horizon", "missing-user-facts")
	}
	return nil
}
