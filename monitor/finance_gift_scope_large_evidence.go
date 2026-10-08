//go:build unix

package monitor

import (
	"context"
	"errors"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
	"gorm.io/gorm"
)

// Pointer fields in the embedded evidence distinguish omitted/null values
// from legitimate zero quota and empty group, unlike the export writer type.
type financeGiftLargeEvidence struct {
	Version          int    `json:"version"`
	PlanSHA256       string `json:"plan_sha256"`
	SourceEpoch      string `json:"source_epoch"`
	LocalContentHash string `json:"local_content_hash"`
	financeGiftLocalEvidence
}

func validateFinanceGiftLargeEvidence(plan financegiftexport.LargeHourPlan, evidence financeGiftLargeEvidence) ([]FinanceGiftBoundaryEvent, error) {
	digest, err := financegiftexport.LargeHourConfirmation(plan)
	if err != nil {
		return nil, err
	}
	if evidence.Version != 1 || evidence.PlanSHA256 != digest || evidence.SourceEpoch != plan.SourceEpoch || evidence.LocalContentHash != plan.LocalContentHash ||
		evidence.UserID != plan.UserID || evidence.Hour != plan.HourTs || len(evidence.Rows) != len(plan.Rows) {
		return nil, errors.New("large-hour evidence differs from confirmed read plan")
	}
	events := make([]FinanceGiftBoundaryEvent, len(evidence.Rows))
	for i, row := range evidence.Rows {
		want := plan.Rows[i]
		if row.ID != want.ID || row.UserID != plan.UserID || row.CreatedAt != want.CreatedAt || row.Type != want.Type || row.Quota == nil || *row.Quota != want.Quota || row.Group == nil || (want.Group != nil && *row.Group != *want.Group) {
			return nil, errors.New("large-hour evidence monetary identity or known group mismatch")
		}
		kind := "usage"
		if row.Type == 6 {
			kind = "refund"
		}
		events[i] = FinanceGiftBoundaryEvent{SourceEpoch: plan.SourceEpoch, HourTs: plan.HourTs, UserID: plan.UserID, SourceLogID: row.ID, EventAt: row.CreatedAt, Kind: kind, Quota: *row.Quota, Group: *row.Group, GroupKnown: true}
		events[i].EvidenceHash = financeGiftBoundaryEventHash(events[i])
	}
	return events, nil
}

// The local transaction must accept either the exact old ledger or the exact
// fully repaired ledger. It cannot silently apply stale evidence to a third
// state, including a partially repaired or independently changed ledger.
func checkFinanceGiftLargeLedger(ctx context.Context, db *gorm.DB, plan financegiftexport.LargeHourPlan, fetched []FinanceGiftBoundaryEvent, allowCompleted bool) (financeGiftScopeSnapshot, []FinanceGiftBoundaryEvent, int, error) {
	prior, err := loadFinanceGiftScopeSnapshot(ctx, db, plan.SourceEpoch, plan.HourTs, plan.UserID)
	if err != nil {
		return prior, nil, 0, err
	}
	merged, updated, err := mergeFinanceGiftScopeEvidence(prior.Events, fetched)
	if err != nil {
		return prior, nil, 0, err
	}
	if prior.State.ContentHash != plan.LocalContentHash && !(allowCompleted && updated == 0 && prior.State.ContentHash == financeGiftBoundaryContentHash(fetched)) {
		return prior, nil, 0, errors.New("large-hour ledger changed; prepare a new plan")
	}
	// Validate aggregates even for an idempotent retry; a valid detail hash
	// alone must not hide subsequent changes to the financial facts.
	var fact FinanceUserHourFact
	if err := db.WithContext(ctx).First(&fact, "hour_ts=? AND user_id=?", plan.HourTs, plan.UserID).Error; err != nil {
		return prior, nil, 0, err
	}
	var requests, refunds, consumption, returned int64
	for _, event := range merged {
		if event.Kind == "usage" {
			requests++
			err = addEconomicsInt64(&consumption, event.Quota)
		} else {
			refunds++
			err = addEconomicsInt64(&returned, event.Quota)
		}
		if err != nil {
			return prior, nil, 0, err
		}
	}
	if fact.Requests != requests || fact.RefundRecords != refunds || fact.ConsumeQuota != consumption || fact.RefundQuota != returned ||
		prior.State.Requests != requests || prior.State.RefundRecords != refunds || prior.State.ConsumeQuota != consumption || prior.State.RefundQuota != returned {
		return prior, nil, 0, errors.New("large-hour ledger no longer reconciles with monetary facts")
	}
	return prior, merged, updated, nil
}
