package monitor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

const financeGiftScopeSourceTimeout = 8 * time.Second

// This is an explicit, one-user/one-hour repair primitive, not a background
// collector. Callers must supply the original epoch's read-only source and a
// bounded context. It never advances the normal finance synchronization cursor.
type financeGiftScopeRepairResult struct {
	RowsChecked int
	RowsUpdated int
	ContentHash string
}

type financeGiftScopeSnapshot struct {
	State     FinanceGiftBoundaryState
	UserState FinanceUserHourState
	Events    []FinanceGiftBoundaryEvent
}

func loadFinanceGiftScopeSnapshot(ctx context.Context, db *gorm.DB, epoch string, hour, user int64) (financeGiftScopeSnapshot, error) {
	var result financeGiftScopeSnapshot
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&result.UserState, "hour_ts=?", hour).Error; err != nil {
			return err
		}
		if result.UserState.Status != "complete" || result.UserState.SourceEpoch != epoch || result.UserState.SemanticsVersion != financeUserFactSemanticsVersion {
			return errors.New("gift scope repair user hour source epoch or completeness mismatch")
		}
		if err := tx.First(&result.State, "source_epoch=? AND hour_ts=? AND user_id=?", epoch, hour, user).Error; err != nil {
			return err
		}
		if result.State.Status != "complete" || result.State.Rows < 0 || result.State.Rows > financeGiftBoundaryMaxRows {
			return errors.New("gift scope repair requires a complete bounded ordering ledger")
		}
		if err := tx.Where("source_epoch=? AND hour_ts=? AND user_id=?", epoch, hour, user).
			Order("event_at,source_log_id").Limit(financeGiftBoundaryMaxRows + 1).Find(&result.Events).Error; err != nil {
			return err
		}
		if int64(len(result.Events)) != result.State.Rows {
			return errors.New("gift scope repair existing row count mismatch")
		}
		for _, event := range result.Events {
			if event.EvidenceHash != financeGiftBoundaryEventHash(event) {
				return errors.New("gift scope repair existing evidence hash mismatch")
			}
		}
		if financeGiftBoundaryContentHash(result.Events) != result.State.ContentHash {
			return errors.New("gift scope repair existing ledger hash mismatch")
		}
		return nil
	})
	return result, err
}

func mergeFinanceGiftScopeEvidence(prior, fetched []FinanceGiftBoundaryEvent) ([]FinanceGiftBoundaryEvent, int, error) {
	if len(prior) != len(fetched) {
		return nil, 0, errors.New("gift scope repair source incomplete: row count changed")
	}
	byID := make(map[int64]FinanceGiftBoundaryEvent, len(fetched))
	for _, event := range fetched {
		if _, exists := byID[event.SourceLogID]; exists || !event.GroupKnown || event.EvidenceHash != financeGiftBoundaryEventHash(event) {
			return nil, 0, errors.New("gift scope repair source evidence invalid")
		}
		byID[event.SourceLogID] = event
	}
	merged := append([]FinanceGiftBoundaryEvent(nil), prior...)
	updated := 0
	for i, old := range prior {
		next, found := byID[old.SourceLogID]
		if !found || next.HourTs != old.HourTs || next.UserID != old.UserID || next.EventAt != old.EventAt || next.Kind != old.Kind || next.Quota != old.Quota {
			return nil, 0, errors.New("gift scope repair source monetary identity changed")
		}
		if old.GroupKnown {
			if old.Group != next.Group {
				return nil, 0, errors.New("gift scope repair cannot overwrite verified historical group")
			}
			continue
		}
		merged[i].Group, merged[i].GroupKnown = next.Group, true
		merged[i].EvidenceHash = financeGiftBoundaryEventHash(merged[i])
		updated++
	}
	return merged, updated, nil
}

func repairFinanceGiftBoundaryScope(ctx context.Context, db *gorm.DB, source financeSourceQuerier, epoch string, hour, user, now int64) (financeGiftScopeRepairResult, error) {
	var result financeGiftScopeRepairResult
	if db == nil || epoch == "" || strings.TrimSpace(epoch) != epoch || len(epoch) > 64 || hour < 0 || hour%3600 != 0 || user <= 0 || now <= hour+3600 {
		return result, errors.New("invalid gift scope repair target")
	}
	prior, err := loadFinanceGiftScopeSnapshot(ctx, db, epoch, hour, user)
	if err != nil {
		return result, fmt.Errorf("read gift scope repair target: %w", err)
	}
	result.RowsChecked, result.ContentHash = len(prior.Events), prior.State.ContentHash
	missing := false
	for _, event := range prior.Events {
		missing = missing || !event.GroupKnown
	}
	if !missing {
		return result, nil // A retry neither calls the source nor republishes timestamps.
	}
	if source == nil {
		return result, errors.New("gift scope repair original source unavailable")
	}
	// Never hold a local write transaction open while waiting for the source.
	queryCtx, cancel := context.WithTimeout(ctx, financeGiftScopeSourceTimeout)
	defer cancel()
	fetched, err := fetchFinanceGiftBoundaryEvents(queryCtx, source, hour, []int64{user})
	if err != nil {
		return result, fmt.Errorf("read gift scope repair source: %w", err)
	}
	merged, updated, err := mergeFinanceGiftScopeEvidence(prior.Events, fetched)
	if err != nil {
		return result, err
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		current, err := loadFinanceGiftScopeSnapshot(ctx, tx, epoch, hour, user)
		if err != nil {
			return err
		}
		if current.State != prior.State || current.UserState != prior.UserState {
			return errors.New("gift scope repair target changed; retry from a new snapshot")
		}
		// Existing publication also reconciles requests, refunds and quota with
		// the user-hour aggregate. Both detail and hash commit in one transaction.
		state, err := replaceFinanceGiftBoundaryUserHour(ctx, tx, hour, user, max(now, prior.State.UpdatedAt+1), epoch, merged)
		if err != nil {
			return err
		}
		result.ContentHash = state.ContentHash
		return nil
	})
	if err != nil {
		return financeGiftScopeRepairResult{}, err
	}
	result.RowsUpdated = updated
	return result, nil
}
