package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"
)

var errUpstreamBalanceSuperseded = errors.New("账户配置或余额状态已变化，本次旧余额结果未保存")

const upstreamBalanceLocalWriteTimeout = 5 * time.Second

// The per-account gate is held by the caller throughout request + persistence.
// A transactional snapshot guard additionally rejects configuration/credential
// changes or newer balance results, including changes made outside that gate.
// Usage progress is not a balance-owned field and must never be overwritten.
func (m *Monitor) persistUpstreamBalanceResult(ctx context.Context, observed ChannelUpstreamAccount, result *ChannelUpstreamAccount) error {
	ctx, cancel := context.WithTimeout(ctx, upstreamBalanceLocalWriteTimeout)
	defer cancel()
	base := *result // Already sealed once; a retry must not mutate this input.
	var committed ChannelUpstreamAccount
	attempts := 0
	started := time.Now()
	err := retryUpstreamLocalStore(ctx, func() error {
		attempts++
		next := base
		return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var current ChannelUpstreamAccount
			if err := tx.First(&current, "domain = ?", observed.Domain).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return errUpstreamBalanceSuperseded // Never resurrect a deleted account.
				}
				return err
			}
			if upstreamBalanceSnapshot(current) != upstreamBalanceSnapshot(observed) {
				return errUpstreamBalanceSuperseded
			}
			// A discovered unit change belongs to this observation, not the
			// account's older configuration-edit timestamp. UpdatedAt itself is
			// not written by a balance refresh.
			next.UpdatedAt = next.LastAttemptAt
			if err := reconcileUpstreamEconomicUnitTx(tx, current, &next); err != nil {
				return err
			}
			write := tx.Model(&current).UpdateColumns(upstreamBalanceColumns(next))
			if write.Error != nil {
				return write.Error
			}
			if write.RowsAffected != 1 {
				return errUpstreamBalanceSuperseded
			}
			// GORM applies the explicit map to current; unrelated fields retain
			// their freshly loaded values. Expose it only after COMMIT succeeds.
			committed = current
			return nil
		})
	})
	if attempts > 1 || err != nil {
		// Do not log the account/credential, SQL or raw database error.
		slog.Info("上游余额本地保存", "domain", observed.Domain, "attempts", attempts,
			"elapsed_ms", time.Since(started).Milliseconds(), "failed", err != nil)
	}
	if err != nil {
		return fmt.Errorf("保存上游余额本地结果失败: %w", err)
	}
	*result = committed
	return nil
}

// Use exact equality, including zero values and same-second credential changes.
// No usage cursors/statuses or UpdatedAt: GORM also advances UpdatedAt during
// operational updates, so that timestamp is not a configuration revision.
func upstreamBalanceSnapshot(row ChannelUpstreamAccount) ChannelUpstreamAccount {
	return ChannelUpstreamAccount{
		Domain: row.Domain, Provider: row.Provider, BaseURL: row.BaseURL,
		Account: row.Account, UserID: row.UserID, Enabled: row.Enabled,
		UsageSyncEnabled: row.UsageSyncEnabled,
		Credential:       row.Credential, CredentialVersion: row.CredentialVersion,
		CreatedAt: row.CreatedAt, UpdatedBy: row.UpdatedBy,
		BalanceUSD: row.BalanceUSD, BalanceRaw: row.BalanceRaw, BalanceKnown: row.BalanceKnown,
		BalanceUnit: row.BalanceUnit, BalanceUnitPrevious: row.BalanceUnitPrevious,
		BalanceUnitEffectiveAt: row.BalanceUnitEffectiveAt, UnitAssumed: row.UnitAssumed,
		Status: row.Status, LastError: row.LastError, ConsecutiveFails: row.ConsecutiveFails,
		LastAttemptAt: row.LastAttemptAt, LastSuccessAt: row.LastSuccessAt, NextSyncAt: row.NextSyncAt,
	}
}

func upstreamBalanceColumns(row ChannelUpstreamAccount) map[string]any {
	return map[string]any{
		"credential": row.Credential, "credential_version": row.CredentialVersion,
		"balance_usd": row.BalanceUSD, "balance_raw": row.BalanceRaw, "balance_known": row.BalanceKnown,
		"balance_unit": row.BalanceUnit, "balance_unit_previous": row.BalanceUnitPrevious,
		"balance_unit_effective_at": row.BalanceUnitEffectiveAt, "unit_assumed": row.UnitAssumed,
		"status": row.Status, "last_error": row.LastError, "consecutive_fails": row.ConsecutiveFails,
		"last_attempt_at": row.LastAttemptAt, "last_success_at": row.LastSuccessAt, "next_sync_at": row.NextSyncAt,
	}
}
