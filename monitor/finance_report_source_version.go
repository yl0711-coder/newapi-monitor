package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"gorm.io/gorm"
)

// financeReportSourceFingerprint is deliberately built from local publication
// ledgers and sync-control rows. It never reads production NewAPI, an upstream
// provider, or AWS. The aggregate queries are substantially cheaper than
// rebuilding the report while still changing whenever a published finance
// input, its coverage proof, or its configuration changes.
func (m *Monitor) financeReportSourceFingerprint(ctx context.Context, from, to int64) (string, error) {
	return m.financeReportSourceFingerprintForScope(ctx, from, to, true)
}

func (m *Monitor) financeReportPeriodSourceFingerprint(ctx context.Context, from, to int64) (string, error) {
	return m.financeReportSourceFingerprintForScope(ctx, from, to, false)
}

func (m *Monitor) financeReportSourceFingerprintForScope(ctx context.Context, from, to int64, includeCUR bool) (string, error) {
	if m == nil || m.storeDB == nil {
		return "", fmt.Errorf("经营核算主库不可用")
	}
	if from < 0 || to <= from {
		return "", fmt.Errorf("经营核算事实版本区间无效")
	}
	hash := sha256.New()
	writeAggregate := func(db *gorm.DB, label, query string, args ...any) error {
		var row financeReportSourceAggregate
		if err := db.WithContext(ctx).Raw(query, args...).Scan(&row).Error; err != nil {
			return fmt.Errorf("读取%s版本: %w", label, err)
		}
		_, _ = fmt.Fprintf(hash, "%s|%d|%d|%d|%d|%d|%d|%d|%d\n", label,
			row.Rows, row.MaxUpdated, row.MetricA, row.MetricB, row.MetricC,
			row.MetricD, row.MetricE, row.MetricF)
		return nil
	}
	var accountRows []struct {
		Domain                 string
		Provider               string
		BaseURL                string
		UserID                 int64
		Enabled                bool
		UsageSyncEnabled       bool
		UsageAdapter           string
		UsageTailMode          string
		BalanceUnit            float64
		BalanceUnitPrevious    float64
		BalanceUnitEffectiveAt int64
		CredentialVersion      int
	}
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT domain,provider,base_url,user_id,enabled,
		usage_sync_enabled,usage_adapter,usage_tail_mode,balance_unit,balance_unit_previous,
		balance_unit_effective_at,credential_version
		FROM channel_upstream_accounts ORDER BY domain`).Scan(&accountRows).Error; err != nil {
		return "", fmt.Errorf("读取上游账户配置版本: %w", err)
	}
	for _, row := range accountRows {
		_, _ = fmt.Fprintf(hash, "upstream-account|%q|%q|%q|%d|%t|%t|%q|%q|%.12g|%.12g|%d|%d\n",
			row.Domain, row.Provider, row.BaseURL, row.UserID, row.Enabled, row.UsageSyncEnabled,
			row.UsageAdapter, row.UsageTailMode, row.BalanceUnit, row.BalanceUnitPrevious,
			row.BalanceUnitEffectiveAt, row.CredentialVersion)
	}
	var channelRows []struct {
		ID           int
		BaseDomain   string
		Status       int
		Groups       string
		Models       string
		EnabledSince int64
		DeletedAt    int64
	}
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT id,base_domain,status,groups,models,enabled_since,deleted_at
		FROM channel_snaps ORDER BY id`).Scan(&channelRows).Error; err != nil {
		return "", fmt.Errorf("读取渠道目录版本: %w", err)
	}
	for _, row := range channelRows {
		if includeCUR {
			_, _ = fmt.Fprintf(hash, "channel|%d|%q|%d|%q|%q|%d|%d\n",
				row.ID, row.BaseDomain, row.Status, row.Groups, row.Models, row.EnabledSince, row.DeletedAt)
		} else {
			// Monthly accounting only resolves immutable channel IDs to domains.
			// Runtime enable/disable changes must not invalidate every closed month.
			_, _ = fmt.Fprintf(hash, "channel-domain|%d|%q\n", row.ID, row.BaseDomain)
		}
	}

	mainAggregates := []struct {
		label string
		query string
		args  []any
	}{
		{
			label: "stability-hour-ledger",
			query: `SELECT COUNT(*) rows, COALESCE(MAX(updated_at),0) max_updated,
				COALESCE(SUM(rows),0) metric_a, COALESCE(SUM(requests),0) metric_b,
				COALESCE(SUM(tokens),0) metric_c, COALESCE(SUM(quota),0) metric_d,
				COALESCE(SUM(internal_test_requests),0) metric_e,
				COALESCE(SUM(internal_test_quota),0) metric_f
				FROM stability_hour_ingest_states WHERE hour_ts>=? AND hour_ts<?`,
			args: []any{from, to},
		},
		{
			label: "upstream-usage-ledger",
			query: `SELECT COUNT(*) rows, COALESCE(MAX(fetched_at),0) max_updated,
				COALESCE(SUM(requests),0) metric_a, COALESCE(SUM(tokens),0) metric_b,
				COALESCE(SUM(CAST(ROUND(cost_usd * 1000000) AS INTEGER)),0) metric_c,
				COALESCE(SUM(CAST(ROUND(quota * 1000) AS INTEGER)),0) metric_d,
				COALESCE(SUM(bucket_seconds),0) metric_e,
				COALESCE(SUM(CASE WHEN provisional THEN 1 ELSE 0 END),0) metric_f
				FROM channel_upstream_usage_hours WHERE hour_ts>=? AND hour_ts<?`,
			args: []any{from, to},
		},
		{
			label: "economics-manifest-ledger",
			query: `SELECT COUNT(*) rows, COALESCE(MAX(updated_at),0) max_updated,
				COALESCE(SUM(revision),0) metric_a,
				COALESCE(SUM(semantics_version),0) metric_b,
				0 metric_c, 0 metric_d, 0 metric_e, 0 metric_f
				FROM channel_economics_hour_manifest_current WHERE hour_ts>=? AND hour_ts<?`,
			args: []any{from, to},
		},
		{
			label: "economics-global-ledger",
			query: `SELECT COUNT(*) rows, COALESCE(MAX(updated_at),0) max_updated,
				COALESCE(SUM(unallocated_refund_records),0) metric_a,
				COALESCE(SUM(unallocated_refund_quota),0) metric_b,
				COALESCE(SUM(semantics_version),0) metric_c,
				0 metric_d, 0 metric_e, 0 metric_f
				FROM channel_economics_global_hour_facts WHERE hour_ts>=? AND hour_ts<?`,
			args: []any{from, to},
		},
		{
			label: "finance-version-ledger",
			query: `SELECT COUNT(*) rows, COALESCE(MAX(created_at),0) max_updated,
				COALESCE(MAX(id),0) metric_a, COALESCE(SUM(version),0) metric_b,
				COALESCE(SUM(effective_at),0) metric_c,
				COALESCE(SUM(LENGTH(snapshot_json)),0) metric_d,
				0 metric_e, 0 metric_f
				FROM channel_finance_versions WHERE effective_at<?`,
			args: []any{to},
		},
	}
	for _, aggregate := range mainAggregates {
		if err := writeAggregate(m.storeDB, aggregate.label, aggregate.query, aggregate.args...); err != nil {
			return "", err
		}
	}

	factsDB := m.usageFactsStore()
	if factsDB == nil {
		_, _ = hash.Write([]byte("usage-facts|unavailable\n"))
	} else {
		// Hash the small internal-account ledger by primary key, not just totals:
		// a correction can move cost between channels without changing any sum.
		var internalState FinanceInternalAccountFactState
		if err := factsDB.WithContext(ctx).Limit(1).Find(&internalState).Error; err != nil {
			return "", fmt.Errorf("读取内部账号事实版本: %w", err)
		}
		stateStatus := internalState.Status
		if internalState.FailureStreak > 0 {
			// The report treats any current sync failure as incomplete even when
			// the persisted status has not changed yet. Cache validity must agree.
			stateStatus = "error"
		} else if (stateStatus == "caught_up" || stateStatus == "backfilling") && internalState.NextHourTs >= to {
			stateStatus = "range_complete"
		}
		_, _ = fmt.Fprintf(hash, "internal-state|%q|%q|%d|%d|%s\n",
			internalState.ConfigHash, internalState.SourceEpoch, internalState.StartHourTs,
			min(internalState.NextHourTs, to), stateStatus)
		rows, err := factsDB.WithContext(ctx).Model(&FinanceInternalAccountHourFact{}).
			Where("hour_ts>=? AND hour_ts<?", from, to).Order("hour_ts,user_id,channel_id,grp").Rows()
		if err != nil {
			return "", fmt.Errorf("读取内部账号金额版本: %w", err)
		}
		encoder := json.NewEncoder(hash)
		for rows.Next() {
			var row FinanceInternalAccountHourFact
			if err := factsDB.ScanRows(rows, &row); err != nil {
				_ = rows.Close()
				return "", err
			}
			if err := encoder.Encode(row); err != nil {
				_ = rows.Close()
				return "", err
			}
		}
		rowsErr := rows.Err()
		_ = rows.Close()
		if rowsErr != nil {
			return "", rowsErr
		}
		factsAggregates := []struct {
			label string
			query string
			args  []any
		}{
			{
				label: "finance-user-hours",
				query: `SELECT COUNT(*) rows, COALESCE(MAX(updated_at),0) max_updated,
					COALESCE(SUM(rows),0) metric_a, COALESCE(SUM(requests),0) metric_b,
					COALESCE(SUM(consume_quota),0) metric_c, COALESCE(SUM(refund_quota),0) metric_d,
					COALESCE(SUM(refund_records),0) metric_e,
					COALESCE(SUM(unattributed_records),0) metric_f
					FROM finance_user_hour_states WHERE hour_ts>=? AND hour_ts<?`,
				args: []any{from, to},
			},
			{
				label: "finance-credit-hours",
				query: `SELECT COUNT(*) rows, COALESCE(MAX(updated_at),0) max_updated,
					COALESCE(SUM(source_rows),0) metric_a, COALESCE(SUM(event_rows),0) metric_b,
					COALESCE(SUM(eligible_trial_rows),0) metric_c,
					COALESCE(SUM(ignored_rows),0) metric_d,
					COALESCE(SUM(unknown_registration_rows),0) metric_e,
					COALESCE(SUM(semantics_version),0) metric_f
					FROM finance_credit_hour_states WHERE hour_ts>=? AND hour_ts<?`,
				args: []any{from, to},
			},
			{
				label: "finance-gift-boundaries",
				query: `SELECT COUNT(*) rows, COALESCE(MAX(updated_at),0) max_updated,
					COALESCE(SUM(rows),0) metric_a, COALESCE(SUM(requests),0) metric_b,
					COALESCE(SUM(consume_quota),0) metric_c, COALESCE(SUM(refund_quota),0) metric_d,
					COALESCE(SUM(refund_records),0) metric_e,
					COALESCE(SUM(user_id),0) metric_f
					FROM finance_gift_boundary_states WHERE hour_ts>=? AND hour_ts<?`,
				args: []any{from, to},
			},
		}
		for _, aggregate := range factsAggregates {
			// Gift allocation is applied AFTER the immutable monthly component
			// is loaded. Scope-only boundary repairs must invalidate the full
			// report, not rebuild unchanged base usage/cost/day aggregates.
			if !includeCUR && aggregate.label == "finance-gift-boundaries" {
				continue
			}
			if err := writeAggregate(factsDB, aggregate.label, aggregate.query, aggregate.args...); err != nil {
				return "", err
			}
		}
		if includeCUR {
			// A selected month's opening gift balance depends on the entire
			// earlier ledger. Monthly base components do not contain gift
			// allocation and deliberately keep their narrower dependencies.
			seed, err := financeStartHour(m.cfg.FinanceStartDate)
			if err != nil {
				return "", err
			}
			rows, err := factsDB.WithContext(ctx).Raw(`
				SELECT 'user' kind,hour_ts,0 user_id,source_epoch,status,content_hash,semantics_version
				FROM finance_user_hour_states WHERE hour_ts>=? AND hour_ts<?
				UNION ALL SELECT 'credit',hour_ts,0,source_epoch,status,content_hash,semantics_version
				FROM finance_credit_hour_states WHERE hour_ts>=? AND hour_ts<?
				UNION ALL SELECT 'boundary',hour_ts,user_id,source_epoch,status,content_hash,0
				FROM finance_gift_boundary_states WHERE hour_ts>=? AND hour_ts<?
				ORDER BY kind,hour_ts,user_id,source_epoch`,
				min(seed, from), to, min(seed, from), to, min(seed, from), to).Rows()
			if err != nil {
				return "", fmt.Errorf("读取赠送期初依据版本: %w", err)
			}
			for rows.Next() {
				var kind, epoch, status, content string
				var hour, user, version int64
				if err := rows.Scan(&kind, &hour, &user, &epoch, &status, &content, &version); err != nil {
					_ = rows.Close()
					return "", err
				}
				_, _ = fmt.Fprintf(hash, "gift-proof|%q|%d|%d|%q|%q|%q|%d\n", kind, hour, user, epoch, status, content, version)
			}
			err = rows.Err()
			_ = rows.Close()
			if err != nil {
				return "", err
			}
		}
	}

	if includeCUR && m.cfg.FinanceCURArtifactEnabled {
		path := strings.TrimSpace(m.cfg.FinanceCURArtifactPath)
		info, err := os.Stat(path)
		switch {
		case err == nil:
			_, _ = fmt.Fprintf(hash, "cur-artifact|%s|%d|%d\n", path, info.Size(), info.ModTime().UnixNano())
		case os.IsNotExist(err):
			_, _ = fmt.Fprintf(hash, "cur-artifact|%s|missing\n", path)
		default:
			return "", fmt.Errorf("读取 AWS CUR 成本产物版本: %w", err)
		}
	} else if includeCUR {
		_, _ = hash.Write([]byte("cur-artifact|disabled\n"))
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

type financeReportSourceAggregate struct {
	Rows       int64 `gorm:"column:rows"`
	MaxUpdated int64 `gorm:"column:max_updated"`
	MetricA    int64 `gorm:"column:metric_a"`
	MetricB    int64 `gorm:"column:metric_b"`
	MetricC    int64 `gorm:"column:metric_c"`
	MetricD    int64 `gorm:"column:metric_d"`
	MetricE    int64 `gorm:"column:metric_e"`
	MetricF    int64 `gorm:"column:metric_f"`
}
