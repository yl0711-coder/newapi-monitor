package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
)

// Historical recharge evidence is an accounting input, not just a row count.
// Same-length evidence corrections must invalidate reports even when IDs,
// effective dates and timestamps are unchanged. The local version ledger is
// small; stream it rather than retaining snapshots or reading provider logs.
func (m *Monitor) hashFinanceRechargeVersions(ctx context.Context, out io.Writer, to int64) error {
	rows, err := m.storeDB.WithContext(ctx).Raw(`SELECT COALESCE(domain,''),COALESCE(version,0),
		COALESCE(effective_at,0),COALESCE(snapshot_json,'')
		FROM channel_finance_versions WHERE effective_at<? ORDER BY domain,effective_at,version`, to).Rows()
	if err != nil {
		return fmt.Errorf("读取历史充值比例内容版本: %w", err)
	}
	defer rows.Close()
	if _, err := io.WriteString(out, "finance-recharge-content-v1\n"); err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var row struct {
			Domain      string
			Version     int64
			EffectiveAt int64
			Snapshot    string
		}
		if err := rows.Scan(&row.Domain, &row.Version, &row.EffectiveAt, &row.Snapshot); err != nil {
			return err
		}
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}
	return rows.Err()
}
