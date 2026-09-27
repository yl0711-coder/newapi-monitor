package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
)

// Hash the small hourly publication ledger in key order, not aggregate sums.
// A same-second validity correction or same-total redistribution must invalidate
// both report and month caches. Preserve the existing timestamp dependency for
// writers without a finer content revision; retry messages themselves are not
// hashed. No request detail scan, schema change or production query is needed.
func (m *Monitor) hashFinanceHourlyProofs(ctx context.Context, out io.Writer, from, to int64) error {
	// Additive SQLite migrations can leave old columns NULL. Match the
	// accounting queries' conservative zero/unknown behavior rather than fail
	// the entire report while scanning an old receipt.
	query := `SELECT hs.hour_ts,COALESCE(hs.status,''),COALESCE(hs.traffic_class_version,0),
		COALESCE(hs.updated_at,0),COALESCE(hs.rows,0),COALESCE(hs.requests,0),COALESCE(hs.tokens,0),COALESCE(hs.quota,0),
		COALESCE(hs.internal_test_rows,0),COALESCE(hs.internal_test_requests,0),
		COALESCE(hs.internal_test_tokens,0),COALESCE(hs.internal_test_quota,0),
		COALESCE((` + accountingCompleteHourPredicateSQL("hs") + `),0) AS coverage_valid
		FROM stability_hour_ingest_states hs WHERE hs.hour_ts>=? AND hs.hour_ts<? ORDER BY hs.hour_ts`
	rows, err := m.storeDB.WithContext(ctx).Raw(query, accountingTrafficVersions(), accountingTrafficVersions(), from, to).Rows()
	if err != nil {
		return fmt.Errorf("读取核算小时覆盖版本: %w", err)
	}
	defer rows.Close()
	_, _ = io.WriteString(out, "stability-hour-proofs-v2\n")
	encoder := json.NewEncoder(out)
	for rows.Next() {
		var proof struct {
			HourTs                                                        int64
			Status                                                        string
			Version                                                       int
			UpdatedAt                                                     int64
			Rows, Requests, Tokens, Quota                                 int64
			InternalRows, InternalRequests, InternalTokens, InternalQuota int64
			CoverageValid                                                 bool
		}
		if err := rows.Scan(&proof.HourTs, &proof.Status, &proof.Version, &proof.UpdatedAt, &proof.Rows, &proof.Requests,
			&proof.Tokens, &proof.Quota, &proof.InternalRows, &proof.InternalRequests, &proof.InternalTokens,
			&proof.InternalQuota, &proof.CoverageValid); err != nil {
			return err
		}
		if err := encoder.Encode(proof); err != nil {
			return err
		}
	}
	return rows.Err()
}
