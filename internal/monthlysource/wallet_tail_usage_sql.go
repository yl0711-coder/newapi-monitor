package monthlysource

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
)

// LIMIT materializes the key page before grouping. Only metadata and at most
// ten aggregate rows leave the server, not thousands of IDs and an IN/JSON
// key list sent back to it. All CTE reads belong to the owner's same snapshot.
func serverTailUsageQuery(r WalletTailPageRequest) (string, []any, error) {
	a := r.After
	if r.Kind != "user_usage" || a != (WalletTailPageCursor{}) && (a.At < r.Wallet.Boundary || a.ID <= 0 || a.Type != 2 && a.Type != 6) {
		return "", nil, errors.New("invalid server usage cursor or kind")
	}
	head := `SELECT id,created_at,type FROM logs FORCE INDEX(idx_user_created_type) WHERE user_id=? AND type IN (2,6) AND `
	order := fmt.Sprintf(" ORDER BY created_at,type,id LIMIT %d", walletTailPageRows)
	keys, args := head+"created_at>=?"+order, []any{r.Wallet.UserID, r.Wallet.Boundary}
	if a != (WalletTailPageCursor{}) {
		keys = "SELECT id,created_at,type FROM ((" + head + "created_at=? AND type=? AND id>?" + order + ") UNION ALL (" + head + "created_at=? AND type>?" + order + ") UNION ALL (" + head + "created_at>?" + order + ")) AS usage_candidates" + order
		args = []any{r.Wallet.UserID, a.At, a.Type, a.ID, r.Wallet.UserID, a.At, a.Type, r.Wallet.UserID, a.At}
	}
	q := `WITH usage_keys AS (` + keys + `),
usage_totals AS (
SELECT l.user_id,l.type,` + usageBillingSourceSQL + ` AS billing_source,
COUNT(*) AS records,COUNT(l.quota) AS known_count,CAST(SUM(l.quota) AS CHAR) AS quota,
MIN(l.quota) AS min_quota,MAX(l.quota) AS max_quota,MIN(l.id) AS min_id,MAX(l.id) AS max_id,
MIN(l.created_at) AS first_at,MAX(l.created_at) AS last_at
FROM usage_keys AS k STRAIGHT_JOIN logs AS l FORCE INDEX(PRIMARY) ON l.id=k.id
WHERE l.user_id=? AND l.type IN (2,6)
GROUP BY l.user_id,l.type,billing_source LIMIT 11),
usage_meta AS (SELECT COUNT(*) AS records FROM usage_keys),
usage_last AS (SELECT created_at,type,id FROM usage_keys ORDER BY created_at DESC,type DESC,id DESC LIMIT 1)
SELECT /*+ MAX_EXECUTION_TIME(2000) */ m.records,
COALESCE(p.created_at,?),COALESCE(p.type,?),COALESCE(p.id,?),
CASE WHEN m.records=0 THEN 0 ELSE t.user_id END,
CASE WHEN m.records=0 THEN 0 ELSE t.type END,
CASE WHEN m.records=0 THEN '' ELSE t.billing_source END,
CASE WHEN m.records=0 THEN 0 ELSE t.records END,
CASE WHEN m.records=0 THEN 0 ELSE t.known_count END,
CASE WHEN m.records=0 THEN '0' ELSE t.quota END,
CASE WHEN m.records=0 THEN 0 ELSE t.min_quota END,
CASE WHEN m.records=0 THEN 0 ELSE t.max_quota END,
CASE WHEN m.records=0 THEN 0 ELSE t.min_id END,
CASE WHEN m.records=0 THEN 0 ELSE t.max_id END,
CASE WHEN m.records=0 THEN 0 ELSE t.first_at END,
CASE WHEN m.records=0 THEN 0 ELSE t.last_at END
FROM usage_meta AS m LEFT JOIN usage_last AS p ON TRUE LEFT JOIN usage_totals AS t ON TRUE
ORDER BY t.type,t.billing_source`
	args = append(args, r.Wallet.UserID, a.At, a.Type, a.ID)
	return q, args, nil
}

// Empty pages have one explicitly all-zero aggregate row. Nonempty pages
// never coalesce NULL money fields: a missing quota/count/join must fail.
func readServerTailUsage(rows *sql.Rows, request WalletTailPageRequest) ([]UsageWindowRow, WalletTailPageCursor, bool, error) {
	defer rows.Close()
	var next WalletTailPageCursor
	result := []UsageWindowRow{}
	count, lines, observed := int64(-1), 0, int64(0)
	window := UsageWindowRequest{UserID: request.Wallet.UserID, From: request.Wallet.Boundary, To: math.MaxInt64}
	for rows.Next() {
		var n, known int64
		var cursor WalletTailPageCursor
		var row UsageWindowRow
		var quota string
		if err := rows.Scan(&n, &cursor.At, &cursor.Type, &cursor.ID, &row.UserID, &row.Type, &row.BillingSource, &row.Records, &known, &quota, &row.MinQuota, &row.MaxQuota, &row.MinID, &row.MaxID, &row.FirstAt, &row.LastAt); err != nil {
			return nil, next, false, err
		}
		amount, err := strconv.ParseInt(quota, 10, 64)
		if err != nil || strconv.FormatInt(amount, 10) != quota || n < 0 || n > walletTailPageRows || lines > 0 && (n != count || cursor != next) {
			return nil, next, false, errors.New("invalid server usage metadata or exact amount")
		}
		row.Quota = amount
		lines++
		if n == 0 {
			if lines != 1 || cursor != request.After || row != (UsageWindowRow{}) || known != 0 {
				return nil, next, false, errors.New("empty server usage page contains data")
			}
		} else {
			if cursor.ID <= 0 || cursor.At < request.Wallet.Boundary || (cursor.Type != 2 && cursor.Type != 6) || !tailPageCursorBefore(request.After, cursor) || known != row.Records || !validUsageWindowRow(row, window) || !validUsageWindowAmount(row) || row.LastAt > cursor.At || len(result) >= usageWindowRowsLimit || len(result) > 0 && !usageRowBefore(result[len(result)-1], row) {
				return nil, next, false, errors.New("server usage scope, ordering or amount differs")
			}
			observed += row.Records
			if observed > n {
				return nil, next, false, errors.New("server usage count exceeds bounded keys")
			}
			result = append(result, row)
		}
		count, next = n, cursor
	}
	if err := rows.Err(); err != nil {
		return nil, next, false, err
	}
	if err := rows.Close(); err != nil {
		return nil, next, false, err
	}
	if lines == 0 || observed != count {
		return nil, next, false, errors.New("server usage omitted metadata or selected rows")
	}
	if count > 0 {
		matchedLast := false
		for _, row := range result {
			if row.Type == int(next.Type) && row.LastAt == next.At && next.ID >= row.MinID && next.ID <= row.MaxID {
				matchedLast = true
			}
		}
		if !matchedLast {
			return nil, next, false, errors.New("server usage cursor differs from aggregates")
		}
	}
	return result, next, count < walletTailPageRows, nil
}
