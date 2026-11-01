package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
)

// Only monetary candidates cross the connection. Type-3 keeps ALL actors;
// type-1/4 belongs to the requested account. Existing covered indexes, no DDL.
// Each branch limits BEFORE union sorting and primary-key body lookup. A
// selective index scan may examine more leaves than its output row limit;
// the unchanged two-second statement limit still applies to the whole query.
func selectiveTailFundingQuery(r WalletTailRequest, after FundingCursor) (string, []any, error) {
	if after != (FundingCursor{}) && (after.At < r.Boundary || after.ID <= 0) {
		return "", nil, errors.New("invalid selective funding cursor")
	}
	branches, args := []string{}, []any{}
	for _, typ := range []int{3, 1, 4} {
		index, own, base := "idx_created_at_type", "", []any{}
		if typ != 3 {
			index, own, base = "idx_user_created_type", "user_id=? AND ", []any{r.UserID}
		}
		head := fmt.Sprintf("SELECT id,created_at FROM logs FORCE INDEX(%s) WHERE %stype=%d AND ", index, own, typ)
		order := " ORDER BY created_at,type,id LIMIT 201"
		if after == (FundingCursor{}) {
			branches = append(branches, "("+head+"created_at>=?"+order+")")
			args = append(args, base...)
			args = append(args, r.Boundary)
		} else {
			branches = append(branches, "("+head+"created_at=? AND id>?"+order+")", "("+head+"created_at>?"+order+")")
			args = append(args, base...)
			args = append(args, after.At, after.ID)
			args = append(args, base...)
			args = append(args, after.At)
		}
	}
	q := `SELECT /*+ MAX_EXECUTION_TIME(2000) */ l.id,l.user_id,l.created_at,l.type,
CASE WHEN OCTET_LENGTH(l.content)<=4096 THEN l.content ELSE NULL END,
CASE WHEN COALESCE(OCTET_LENGTH(l.other),0)<=65536 THEN COALESCE(l.other,'') ELSE NULL END
FROM (SELECT id,created_at FROM (` + strings.Join(branches, " UNION ALL ") + `) AS funding_candidates
ORDER BY created_at,id LIMIT 201) AS funding_keys
STRAIGHT_JOIN logs AS l FORCE INDEX(PRIMARY) ON l.id=funding_keys.id
ORDER BY funding_keys.created_at,funding_keys.id`
	return q, args, nil
}

func readSelectiveTailFunding(ctx context.Context, tx *sql.Tx, r WalletTailRequest, pages *int, budget int) ([]FundingLog, error) {
	out, after, size := []FundingLog{}, FundingCursor{}, 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if *pages >= budget {
			return nil, errors.New("paged tail read work budget reached")
		}
		q, args, err := selectiveTailFundingQuery(r, after)
		if err != nil {
			return nil, err
		}
		rows, err := tx.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		page, err := readFundingRows(rows, FundingPage{Scan: FundingScan{UserIDs: []int64{r.UserID}, From: r.Boundary, To: math.MaxInt64}, FenceID: math.MaxInt64, After: after})
		if err != nil {
			return nil, err
		}
		*pages++
		n := len(page)
		if n > FundingPageLimit {
			page = page[:FundingPageLimit]
		}
		for _, row := range page {
			size += len(row.Content) + len(row.Other)
			if len(out) >= walletTailRowsLimit || size > walletTailBytesLimit {
				return nil, errors.New("selective funding exceeds whole-view observation budget")
			}
			out = append(out, row)
		}
		if n <= FundingPageLimit {
			return out, nil
		}
		last := page[len(page)-1]
		after = FundingCursor{At: last.CreatedAt, ID: last.ID}
	}
}
