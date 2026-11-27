package monthlysource

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Fixed plan members, not a scan of all historical pages. Each correlated
// lookup seeks the latest sequence using the existing (scan_id,sequence) key.
// Do not ORDER BY here: sorting full payloads would buffer up to 62 large
// originals. The caller keeps only small validated summaries in plan order.
func fundingMonthHeadsQuery(plan FundingMonthCapturePlan) (string, []any, error) {
	if len(plan.Scans) == 0 || len(plan.Scans) > 62 {
		return "", nil, ErrFundingMonthCaptureIntegrity
	}
	values, args := make([]string, len(plan.Scans)), make([]any, len(plan.Scans))
	for i, scan := range plan.Scans {
		if !validFundingScanID(scan.ID) {
			return "", nil, ErrFundingMonthCaptureIntegrity
		}
		values[i], args[i] = fmt.Sprintf("(%d,?)", i), scan.ID
	}
	return `WITH requested(ordinal,scan_id) AS (VALUES ` + strings.Join(values, ",") + `)
SELECT r.ordinal,p.id,p.scan_id,p.sequence,
CASE WHEN length(p.payload)<=2097152 THEN p.payload ELSE NULL END
FROM requested r LEFT JOIN portal_monthly_funding_pages p
ON p.scan_id=r.scan_id AND p.sequence=(
 SELECT h.sequence FROM portal_monthly_funding_pages h
 WHERE h.scan_id=r.scan_id ORDER BY h.sequence DESC LIMIT 1)`, args, nil
}

type fundingMonthHead struct {
	seen, present, end bool
	started, completed int64
	err                error
}

func inspectFundingMonthCaptureHeads(ctx context.Context, receiver *sql.DB, plan FundingMonthCapturePlan, now time.Time) (FundingMonthCaptureProgress, error) {
	query, args, err := fundingMonthHeadsQuery(plan)
	if err != nil {
		return FundingMonthCaptureProgress{}, err
	}
	rows, err := receiver.QueryContext(ctx, query, args...)
	if err != nil {
		return FundingMonthCaptureProgress{}, err
	}
	defer rows.Close()
	heads := make([]fundingMonthHead, len(plan.Scans))
	for rows.Next() {
		var ordinal int
		var id, scanID sql.NullString
		var sequence sql.NullInt64
		var raw []byte
		if err := rows.Scan(&ordinal, &id, &scanID, &sequence, &raw); err != nil {
			return FundingMonthCaptureProgress{}, err
		}
		if ordinal < 0 || ordinal >= len(heads) || heads[ordinal].seen {
			return FundingMonthCaptureProgress{}, ErrFundingMonthCaptureIntegrity
		}
		h := &heads[ordinal]
		h.seen = true
		if !id.Valid {
			if scanID.Valid || sequence.Valid || raw != nil {
				return FundingMonthCaptureProgress{}, ErrFundingMonthCaptureIntegrity
			}
			continue
		}
		h.present = true
		if !scanID.Valid || !sequence.Valid {
			h.err = ErrFundingMonthCaptureIntegrity
			continue
		}
		page, err := decodeFundingStoredPage(id.String, scanID.String, sequence.Int64, raw)
		h.err = err
		if err != nil {
			continue
		}
		if !sameFundingScan(page.Scan, plan.Scans[ordinal]) || page.CompletedAtMS > now.UnixMilli() {
			h.err = ErrFundingMonthCaptureIntegrity
		}
		h.started, h.completed, h.end = page.StartedAtMS, page.CompletedAtMS, page.EndReached
	}
	if err := rows.Err(); err != nil {
		return FundingMonthCaptureProgress{}, err
	}
	if err := rows.Close(); err != nil {
		return FundingMonthCaptureProgress{}, err
	}
	out := FundingMonthCaptureProgress{Mode: "funding_traversals_not_bill_approval", TotalScans: len(plan.Scans)}
	var oldEnd int64
	missing := false
	for i, h := range heads {
		if !h.seen {
			return FundingMonthCaptureProgress{}, ErrFundingMonthCaptureIntegrity
		}
		if !h.present {
			missing = true
			if out.NextScanID == "" {
				out.NextScanID = plan.Scans[i].ID
			}
			continue
		}
		// Preserve plan-order error/continuation decisions even if SQLite
		// returns the requested rows in a different physical order.
		if h.err != nil {
			return FundingMonthCaptureProgress{}, h.err
		}
		if missing {
			return FundingMonthCaptureProgress{}, ErrFundingMonthCaptureIntegrity
		}
		if i < len(plan.Scans)/2 {
			oldEnd = max(oldEnd, h.completed)
		} else if h.started <= oldEnd {
			return FundingMonthCaptureProgress{}, ErrFundingMonthCaptureIntegrity
		}
		if !h.end {
			missing, out.NextScanID = true, plan.Scans[i].ID
		} else {
			out.FinishedScans++
		}
	}
	if err := ctx.Err(); err != nil {
		return FundingMonthCaptureProgress{}, err
	}
	if out.FinishedScans >= len(plan.Scans)/2 && now.UnixMilli() <= oldEnd {
		out.NextEligibleMS = oldEnd + 1
	}
	out.TraversalsFinished = out.FinishedScans == len(plan.Scans)
	return out, nil
}
