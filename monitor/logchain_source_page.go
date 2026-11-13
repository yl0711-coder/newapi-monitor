package monitor

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
)

// Aggregate only AFTER the existing predicates, ordering and LIMIT have
// selected one page. This reduces tunnel packet overhead without truncating
// content/other, changing the source, or aggregating the whole time range.
func logChainPackedPageQuery(page string) string {
	return "SELECT /*+ MAX_EXECUTION_TIME(" + strconv.Itoa(logChainQueryTimeoutMS) + ") */ " +
		"JSON_ARRAYAGG(JSON_ARRAY(id,created_at,c_type,c_uid,c_uname,c_group,c_token,c_chid,c_model," +
		"c_pt,c_ct,c_ut,c_stream,c_quota,c_content,c_other,c_reqid)) FROM (" + page + ") logchain_page"
}

type logChainWireRow struct {
	cells         []json.RawMessage
	id, createdAt int64
}

func decodeLogChainPackedPage(payload []byte, limit int, asc bool) ([]logChainWireRow, error) {
	if len(payload) == 0 { // JSON_ARRAYAGG of an empty page is SQL NULL.
		return nil, nil
	}
	var raw [][]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, fmt.Errorf("invalid packed log page: %w", err)
	}
	if len(raw) > limit {
		return nil, errors.New("packed log page exceeds requested row limit")
	}
	out := make([]logChainWireRow, len(raw))
	for i, cells := range raw {
		if len(cells) != 17 {
			return nil, errors.New("packed log row has an invalid column count")
		}
		out[i].cells = cells
		if err := json.Unmarshal(cells[0], &out[i].id); err != nil {
			return nil, fmt.Errorf("invalid packed log id: %w", err)
		}
		if err := json.Unmarshal(cells[1], &out[i].createdAt); err != nil {
			return nil, fmt.Errorf("invalid packed log time: %w", err)
		}
	}
	// MySQL does not promise JSON_ARRAYAGG order. Reapply the exact page key,
	// including the same-second tie breaker, before calculating the cursor.
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.createdAt == b.createdAt {
			if asc {
				return a.id < b.id
			}
			return a.id > b.id
		}
		if asc {
			return a.createdAt < b.createdAt
		}
		return a.createdAt > b.createdAt
	})
	return out, nil
}

func (r logChainWireRow) scan(dest ...any) error {
	if len(dest) != len(r.cells) {
		return errors.New("packed log destination column count mismatch")
	}
	for i, value := range r.cells {
		if nullable, ok := dest[i].(*sql.NullInt64); ok {
			var n *int64
			if err := json.Unmarshal(value, &n); err != nil {
				return fmt.Errorf("invalid packed log column %d: %w", i, err)
			}
			*nullable = sql.NullInt64{}
			if n != nil {
				*nullable = sql.NullInt64{Int64: *n, Valid: true}
			}
			continue
		}
		// Decode directly into integers/strings, never through float64: source
		// ids and quota may exceed JSON's JavaScript safe-integer range.
		if err := json.Unmarshal(value, dest[i]); err != nil {
			return fmt.Errorf("invalid packed log column %d: %w", i, err)
		}
	}
	return nil
}
