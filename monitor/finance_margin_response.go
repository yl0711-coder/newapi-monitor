package monitor

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Correct the small derived display field on delivery, including unchanged
// historical/monthly cache entries. Do not force expensive month rebuilds,
// alter money, refresh timestamps, write snapshots or query a database.
func writeFinanceReportJSON(c *gin.Context, payload []byte) {
	corrected, err := clearFinanceInvalidMargins(payload)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "经营核算报表格式异常，请重试"})
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", corrected)
}

func clearFinanceInvalidMargins(payload []byte) ([]byte, error) {
	var report map[string]json.RawMessage
	if err := json.Unmarshal(payload, &report); err != nil {
		return nil, err
	}
	changed, err := clearFinanceStatementMargin(report)
	if err != nil {
		return nil, err
	}
	for _, key := range []string{"periods", "days"} {
		raw, present := report[key]
		if !present {
			continue
		}
		var entries []map[string]json.RawMessage
		if err := json.Unmarshal(raw, &entries); err != nil {
			return nil, err
		}
		entriesChanged := false
		for _, entry := range entries {
			entryChanged, err := clearFinanceStatementMargin(entry)
			if err != nil {
				return nil, err
			}
			entriesChanged = entriesChanged || entryChanged
		}
		if entriesChanged {
			report[key], err = json.Marshal(entries)
			if err != nil {
				return nil, err
			}
			changed = true
		}
	}
	if !changed {
		return payload, nil // Retain the original bytes for unaffected reports.
	}
	return json.Marshal(report)
}

func clearFinanceStatementMargin(container map[string]json.RawMessage) (bool, error) {
	raw, present := container["statement"]
	if !present || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false, nil
	}
	var statement map[string]json.RawMessage
	if err := json.Unmarshal(raw, &statement); err != nil {
		return false, err
	}
	var revenue channelEconomicsMoneyView
	if json.Unmarshal(statement["paired_user_consumption"], &revenue) == nil {
		if amount, known := financeMoneyInt64(revenue); known && amount > 0 {
			return false, nil
		}
	}
	changed := false
	for _, key := range []string{"paired_contribution_margin_percent", "contribution_margin_percent"} {
		if value, ok := statement[key]; ok && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			statement[key], changed = json.RawMessage("null"), true
		}
	}
	if changed {
		encoded, err := json.Marshal(statement)
		if err != nil {
			return false, err
		}
		container["statement"] = encoded
	}
	return changed, nil
}
