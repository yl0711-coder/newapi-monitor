package monitor

import (
	"encoding/json"
	"fmt"
	"sort"

	"gorm.io/gorm"
)

func tokenReportFilters(db *gorm.DB, q tokenDetectionQuery) *gorm.DB {
	if (q.Status == "insufficient" || q.Status == "not_hit") && tokenReportUseEntryIndex(db) {
		if !tokenReportSummaryStatusJoined(db) {
			db = db.Where("EXISTS (SELECT 1 FROM "+tokenReportIndexTable+" e WHERE e.source_epoch=token_detection_currents.source_epoch AND e.log_id=token_detection_currents.log_id AND e.status=?)", q.Status)
		}
	} else {
		switch q.Status {
		case "candidate":
			db = db.Where("candidate = ? AND COALESCE(detection_state,'') <> ''", true)
		case "insufficient":
			db = db.Where("candidate = ? AND COALESCE(detection_state,'') <> '' AND (source_complete = 0 OR EXISTS (SELECT 1 FROM json_each(result_json, '$.rules') WHERE json_extract(value,'$.eligible') = 0))", false)
		case "not_hit":
			db = db.Where("candidate = ? AND COALESCE(detection_state,'') <> '' AND source_complete = 1 AND NOT EXISTS (SELECT 1 FROM json_each(result_json, '$.rules') WHERE json_extract(value,'$.eligible') = 0)", false)
		}
	}
	if q.Rule != "" || q.Evidence != "" {
		sql := "EXISTS (SELECT 1 FROM json_each(result_json, '$.rules') WHERE 1=1"
		args := []any{}
		if q.Rule != "" {
			sql += " AND json_extract(value,'$.name') = ? AND json_extract(value,'$.hit') = 1"
			args = append(args, q.Rule)
		}
		if q.Evidence != "" {
			sql += " AND json_extract(value,'$.evidence_level') = ?"
			args = append(args, q.Evidence)
		}
		db = db.Where(sql+")", args...)
	}
	paths := map[string]string{"observed_consumption_not_loss": "actual_quota", "historical_formula_v1": "formula_difference_quota", "input_upper_reference_v1": "estimated_loss_quota"}
	if field, ok := paths[q.Method]; ok {
		// Frozen prices store canonical positive integer/fraction operands. Match
		// the summary's monetary identity guard; a quota without a unit is not money.
		usable := "(json_extract(result_json, '$." + field + "') IS NOT NULL AND COALESCE(json_extract(result_json,'$.price.subject'),'') != '' AND COALESCE(json_extract(result_json,'$.price.unit'),'') != '' AND CAST(COALESCE(json_extract(result_json,'$.price.quota_per_unit'),'0') AS REAL) > 0)"
		if q.Unavailable {
			db = db.Where("candidate = 1 AND COALESCE(detection_state,'') <> '' AND NOT " + usable)
		} else {
			db = db.Where("candidate = 1 AND COALESCE(detection_state,'') <> '' AND " + usable)
		}
	}

	if q.Subject != "" {
		db = db.Where("json_extract(result_json,'$.price.subject') = ? AND json_extract(result_json,'$.price.unit') = ? AND json_extract(result_json,'$.price.quota_per_unit') = ?", q.Subject, q.Unit, q.QuotaPerUnit)
	}
	return db
}
func tokenReportFiltersValid(q tokenDetectionQuery) bool {
	if q.Status != "" && q.Status != "candidate" && q.Status != "insufficient" && q.Status != "not_hit" {
		return false
	}
	if q.Rule != "" && !tokenRuleName(q.Rule) {
		return false
	}
	if q.Evidence != "" && q.Evidence != "unavailable" && tokenEvidenceRank(q.Evidence) == 0 {
		return false
	}
	if q.Method != "" && !tokenMoneyMethod(q.Method) || q.Unavailable && q.Method == "" {
		return false
	}
	if q.Subject != "" {
		return q.Method != "" && len(q.Subject) <= 64 && len(q.Unit) <= 32 && len(q.QuotaPerUnit) <= 64 && q.Unit != "" && q.QuotaPerUnit != ""
	}
	return q.Unit == "" && q.QuotaPerUnit == ""
}
func tokenResultStatus(r tokenDetectionResult) string {
	if r.Candidate {
		return "candidate"
	}
	if !r.SourceComplete {
		return "insufficient"
	}
	for _, rule := range r.Rules {
		if !rule.Eligible {
			return "insufficient"
		}
	}
	return "not_hit"
}

type tokenReportDimension struct {
	Kind       string `json:"kind"`
	Key        string `json:"key"`
	Requests   int64  `json:"requests"`
	Candidates int64  `json:"candidate_requests"`
}
type tokenReportAnalysis struct {
	StatusCounts map[string]int64       `json:"status_counts"`
	Calculable   map[string]int64       `json:"calculable_candidates"`
	Breakdown    []tokenReportDimension `json:"breakdown"`
}

func tokenReportAnalyze(rows []TokenDetectionCurrent) (tokenReportAnalysis, error) {
	a := tokenReportAnalysis{StatusCounts: map[string]int64{"candidate": 0, "insufficient": 0, "not_hit": 0}, Calculable: map[string]int64{"historical_formula_v1": 0, "input_upper_reference_v1": 0}, Breakdown: []tokenReportDimension{}}
	bins := map[string]tokenReportDimension{}
	for _, h := range rows {
		var r tokenDetectionResult
		if err := json.Unmarshal([]byte(h.ResultJSON), &r); err != nil {
			return a, err
		}
		a.StatusCounts[tokenResultStatus(r)]++
		if r.Candidate {
			if tokenReportMoneyAvailable(r, "historical_formula_v1") {
				a.Calculable["historical_formula_v1"]++
			}
			if tokenReportMoneyAvailable(r, "input_upper_reference_v1") {
				a.Calculable["input_upper_reference_v1"]++
			}
		}
		for kind, key := range map[string]string{"channel_id": fmt.Sprint(h.ChannelID), "user_id": fmt.Sprint(h.UserID), "model": h.Model, "group": h.UserGroup, "hour": fmt.Sprint(h.CreatedAt / 3600 * 3600)} {
			id := kind + "\x00" + key
			b := bins[id]
			b.Kind, b.Key = kind, key
			b.Requests++
			if r.Candidate {
				b.Candidates++
			}
			bins[id] = b
		}
	}
	for _, b := range bins {
		a.Breakdown = append(a.Breakdown, b)
	}
	sort.Slice(a.Breakdown, func(i, j int) bool {
		a, b := a.Breakdown[i], a.Breakdown[j]
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Candidates != b.Candidates {
			return a.Candidates > b.Candidates
		}
		return a.Key < b.Key
	})
	return a, nil
}

func tokenReportMoneyAvailable(r tokenDetectionResult, method string) bool {
	v := tokenNotifyMoney(r, method)
	if v == nil {
		return false
	}
	s := tokenDetectionSummary{Amounts: map[string]tokenDetectionAmounts{}, Unavailable: map[string]int64{}}
	tokenDetectionAddAmount(&s, r.Price, method, *v)
	return len(s.Amounts) == 1
}
