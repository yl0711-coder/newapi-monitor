package monitor

import (
	"encoding/json"
	"sync"

	"gorm.io/gorm"
)

const tokenReportSummaryVersions = "token_report_summary_versions_v1"

// Versions describe committed summary operands in five-minute event windows.
// They are read in the same WAL transaction as the list. They are not a TTL:
// changed, inserted, moved or deleted results invalidate the next read.
func initTokenReportSummaryVersions(tx *gorm.DB) error {
	if err := tx.Exec(`CREATE TABLE IF NOT EXISTS ` + tokenReportSummaryVersions + ` (
		source_epoch TEXT NOT NULL,start_at INTEGER NOT NULL,generation INTEGER NOT NULL,
		PRIMARY KEY(source_epoch,start_at)) WITHOUT ROWID`).Error; err != nil {
		return err
	}
	bump := func(alias string) string {
		return `INSERT INTO ` + tokenReportSummaryVersions + ` VALUES (` + alias + `.source_epoch,(` + alias + `.created_at/300)*300,1)
		ON CONFLICT(source_epoch,start_at) DO UPDATE SET generation=generation+1;`
	}
	for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
		body := ""
		if event != "INSERT" {
			body += bump("OLD")
		}
		if event != "DELETE" {
			body += bump("NEW")
		}
		if err := tx.Exec(`CREATE TRIGGER IF NOT EXISTS token_report_summary_version_` + event + `_v1 AFTER ` + event + ` ON ` + tokenReportSummaryTable + ` BEGIN ` + body + ` END`).Error; err != nil {
			return err
		}
	}
	// MAX(revision) cannot invalidate a change to a row below the maximum.
	// Keep dependency versions in the same transaction as the actual change.
	for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
		body, update := "", ""
		if event == "UPDATE" {
			update = " OF revision,link_revision,created_at ON token_metering_facts WHEN OLD.revision IS NOT NEW.revision OR OLD.link_revision IS NOT NEW.link_revision OR OLD.created_at IS NOT NEW.created_at"
		} else {
			update = " ON token_metering_facts"
		}
		if event != "INSERT" {
			body += bump("OLD")
		}
		if event != "DELETE" {
			body += bump("NEW")
		}
		if err := tx.Exec(`CREATE TRIGGER IF NOT EXISTS token_summary_fact_` + event + `_v1 AFTER ` + event + update + ` BEGIN ` + body + ` END`).Error; err != nil {
			return err
		}
	}
	if err := tx.Exec(`CREATE TABLE IF NOT EXISTS token_summary_dependency_version_v1(id INTEGER PRIMARY KEY,generation INTEGER NOT NULL)`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`INSERT OR IGNORE INTO token_summary_dependency_version_v1 VALUES(1,0)`).Error; err != nil {
		return err
	}
	for _, table := range []string{"token_detection_buckets", "token_metering_windows"} {
		for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
			when := ""
			if event == "UPDATE" {
				if table == "token_detection_buckets" {
					when = " WHEN OLD.generation IS NOT NEW.generation"
				} else {
					when = " WHEN OLD.done IS NOT NEW.done OR OLD.source_complete IS NOT NEW.source_complete OR OLD.replay IS NOT NEW.replay"
				}
			}
			if err := tx.Exec(`CREATE TRIGGER IF NOT EXISTS token_summary_` + table + `_` + event + `_v1 AFTER ` + event + ` ON ` + table + when + ` BEGIN UPDATE token_summary_dependency_version_v1 SET generation=generation+1 WHERE id=1; END`).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

type tokenSummaryCacheEntry struct {
	store   *gorm.DB
	through int64
	stamp   string
	data    []byte
}

type tokenSummaryCache struct {
	mu      sync.Mutex
	entries map[string]tokenSummaryCacheEntry
}

type tokenReportAggregates struct {
	Summary  tokenDetectionSummary
	Analysis tokenReportAnalysis
}

func tokenReportSummaryStamp(tx *gorm.DB, q tokenDetectionQuery, through int64) (string, error) {
	var versions []struct{ StartAt, Generation int64 }
	err := tx.Table(tokenReportSummaryVersions).Select("start_at,generation").
		Where("source_epoch=? AND start_at>=? AND start_at<?", q.SourceEpoch, q.From/300*300, through).
		Order("start_at").Scan(&versions).Error
	if err != nil {
		return "", err
	}
	var dependencies int64
	if err := tx.Table("token_summary_dependency_version_v1").Select("generation").Where("id=1").Scan(&dependencies).Error; err != nil {
		return "", err
	}
	body, err := json.Marshal(struct {
		Versions     []struct{ StartAt, Generation int64 }
		Dependencies int64
	}{versions, dependencies})
	return string(body), err
}

func (m *Monitor) tokenDetectionReportAggregates(tx *gorm.DB, q tokenDetectionQuery, empty bool) (tokenDetectionSummary, tokenReportAnalysis, error) {
	// A cache can only publish committed read snapshots. Shared legacy stores
	// and writer-transaction callers use the original uncached path.
	entriesReady, err := tokenReportEntriesReady(tx)
	if err != nil {
		return tokenDetectionSummary{}, tokenReportAnalysis{}, err
	}
	cacheable := entriesReady && !empty && m.tokenReadDB != nil && tx.Migrator().HasTable(tokenReportSummaryVersions)
	key := ""
	if cacheable {
		scope := q
		scope.AfterAt, scope.AfterID, scope.Limit = 0, 0, 0
		key = tokenDetectionJSON(scope) + ":" + m.cfg.TokenDetectionPolicy
		m.tokenReportSummaryCache.mu.Lock()
		entry, ok := m.tokenReportSummaryCache.entries[key]
		m.tokenReportSummaryCache.mu.Unlock()
		if ok && entry.store == m.tokenReadDB {
			stamp, err := tokenReportSummaryStamp(tx, q, entry.through)
			if err != nil {
				return tokenDetectionSummary{}, tokenReportAnalysis{}, err
			}
			if stamp == entry.stamp {
				// The response adds live coverage/gaps. Decode a private copy so
				// simultaneous pages cannot mutate each other's maps or slices.
				var cached tokenReportAggregates
				if err := json.Unmarshal(entry.data, &cached); err != nil {
					return tokenDetectionSummary{}, tokenReportAnalysis{}, err
				}
				return cached.Summary, cached.Analysis, nil
			}
		}
	}
	var rows []TokenDetectionCurrent
	if !empty {
		query := tokenReportSummaryRange(tx, q)
		selectFields := "source_epoch,log_id,user_id,channel_id,model,user_group,created_at,result_json"
		order := "created_at,log_id"
		if tokenReportSummaryStatusJoined(query) {
			selectFields = "token_detection_currents.source_epoch,token_detection_currents.log_id,token_detection_currents.user_id,token_detection_currents.channel_id,token_detection_currents.model,token_detection_currents.user_group,token_detection_currents.created_at,token_detection_currents.result_json"
			order = "token_report_entries.created_at,token_report_entries.log_id"
		}
		if err := query.Select(selectFields).Order(order).Limit(5001).Find(&rows).Error; err != nil {
			return tokenDetectionSummary{}, tokenReportAnalysis{}, err
		}
		if err := tokenDetectionSuppressStaleSummaryRows(tx, q.SourceEpoch, rows, m.tokenDetectionConfiguredPolicyHash()); err != nil {
			return tokenDetectionSummary{}, tokenReportAnalysis{}, err
		}
	}
	truncated, through := len(rows) > 5000, q.To
	if truncated {
		// Changes after the extra sentinel cannot affect the first 5,000
		// selected results. Include its whole boundary window conservatively.
		through = min(through, rows[5000].CreatedAt/300*300+300)
		rows = rows[:5000]
	}
	summary, err := tokenDetectionSummarize(rows)
	if err != nil {
		return summary, tokenReportAnalysis{}, err
	}
	analysis, err := tokenReportAnalyze(rows)
	if err != nil {
		return summary, analysis, err
	}
	summary.Truncated = truncated
	if cacheable {
		stamp, err := tokenReportSummaryStamp(tx, q, through)
		if err != nil {
			return summary, analysis, err
		}
		// The private bounded cache keeps its examined prefix for invalidation.
		// Public serialization suppresses these totals when Truncated is true.
		type internalSummary tokenDetectionSummary
		data, err := json.Marshal(struct {
			Summary  internalSummary
			Analysis tokenReportAnalysis
		}{internalSummary(summary), analysis})
		if err != nil {
			return summary, analysis, err
		}
		// Bound retained data to 16 entries of at most 256 KiB. Oversized
		// analyses still work through the uncached path, without extra growth.
		if len(data) <= 256<<10 {
			m.tokenReportSummaryCache.mu.Lock()
			if m.tokenReportSummaryCache.entries == nil || len(m.tokenReportSummaryCache.entries) >= 16 {
				m.tokenReportSummaryCache.entries = map[string]tokenSummaryCacheEntry{}
			}
			m.tokenReportSummaryCache.entries[key] = tokenSummaryCacheEntry{m.tokenReadDB, through, stamp, data}
			m.tokenReportSummaryCache.mu.Unlock()
		}
	}
	return summary, analysis, nil
}

// Summary projections intentionally omit worker revision columns. Reconcile a
// bounded page against the authoritative fact/head identities before parsing
// result JSON so a late association change cannot remain a candidate in the
// aggregate while its queue item is pending.
func tokenDetectionSuppressStaleSummaryRows(tx *gorm.DB, epoch string, rows []TokenDetectionCurrent, policy ...string) error {
	if len(rows) == 0 || !tx.Migrator().HasTable(&TokenMeteringFact{}) {
		return nil
	}
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.LogID)
	}
	hash := ""
	if len(policy) > 0 {
		hash = policy[0]
	}
	stale, err := tokenDetectionStaleIdentities(tx, epoch, hash, ids)
	if err != nil {
		return err
	}
	for i := range rows {
		invalid, headOK := stale[rows[i].LogID]
		if !headOK || invalid {
			rows[i].Candidate = false
			rows[i].SourceComplete = false
			rows[i].ResultJSON = tokenDetectionJSON(tokenDetectionResult{Version: tokenDetectionVersion, SourceComplete: false, Gaps: []string{"association_or_detection_pending"}})
		}
	}
	return nil
}
