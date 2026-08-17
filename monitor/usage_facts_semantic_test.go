package monitor

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"
)

// BenchmarkUsageFactSemanticAudit200x366 是显式运行的本地容量基准，不进入
// 普通 go test。它构造 200 成员×366 天×4 维日事实与成员日证明，量化发布前/
// 小时级语义审计的时间、分配和文件规模，防止完整性保护退化成频繁全表重负载。
func BenchmarkUsageFactSemanticAudit200x366(b *testing.B) {
	const (
		users = 200
		days  = 366
		dims  = 4
	)
	dir := b.TempDir()
	factsPath := dir + "/usage-facts.db"
	m := &Monitor{cfg: Settings{
		StorePath:               dir + "/monitor.db",
		UsageFactsStorePath:     factsPath,
		UsageFactsBackfillDays:  days,
		UsageFactsRetentionDays: 400,
	}, chNames: map[string]string{}}
	if err := m.openStore(m.cfg.StorePath); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(m.Close)
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, usageCST).Unix()
	to := from + days*usageFactDaySeconds
	ids := make([]int64, users)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	sqlDB, err := m.usageFactsStore().DB()
	if err != nil {
		b.Fatal(err)
	}
	tx, err := sqlDB.Begin()
	if err != nil {
		b.Fatal(err)
	}
	factStmt, err := tx.Prepare(`INSERT INTO usage_daily_facts
 (date_ts,user_id,channel_id,grp,model_name,token_id,token_name,requests,refund_records,prompt_tokens,completion_tokens,consume_quota,refund_quota)
 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		_ = tx.Rollback()
		b.Fatal(err)
	}
	proofStmt, err := tx.Prepare(`INSERT INTO usage_fact_member_day_states
 (user_id,date_ts,rows,requests,tokens,content_hash,updated_at) VALUES (?,?,?,?,?,?,?)`)
	if err != nil {
		_ = factStmt.Close()
		_ = tx.Rollback()
		b.Fatal(err)
	}
	for day := 0; day < days; day++ {
		dateTs := from + int64(day)*usageFactDaySeconds
		for user := 1; user <= users; user++ {
			rows := make([]UsageDailyFact, 0, dims)
			for dim := 0; dim < dims; dim++ {
				row := UsageDailyFact{
					DateTs: dateTs, UserID: int64(user), ChannelID: int64(dim + 1),
					Grp: fmt.Sprintf("g-%02d", dim), ModelName: fmt.Sprintf("model-%02d", dim),
					TokenID: int64(user*1000 + dim), TokenName: fmt.Sprintf("token-%d-%d", user, dim),
					Requests: 10, PromptTokens: 1000, CompletionTokens: 200, ConsumeQuota: 500000,
				}
				rows = append(rows, row)
				if _, err := factStmt.Exec(row.DateTs, row.UserID, row.ChannelID, row.Grp, row.ModelName,
					row.TokenID, row.TokenName, row.Requests, row.RefundRecords, row.PromptTokens,
					row.CompletionTokens, row.ConsumeQuota, row.RefundQuota); err != nil {
					_ = factStmt.Close()
					_ = proofStmt.Close()
					_ = tx.Rollback()
					b.Fatal(err)
				}
			}
			metrics := dailyFactsMetrics(rows)
			if _, err := proofStmt.Exec(user, dateTs, metrics.Rows, metrics.Requests, metrics.tokens(),
				usageDailyFactContentHash(rows), to); err != nil {
				_ = factStmt.Close()
				_ = proofStmt.Close()
				_ = tx.Rollback()
				b.Fatal(err)
			}
		}
	}
	_ = factStmt.Close()
	_ = proofStmt.Close()
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	if _, err := sqlDB.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		b.Fatal(err)
	}
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := m.auditUsageFactSnapshot(context.Background(), from, to, ids); err != nil {
			b.Fatal(err)
		}
	}
	if info, err := os.Stat(factsPath); err == nil {
		b.ReportMetric(float64(info.Size())/(1024*1024), "MiB-db")
	}
	b.ReportMetric(float64(users*days*dims), "daily-rows")
	b.ReportMetric(float64(users*days), "proof-rows")
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	b.ReportMetric(float64(mem.HeapAlloc)/(1024*1024), "MiB-heap-live")
	b.ReportMetric(float64(mem.HeapSys)/(1024*1024), "MiB-heap-sys")
}

func BenchmarkUsageFactSemanticAuditBatch200x366(b *testing.B) {
	const (
		users = 200
		days  = 366
		dims  = 4
	)
	dir := b.TempDir()
	m := &Monitor{cfg: Settings{
		StorePath: dir + "/monitor.db", UsageFactsStorePath: dir + "/usage-facts.db",
		UsageFactsBackfillDays: days, UsageFactsRetentionDays: 400,
	}, chNames: map[string]string{}}
	if err := m.openStore(m.cfg.StorePath); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(m.Close)
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, usageCST).Unix()
	to := from + days*usageFactDaySeconds
	ids := make([]int64, users)
	for i := range ids {
		ids[i] = int64(i + 1)
	}
	sqlDB, err := m.usageFactsStore().DB()
	if err != nil {
		b.Fatal(err)
	}
	tx, err := sqlDB.Begin()
	if err != nil {
		b.Fatal(err)
	}
	factStmt, err := tx.Prepare(`INSERT INTO usage_daily_facts
 (date_ts,user_id,channel_id,grp,model_name,token_id,token_name,requests,refund_records,prompt_tokens,completion_tokens,consume_quota,refund_quota)
 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		_ = tx.Rollback()
		b.Fatal(err)
	}
	proofStmt, err := tx.Prepare(`INSERT INTO usage_fact_member_day_states
 (user_id,date_ts,rows,requests,tokens,content_hash,updated_at) VALUES (?,?,?,?,?,?,?)`)
	if err != nil {
		_ = factStmt.Close()
		_ = tx.Rollback()
		b.Fatal(err)
	}
	for day := 0; day < days; day++ {
		dateTs := from + int64(day)*usageFactDaySeconds
		for user := 1; user <= users; user++ {
			rows := make([]UsageDailyFact, 0, dims)
			for dim := 0; dim < dims; dim++ {
				row := UsageDailyFact{DateTs: dateTs, UserID: int64(user), ChannelID: int64(dim + 1),
					Grp: fmt.Sprintf("g-%02d", dim), ModelName: fmt.Sprintf("model-%02d", dim),
					TokenID: int64(user*1000 + dim), TokenName: fmt.Sprintf("token-%d-%d", user, dim),
					Requests: 10, PromptTokens: 1000, CompletionTokens: 200, ConsumeQuota: 500000}
				rows = append(rows, row)
				if _, err := factStmt.Exec(row.DateTs, row.UserID, row.ChannelID, row.Grp, row.ModelName,
					row.TokenID, row.TokenName, row.Requests, row.RefundRecords, row.PromptTokens,
					row.CompletionTokens, row.ConsumeQuota, row.RefundQuota); err != nil {
					b.Fatal(err)
				}
			}
			metrics := dailyFactsMetrics(rows)
			if _, err := proofStmt.Exec(user, dateTs, metrics.Rows, metrics.Requests, metrics.tokens(),
				usageDailyFactContentHash(rows), to); err != nil {
				b.Fatal(err)
			}
		}
	}
	_ = factStmt.Close()
	_ = proofStmt.Close()
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	batchEnd := from + int64(usageFactSemanticAuditDays)*usageFactDaySeconds
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := auditUsageFactDailyRange(m.usageFactsStore(), from, batchEnd, ids); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(users*usageFactSemanticAuditDays*dims), "daily-rows")
	b.ReportMetric(float64(users*usageFactSemanticAuditDays), "proof-rows")
}
