package monitor

// 这个文件不是常规单测数据。只有显式指定
// MONITOR_ACCEPTANCE_FIXTURE_DIR 时才生成大规模、纯合成的本地验收快照。
// 默认 go test 会立即 Skip，不增加 CI 耗时，也绝不读取来源 MySQL。

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

const (
	localAcceptancePortalEmail    = "director@local.test"
	localAcceptancePortalPassword = "local-director-pass"
)

type localAcceptanceFixtureManifest struct {
	GeneratedAt          string `json:"generated_at"`
	MainStore            string `json:"main_store"`
	FactsStore           string `json:"facts_store"`
	Members              int    `json:"members"`
	Days                 int    `json:"days"`
	Dimensions           int    `json:"dimensions"`
	RangeStart           int64  `json:"range_start"`
	PublishedThrough     int64  `json:"published_through"`
	DailyFacts           int64  `json:"daily_facts"`
	HourFacts            int64  `json:"hour_facts"`
	MemberHourProofs     int64  `json:"member_hour_proofs"`
	MemberDayProofs      int64  `json:"member_day_proofs"`
	MainBytes            int64  `json:"main_bytes"`
	FactsBytes           int64  `json:"facts_bytes"`
	PortalEmail          string `json:"portal_email"`
	PortalPassword       string `json:"portal_password"`
	SessionSecret        string `json:"session_secret"`
	ExpectedMatrixMaxDay int    `json:"expected_matrix_max_days"`
}

// TestGenerateLocalFactsAcceptanceFixture 生成一份可直接挂载给
// LOCAL_SNAPSHOT_ONLY 容器的完整 SQLite 快照。数据维度是确定性的，
// 包含消费、退款、token/model/group/channel 维度、长用户名和零流量边界。
func TestGenerateLocalFactsAcceptanceFixture(t *testing.T) {
	dir := strings.TrimSpace(os.Getenv("MONITOR_ACCEPTANCE_FIXTURE_DIR"))
	if dir == "" {
		t.Skip("set MONITOR_ACCEPTANCE_FIXTURE_DIR to generate the local scale fixture")
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if absDir == string(filepath.Separator) || filepath.Dir(absDir) == absDir {
		t.Fatalf("拒绝在过宽目录生成验收数据: %q", absDir)
	}
	if err := os.MkdirAll(absDir, 0o777); err != nil {
		t.Fatal(err)
	}
	members := localAcceptanceEnvInt("MONITOR_ACCEPTANCE_MEMBERS", 200, 1, maxTrackedUsers)
	days := localAcceptanceEnvInt("MONITOR_ACCEPTANCE_DAYS", 366, 1, 366)
	dimensions := localAcceptanceEnvInt("MONITOR_ACCEPTANCE_DIMENSIONS", 4, 1, 8)
	mainPath := filepath.Join(absDir, "nexus_monitor.db")
	factsPath := filepath.Join(absDir, "usage-facts.db")
	manifestPath := filepath.Join(absDir, "fixture-manifest.json")
	for _, path := range []string{mainPath, factsPath, manifestPath} {
		if _, statErr := os.Stat(path); statErr == nil {
			t.Fatalf("验收目标已存在，拒绝覆盖: %s", path)
		} else if !os.IsNotExist(statErr) {
			t.Fatal(statErr)
		}
	}

	cfg := Settings{
		StorePath:                   mainPath,
		UsageFactsStorePath:         factsPath,
		UsageFactsBackfillDays:      days,
		UsageFactsRetentionDays:     min(days+34, 732),
		UsageFactsHourRetentionDays: min(8, days),
		UsageFactsReadEnabled:       true,
		UsageFactsLocalReadOnly:     true,
		LocalSnapshotOnly:           true,
		UsageFactsLagMinutes:        10,
		UsageFactsQueryTimeoutSec:   30,
		SessionSecret:               "local-acceptance-session-only",
	}
	m := &Monitor{cfg: cfg, chNames: map[string]string{}}
	if err := m.openStore(mainPath); err != nil {
		t.Fatal(err)
	}
	defer closeLocalAcceptanceStores(m)

	passwordHash, err := hashPassword(localAcceptancePortalPassword)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().In(usageCST)
	generatedAt := now.Unix()
	group := CustomerGroup{
		ID: 1, Name: "本地董事验收组", Note: "synthetic local acceptance only",
		PortalEmail: localAcceptancePortalEmail, PortalPwAdmin: passwordHash, PortalAuthVer: 1,
		CreatedAt: generatedAt,
	}
	if err := m.storeDB.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	tracked := make([]TrackedUser, 0, members)
	ids := make([]int64, 0, members)
	for i := 1; i <= members; i++ {
		username := localAcceptanceUsername(i, members)
		tracked = append(tracked, TrackedUser{
			UserID: int64(i), Username: username, Email: fmt.Sprintf("user-%04d@local.test", i),
			GroupID: 1, Note: "synthetic", AddedAt: generatedAt + int64(i),
		})
		ids = append(ids, int64(i))
	}
	if err := m.storeDB.CreateInBatches(tracked, 200).Error; err != nil {
		t.Fatal(err)
	}

	end := m.usageFactFinalizedHour(now)
	start := end - int64(days)*usageFactDaySeconds
	if err := seedLocalAcceptanceFactMetadata(m, ids, start, end, days, dimensions, generatedAt); err != nil {
		t.Fatal(err)
	}
	counts, err := writeLocalAcceptanceFacts(m, members, dimensions, start, end, generatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkpointAndVerifyLocalAcceptanceStore(m.usageFactsStore()); err != nil {
		t.Fatal(err)
	}
	if err := checkpointAndVerifyLocalAcceptanceStore(m.storeDB); err != nil {
		t.Fatal(err)
	}
	mainInfo, err := os.Stat(mainPath)
	if err != nil {
		t.Fatal(err)
	}
	factsInfo, err := os.Stat(factsPath)
	if err != nil {
		t.Fatal(err)
	}
	manifest := localAcceptanceFixtureManifest{
		GeneratedAt: now.Format(time.RFC3339), MainStore: mainPath, FactsStore: factsPath,
		Members: members, Days: days, Dimensions: dimensions, RangeStart: start, PublishedThrough: end,
		DailyFacts: counts.dailyFacts, HourFacts: counts.hourFacts,
		MemberHourProofs: counts.memberHourProofs, MemberDayProofs: counts.memberDayProofs,
		MainBytes: mainInfo.Size(), FactsBytes: factsInfo.Size(), PortalEmail: localAcceptancePortalEmail,
		PortalPassword: localAcceptancePortalPassword, SessionSecret: cfg.SessionSecret,
		ExpectedMatrixMaxDay: usageMatrixMaxCells / members,
	}
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	// Docker 验收镜像使用 uid=1000；这个临时目录只含纯合成数据。
	if err := os.Chmod(absDir, 0o777); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{mainPath, factsPath} {
		if err := os.Chmod(path, 0o666); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("fixture=%s members=%d days=%d daily=%d hour=%d member_hours=%d facts_bytes=%d",
		absDir, members, days, counts.dailyFacts, counts.hourFacts, counts.memberHourProofs, factsInfo.Size())
}

func localAcceptanceEnvInt(name string, fallback, minValue, maxValue int) int {
	v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name)))
	if err != nil || v < minValue || v > maxValue {
		return fallback
	}
	return v
}

func localAcceptanceUsername(id, members int) string {
	if id == members {
		return "acceptance-user-with-a-very-long-visible-name-for-browser-layout-verification-" + strconv.Itoa(id)
	}
	return fmt.Sprintf("acceptance-user-%04d", id)
}

func closeLocalAcceptanceStores(m *Monitor) {
	if m == nil {
		return
	}
	if m.usageFactsDB != nil && m.usageFactsDB != m.storeDB {
		if db, err := m.usageFactsDB.DB(); err == nil {
			_ = db.Close()
		}
	}
	if m.storeDB != nil {
		if db, err := m.storeDB.DB(); err == nil {
			_ = db.Close()
		}
	}
}

func seedLocalAcceptanceFactMetadata(m *Monitor, ids []int64, start, end int64, days, dimensions int, now int64) error {
	fingerprint := portalMemberFingerprintFromIDs(ids)
	members := make([]UsageFactMemberState, 0, len(ids))
	published := make([]UsageFactPublishedMember, 0, len(ids))
	users := make([]UsageUserSnapshot, 0, len(ids))
	tokens := make([]UsageTokenSnapshot, 0, len(ids))
	for _, id := range ids {
		members = append(members, UsageFactMemberState{
			UserID: id, Active: true, BackfillWindowDays: days, RangeStart: start,
			NextBackfillHour: end, LastSyncAt: now, UpdatedAt: now,
		})
		published = append(published, UsageFactPublishedMember{UserID: id, PublishedAt: now})
		users = append(users, UsageUserSnapshot{
			UserID: id, Username: localAcceptanceUsername(int(id), len(ids)),
			Email: fmt.Sprintf("user-%04d@local.test", id), BalanceQuota: 50_000_000,
			UsedQuota: 1_000_000 + id*1000, Exists: true, CapturedAt: now,
		})
		for dimension := 0; dimension < dimensions; dimension++ {
			tokenID := id*10 + int64(dimension+1)
			tokens = append(tokens, UsageTokenSnapshot{
				TokenID: tokenID, UserID: id, Name: fmt.Sprintf("token-%d-%d", id, dimension+1),
				MaskedKey: fmt.Sprintf("sk-***%04d", tokenID%10000), Grp: fmt.Sprintf("group-%d", dimension%2+1),
				UsedQuota: int64(500_000 + dimension*10_000), CapturedAt: now,
			})
		}
	}
	if err := m.usageFactsStore().CreateInBatches(members, 200).Error; err != nil {
		return err
	}
	if err := m.usageFactsStore().CreateInBatches(published, 200).Error; err != nil {
		return err
	}
	if err := m.usageFactsStore().CreateInBatches(users, 200).Error; err != nil {
		return err
	}
	if err := m.usageFactsStore().CreateInBatches(tokens, 200).Error; err != nil {
		return err
	}
	return m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = ?", 1).Updates(map[string]any{
		"generation": 1, "serving_generation": 1, "member_fingerprint": fingerprint,
		"backfill_window_days": days, "next_backfill_hour": end, "next_reconcile_hour": start,
		"published_fingerprint": fingerprint, "published_window_days": days,
		"published_range_start": start, "published_through": end, "published_at": now,
		"last_fact_sync_at": now, "last_profile_sync_at": now,
	}).Error
}

type localAcceptanceFactCounts struct {
	dailyFacts       int64
	hourFacts        int64
	memberHourProofs int64
	memberDayProofs  int64
}

func writeLocalAcceptanceFacts(m *Monitor, members, dimensions int, start, end, now int64) (localAcceptanceFactCounts, error) {
	db, err := m.usageFactsStore().DB()
	if err != nil {
		return localAcceptanceFactCounts{}, err
	}
	if _, err := db.Exec("PRAGMA synchronous=NORMAL"); err != nil {
		return localAcceptanceFactCounts{}, err
	}
	tx, err := db.Begin()
	if err != nil {
		return localAcceptanceFactCounts{}, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	dailyStmt, err := tx.Prepare(`INSERT INTO usage_daily_facts
 (date_ts,user_id,channel_id,grp,model_name,token_id,token_name,requests,refund_records,prompt_tokens,completion_tokens,consume_quota,refund_quota)
 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return localAcceptanceFactCounts{}, err
	}
	defer dailyStmt.Close()
	dayProofStmt, err := tx.Prepare(`INSERT INTO usage_fact_member_day_states
 (user_id,date_ts,rows,requests,tokens,content_hash,updated_at) VALUES (?,?,?,?,?,?,?)`)
	if err != nil {
		return localAcceptanceFactCounts{}, err
	}
	defer dayProofStmt.Close()
	hourStmt, err := tx.Prepare(`INSERT INTO usage_hour_facts
 (hour_ts,user_id,channel_id,grp,model_name,token_id,day_ts,token_name,requests,refund_records,prompt_tokens,completion_tokens,consume_quota,refund_quota)
 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return localAcceptanceFactCounts{}, err
	}
	defer hourStmt.Close()
	hourProofStmt, err := tx.Prepare(`INSERT INTO usage_fact_member_hour_states
 (user_id,hour_ts,status,rows,requests,tokens,content_hash,attempts,updated_at,completed_at,last_error,lease_token,lease_until)
 VALUES (?,?,'complete',?,?,?,?,1,?,?,'','',0)`)
	if err != nil {
		return localAcceptanceFactCounts{}, err
	}
	defer hourProofStmt.Close()

	var counts localAcceptanceFactCounts
	firstDay, throughDay := usageFactSemanticFullDayRange(start, end)
	for day := firstDay; day < throughDay; day += usageFactDaySeconds {
		for user := 1; user <= members; user++ {
			rows := localAcceptanceDailyRows(day, int64(user), dimensions)
			for _, row := range rows {
				if _, err := dailyStmt.Exec(row.DateTs, row.UserID, row.ChannelID, row.Grp, row.ModelName,
					row.TokenID, row.TokenName, row.Requests, row.RefundRecords, row.PromptTokens,
					row.CompletionTokens, row.ConsumeQuota, row.RefundQuota); err != nil {
					return counts, err
				}
				counts.dailyFacts++
			}
			metrics := dailyFactsMetrics(rows)
			if _, err := dayProofStmt.Exec(user, day, len(rows), metrics.Requests, metrics.tokens(),
				usageDailyFactContentHash(rows), now); err != nil {
				return counts, err
			}
			counts.memberDayProofs++
		}
	}

	retainedDays := min(int64(8), (end-start)/usageFactDaySeconds)
	hourCutoff := usageFactDayStart(time.Unix(now, 0).In(usageCST).AddDate(0, 0, -int(retainedDays)).Unix())
	for hour := start; hour < end; hour += usageFactHourSeconds {
		day := usageFactDayStart(hour)
		hourOfDay := time.Unix(hour, 0).In(usageCST).Hour()
		for user := 1; user <= members; user++ {
			rows := localAcceptanceHourRows(hour, day, hourOfDay, int64(user), dimensions)
			if hour >= hourCutoff {
				for _, row := range rows {
					if _, err := hourStmt.Exec(row.HourTs, row.UserID, row.ChannelID, row.Grp, row.ModelName,
						row.TokenID, row.DayTs, row.TokenName, row.Requests, row.RefundRecords,
						row.PromptTokens, row.CompletionTokens, row.ConsumeQuota, row.RefundQuota); err != nil {
						return counts, err
					}
					counts.hourFacts++
				}
			}
			metrics := factsMetrics(rows)
			if _, err := hourProofStmt.Exec(user, hour, len(rows), metrics.Requests, metrics.tokens(),
				usageFactContentHash(rows), now, now); err != nil {
				return counts, err
			}
			counts.memberHourProofs++
		}
	}
	if err := tx.Commit(); err != nil {
		return counts, err
	}
	committed = true
	return counts, nil
}

func localAcceptanceHourRows(hour, day int64, hourOfDay int, userID int64, dimensions int) []UsageHourFact {
	rows := make([]UsageHourFact, 0, dimensions)
	for dimension := 0; dimension < dimensions; dimension++ {
		refund := int64(0)
		refundQuota := int64(0)
		if dimension == 0 && hourOfDay == 3 {
			refund = 1
			refundQuota = 100
		}
		rows = append(rows, UsageHourFact{
			HourTs: hour, DayTs: day, UserID: userID, ChannelID: int64(dimension + 1),
			Grp: fmt.Sprintf("group-%d", dimension%2+1), ModelName: fmt.Sprintf("model-%d", dimension+1),
			TokenID: userID*10 + int64(dimension+1), TokenName: fmt.Sprintf("token-%d-%d", userID, dimension+1),
			Requests: 1, RefundRecords: refund, PromptTokens: int64(10 + int(userID)%5 + dimension),
			CompletionTokens: int64(2 + dimension), ConsumeQuota: int64(1000*(dimension+1)) + userID%13,
			RefundQuota: refundQuota,
		})
	}
	return rows
}

func localAcceptanceDailyRows(day, userID int64, dimensions int) []UsageDailyFact {
	rows := make([]UsageDailyFact, 0, dimensions)
	for dimension := 0; dimension < dimensions; dimension++ {
		refund := int64(0)
		refundQuota := int64(0)
		if dimension == 0 {
			refund = 1
			refundQuota = 100
		}
		rows = append(rows, UsageDailyFact{
			DateTs: day, UserID: userID, ChannelID: int64(dimension + 1),
			Grp: fmt.Sprintf("group-%d", dimension%2+1), ModelName: fmt.Sprintf("model-%d", dimension+1),
			TokenID: userID*10 + int64(dimension+1), TokenName: fmt.Sprintf("token-%d-%d", userID, dimension+1),
			Requests: 24, RefundRecords: refund,
			PromptTokens:     int64(24 * (10 + int(userID)%5 + dimension)),
			CompletionTokens: int64(24 * (2 + dimension)),
			ConsumeQuota:     24 * (int64(1000*(dimension+1)) + userID%13), RefundQuota: refundQuota,
		})
	}
	return rows
}

func checkpointAndVerifyLocalAcceptanceStore(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("store is nil")
	}
	if err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)").Error; err != nil {
		return err
	}
	var result string
	if err := db.Raw("PRAGMA quick_check").Row().Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("SQLite quick_check=%s", result)
	}
	return nil
}
