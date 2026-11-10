package monitor

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTokenAcquisitionRecordedTimeRules(t *testing.T) {
	s := tokenMeteringConfig()
	p := tokenRC48TestPolicy(s.TokenFactsSourceEpoch, 100)
	s.TokenDetectionPolicy = tokenDetectionJSON(p)
	condition := `hour("Asia/Shanghai") >= 14 && hour("Asia/Shanghai") < 18`
	expr := `tier("base",p*4+cr*0.4+cc*5+c*20)*(` + condition + `?2:1)`
	for _, matched := range []bool{false, true} {
		f := tokenRC48RuleFixture(t, s, 1, 101, 100, 0, 0, map[string]any{"expr_b64": base64.StdEncoding.EncodeToString([]byte(expr)), "request_rules": []map[string]any{{"cond": condition, "matched": matched, "multiplier": 2}}, "response_model": map[string]string{"requested_model": "model", "returned_model": "actual-model"}})
		u := tokenMeteringDecode(t, f)
		price := tokenPrice(u, p, big.NewRat(1, 1))
		want := "200"
		if matched {
			want = "400"
		}
		if !price.Supported || price.Quota != want || price.Method != "newapi_rc48_recorded_rules_float64_v1" {
			t.Fatalf("recorded %t: %+v fields=%+v", matched, price, u.Fields)
		}
		if u.Fields["response_model"].Value != "actual-model" {
			t.Fatal("structured returned model lost")
		}
		// Contract-free and mismatched traces cannot authorize the calculation.
		u.Contract = nil
		if tokenPrice(u, p, big.NewRat(1, 1)).Supported {
			t.Fatal("unbound producer allowed")
		}
	}
	for _, raw := range []string{
		`[{"cond":"hour(\"Asia/Shanghai\")>=14&&hour(\"Asia/Shanghai\")<18","matched":true,"multiplier":3}]`,
		`[{"cond":"hour(\"Asia/Shanghai\")>=14&&hour(\"Asia/Shanghai\")<18","multiplier":2}]`,
		`[{"cond":"header(\"Authorization\")==\"private\"","matched":true,"multiplier":2}]`,
	} {
		rules := tokenMeteringRules([]byte(raw))
		field := tokenMeteringRecordedExpression([]byte(tokenDetectionJSON(base64.StdEncoding.EncodeToString([]byte(expr)))), rules)
		if field.State == "valid" {
			t.Fatal("unverified branch accepted", field)
		}
		if rules.State == "valid" && rules.Kind != "recorded_time_rules_v1" {
			t.Fatal(rules)
		}
	}
}

func TestTokenAcquisitionDiscoveryCommittedPrefix(t *testing.T) {
	rows := make([]tokenDetectionScanRow, 512)
	jobs := make([]TokenMeteringFact, 250)
	for i := range rows {
		rows[i] = tokenDetectionScanRow{LogID: int64(i + 1), CreatedAt: 100, Changed: true, HeadPresent: true}
	}
	for i := range jobs {
		jobs[i] = TokenMeteringFact{LogID: int64(i + 1)}
	}
	at, id, ok := tokenDetectionRecentCommittedPrefix(rows, jobs)
	if !ok || at != 100 || id != 250 {
		t.Fatalf("512 discoveries/250 commits stuck: %d %d %t", at, id, ok)
	}
	at, id, ok = tokenDetectionRecentCommittedPrefix(rows, jobs[1:])
	if ok || at != 0 || id != 0 {
		t.Fatal("skipped uncommitted first identity")
	}
	rows[0].Changed = false
	at, id, ok = tokenDetectionRecentCommittedPrefix(rows, jobs[1:])
	if !ok || id != 250 {
		t.Fatal("unchanged prefix not traversed")
	}
	at, id, ok = tokenDetectionRecentCommittedPrefix(rows[:200], jobs)
	if !ok || at != 0 || id != 0 {
		t.Fatal("completed bounded discovery should wrap")
	}
	rows[0].SourceEpoch, rows[0].Changed = "other-epoch", true
	if _, _, ok := tokenDetectionRecentCommittedPrefix(rows, jobs); ok {
		t.Fatal("log id from another epoch advanced cursor")
	}
}

func TestTokenAcquisitionSourceRepairUsesDurableWindow(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	defer m.Close()
	db, epoch := m.tokenStore(), m.cfg.TokenFactsSourceEpoch
	start := now - 1800
	state := tokenMeteringStateRead(t, m)
	state.StartAt, state.ScanThrough, state.CompleteThrough = start, start+300, start+300
	state.LeaseToken, state.LeaseUntil = "repair-test", now+600
	if err := db.Save(&state).Error; err != nil {
		t.Fatal(err)
	}
	w := TokenMeteringWindow{SourceEpoch: epoch, StartAt: start, EndAt: start + 300, CursorAt: start + 299, CursorID: 10, Done: true, SourceComplete: true, SealStarted: true, NextAt: now + 1800}
	if err := db.Create(&w).Error; err != nil {
		t.Fatal(err)
	}
	if err := tokenMeteringRequestSourceReplay(db, TokenMeteringFact{SourceEpoch: epoch, CreatedAt: start + 10}, now); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&TokenMeteringLaneState{SourceEpoch: epoch, Lane: tokenMeteringLaneRealtimeTail, WindowStart: now}).Error; err != nil {
		t.Fatal(err)
	}
	w, ok, err := m.nextTokenMeteringWork(context.Background(), epoch, state.LeaseToken, now, tokenMeteringLaneRecent)
	if err != nil || !ok || w.CursorAt != start || w.CursorID != 0 || !w.Replay || w.Done || w.SourceComplete {
		t.Fatal("repair delayed or falsely complete", w, ok, err)
	}
	if tokenMeteringStateRead(t, m).CompleteThrough > start {
		t.Fatal("old coverage survived repair")
	}
	// The next reserved recent turn must return to new arrivals.
	tail, ok, err := m.nextTokenMeteringWork(context.Background(), epoch, state.LeaseToken, now, tokenMeteringLaneRecent)
	if err != nil || !ok || tail.StartAt != now {
		t.Fatal("repair blocked tail", tail, ok, err)
	}
	// Simulate a committed partial replay and a real source failure. Restart
	// must preserve the prefix and backoff, then continue from it when due.
	w.CursorAt, w.CursorID, w.NextAt, w.LastReason, w.Attempts = start+10, 3, now+30, "source_read_failed", 1
	if err := db.Save(&w).Error; err != nil {
		t.Fatal(err)
	}
	if _, ok, err := tokenMeteringRecentSourceRepair(db, state, now); err != nil || ok {
		t.Fatal("failure backoff bypassed", ok, err)
	}
	resumed, ok, err := tokenMeteringRecentSourceRepair(db, state, now+31)
	if err != nil || !ok || resumed.CursorAt != w.CursorAt || resumed.CursorID != w.CursorID {
		t.Fatal("partial prefix rewound", resumed, err)
	}
	// Completing through the normal collection path clears the same repair
	// identity and keeps source rows deduplicated across another process.
	tokenMeteringSourceRow(t, m, 4, start+11, "source-repair-request")
	for i := int64(0); i < 4; i++ {
		if _, err := m.runTokenMeteringRound(context.Background(), time.Unix(now+700+i*10, 0)); err != nil {
			t.Fatal(err)
		}
	}
	var count int64
	if err := db.Model(&TokenMeteringFact{}).Where("source_epoch=? AND log_id=4", epoch).Count(&count).Error; err != nil || count != 1 {
		t.Fatal("replay facts", count, err)
	}
	if err := db.Table("token_metering_source_repairs_v1").Where("source_epoch=? AND start_at=?", epoch, start).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("completed repair retained", count, err)
	}
}

func TestTokenAcquisitionDenseDependencyRepairAdvancesDuringRebuild(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	m.cfg.TokenProcessingBatchRows = 250
	base := detectionFact(t, m, 1, now-600, 3, 100, 0, 0)
	db := m.tokenStore()
	if err := db.Delete(&TokenMeteringFact{}, "source_epoch=?", base.SourceEpoch).Error; err != nil {
		t.Fatal(err)
	}
	var facts []TokenMeteringFact
	var heads []TokenDetectionCurrent
	for id := int64(1); id <= 600; id++ {
		f := base
		f.LogID, f.UserID = id, id
		facts = append(facts, f)
		heads = append(heads, TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: id, CreatedAt: f.CreatedAt, Revision: 1, FactRevision: f.Revision, LinkRevision: f.LinkRevision, PolicyHash: "old-policy", ResultJSON: "{}"})
	}
	if err := db.CreateInBatches(facts, 100).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.CreateInBatches(heads, 100).Error; err != nil {
		t.Fatal(err)
	}
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("UPDATE " + tokenPendingMigrationTable + " SET ready=0").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TRIGGER reject_repair BEFORE UPDATE ON token_detection_currents WHEN NEW.log_id=20 BEGIN SELECT RAISE(ABORT,'injected decision failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if count, err := m.runTokenDetectionRound(context.Background(), now); err == nil || count == 0 {
		t.Fatal("injected failure did not preserve a partial prefix", count, err)
	}
	var failedState TokenDetectionState
	if err := db.First(&failedState, "source_epoch=?", base.SourceEpoch).Error; err != nil || failedState.RecentScanID == 0 || failedState.RecentScanID >= 20 {
		t.Fatal("uncommitted discovery was published", failedState, err)
	}
	if err := db.Exec(`DROP TRIGGER reject_repair`).Error; err != nil {
		t.Fatal(err)
	}
	for round := int64(0); round < 8; round++ {
		if _, err := m.runTokenDetectionRound(context.Background(), now+15+round*15); err != nil {
			t.Fatal(err)
		}
		var remaining int64
		if err := db.Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND policy_hash<>?", base.SourceEpoch, tokenDetectionPolicyHash(detectionPolicy())).Count(&remaining).Error; err != nil {
			t.Fatal(err)
		} else if remaining == 0 {
			break
		}
	}
	var pending int64
	if err := db.Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND policy_hash<>?", base.SourceEpoch, tokenDetectionPolicyHash(detectionPolicy())).Count(&pending).Error; err != nil || pending != 0 {
		t.Fatal("middle identities starved", pending, err)
	}
	if ready, err := tokenPendingProjectionReady(db); err != nil || ready {
		t.Fatal("global readiness changed", ready, err)
	}
}

func TestTokenAcquisitionCapturedPriceReplay(t *testing.T) {
	file := os.Getenv("TOKEN_ACQUISITION_CAPTURE")
	if file == "" {
		t.Skip("bounded production capture not supplied")
	}
	var sample struct {
		Rows []struct {
			ID, CreatedAt, UserID, ChannelID                 int64
			Input, Output, Quota, Stream, UseTime, RequestID string
			Other                                            json.RawMessage
		}
	}
	// Source column names intentionally mirror the capture, not Go casing.
	var raw struct{ Rows []map[string]json.RawMessage }
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	for _, row := range raw.Rows {
		encoded := map[string]json.RawMessage{}
		for k, v := range row {
			encoded[k] = v
		}
		for a, b := range map[string]string{"created_at": "CreatedAt", "user_id": "UserID", "channel_id": "ChannelID", "request_id": "RequestID", "use_time": "UseTime"} {
			encoded[b] = encoded[a]
		}
		body, _ := json.Marshal(encoded)
		var next = struct {
			ID, CreatedAt, UserID, ChannelID                 int64
			Input, Output, Quota, Stream, UseTime, RequestID string
			Other                                            json.RawMessage
		}{}
		if err = json.Unmarshal(body, &next); err != nil {
			t.Fatal(err)
		}
		sample.Rows = append(sample.Rows, next)
	}
	s := tokenMeteringConfig()
	policyBody, err := os.ReadFile(filepath.Join(filepath.Dir(file), "policy.json"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := parseTokenDetectionPolicy(string(policyBody))
	if err != nil || p.SourceContract == nil {
		t.Fatal("captured source contract invalid", err)
	}
	s.TokenFactsSourceEpoch, s.TokenDetectionPolicy = p.SourceContract.Epoch, string(policyBody)
	proof := []map[string]any{}
	repaired := 0
	for _, r := range sample.Rows {
		v := func(s string) sql.NullString { return sql.NullString{String: s, Valid: s != ""} }
		input := tokenMeteringRaw{ID: r.ID, CreatedAt: r.CreatedAt, UserID: r.UserID, ChannelID: r.ChannelID, RequestID: r.RequestID, Input: v(r.Input), Output: v(r.Output), Quota: v(r.Quota), Stream: v(r.Stream), UseTime: v(r.UseTime), Other: v(string(r.Other))}
		f, _ := parseTokenMetering(s, input, r.CreatedAt+1)
		u := tokenMeteringDecode(t, f)
		price := tokenPrice(u, p, big.NewRat(1, 1))
		if !price.Supported || price.Quota != r.Quota {
			t.Errorf("log %d: recorded=%s price=%+v", r.ID, r.Quota, price)
		}
		if price.Method == "newapi_rc48_recorded_rules_float64_v1" {
			repaired++
		}
		proof = append(proof, map[string]any{"log_id": r.ID, "request_id": r.RequestID, "recorded_quota": r.Quota, "price": price, "derived": u.ContractOperands, "rules": u.Fields["request_rules"]})
	}
	if len(proof) == 0 || repaired == 0 {
		t.Fatal("no real affected requests replayed")
	}
	if out := os.Getenv("TOKEN_ACQUISITION_REPORT"); out != "" {
		b, _ := json.MarshalIndent(proof, "", "  ")
		if err = os.WriteFile(out, b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("source rows=%d recorded-time-rule repairs=%d", len(proof), repaired)
}
