package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestMonthlyEvidenceStrictFieldNames(t *testing.T) {
	now := time.Now()
	start, end, _ := monthlyWindow("", now)
	e := monthlyEvidenceFixture(1, start.Format("2006-01"), now)
	e.ReviewedBy = "本机核验人"
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeMonthlyBillEvidence(raw); err != nil {
		t.Fatal("valid Unicode values must remain accepted", err)
	}
	empty := e
	empty.Users = []MonthlyBillUserEvidence{}
	shape, err := json.Marshal(empty)
	if err != nil {
		t.Fatal(err)
	}
	bad := map[string]string{
		"false attestation shadowed": strings.Replace(string(raw), `"complete":true`, `"complete":false,"COMPLETE":true`, 1),
		"reverse alias order":        strings.Replace(string(raw), `"complete":true`, `"COMPLETE":false,"complete":true`, 1),
		"money alias":                strings.Replace(string(raw), `"consume_quota":250000000`, `"consume_quota":250000000,"CONSUME_QUOTA":0`, 1),
		"event alias":                strings.Replace(string(raw), `"delta_quota":500000`, `"delta_quota":500000,"DELTA_QUOTA":1`, 1),
		"reversal alias":             strings.Replace(string(raw), `"credit_id":3`, `"credit_id":3,"CREDIT_ID":2`, 1),
		"Unicode fold alias":         strings.Replace(string(raw), `"users":`, `"uſers":`, 1),
		"escaped duplicate":          strings.Replace(string(raw), `"complete":true`, `"complete":true,"\u0063omplete":true`, 1),
		"lowercase unknown":          strings.Replace(string(raw), `"version":2`, `"version":2,"unexpected":0`, 1),
		"missing amounts shadowed": string(shape[:len(shape)-1]) + fmt.Sprintf(
			`,"USERS":[{"user_id":43,"complete":true,"wallet_only":true,"through":%d}]}`, end.Unix()),
	}
	m, _, _ := newPortalTestMonitor(t)
	g := CustomerGroup{ID: 1, Name: "synthetic", PortalEmail: "review@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 43, GroupID: g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	for name, input := range bad {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeMonthlyBillEvidence([]byte(input)); err == nil {
				t.Fatal("ambiguous evidence accepted by decoder")
			}
			for _, apply := range []bool{false, true} {
				if _, err := ImportMonthlyBill(context.Background(), m.storeDB, []byte(input), apply, now); err == nil {
					t.Fatal("ambiguous evidence accepted by publication")
				}
			}
		})
	}
	var count int64
	if err := m.storeDB.Model(&PortalMonthlyBill{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("invalid evidence wrote a bill: count=%d err=%v", count, err)
	}
}

func TestMonthlyEvidenceMissingAmountsStillRejected(t *testing.T) {
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	e := monthlyEvidenceFixture(1, start.Format("2006-01"), now)
	raw, _ := json.Marshal(e)
	var shape map[string]json.RawMessage
	if err := json.Unmarshal(raw, &shape); err != nil {
		t.Fatal(err)
	}
	var users []map[string]json.RawMessage
	if err := json.Unmarshal(shape["users"], &users); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"opening_quota", "closing_quota", "reverse_closing_quota", "consume_quota", "refund_quota", "requests", "tokens"} {
		t.Run(key, func(t *testing.T) {
			saved := users[0][key]
			defer func() { users[0][key] = saved }()
			for _, null := range []bool{false, true} {
				delete(users[0], key)
				if null {
					users[0][key] = json.RawMessage(`null`)
				}
				shape["users"], _ = json.Marshal(users)
				b, _ := json.Marshal(shape)
				if _, err := decodeMonthlyBillEvidence(b); err == nil {
					t.Fatalf("missing/null %s became zero", key)
				}
			}
		})
	}
}
