package monitor

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func recordTestDay() int64 { return time.Date(2026, 9, 6, 0, 0, 0, 0, cstLocation).Unix() }
func recordTestEnvelope(day int64, key int, items []map[string]any, more bool, cursor string) map[string]any {
	date := time.Unix(day, 0).In(cstLocation).Format("2006-01-02")
	return map[string]any{"data": map[string]any{"api_key_id": key, "period": map[string]string{"start": date, "end": date}, "group_by": "record", "records": items, "has_more": more, "next_cursor": cursor}}
}
func recordTestItem(day int64, id int, hour int, cost string, tokens int) map[string]any {
	return map[string]any{"id": id, "created_at": time.Unix(day+int64(hour)*3600+1800, 0).UTC().Format(time.RFC3339Nano), "cost": cost, "total_tokens": tokens}
}

func TestAICodeWithRecordExactMoney(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want int64
		bad  bool
	}{
		{`"0.0325"`, 32500000, false}, {`"0.000000001"`, 1, false}, {`1e-9`, 1, false}, {`0`, 0, false},
		{`"0.0000000001"`, 0, true}, {`"-1"`, 0, true}, {`null`, 0, true}, {`"NaN"`, 0, true},
		{`"1/2"`, 0, true}, {`"0x1"`, 0, true}, {`"1e999999999"`, 0, true}, {`"9999999999"`, 0, true},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := aiCodeWithExactCost(json.RawMessage(tc.raw))
			if (err != nil) != tc.bad || (!tc.bad && got != tc.want) {
				t.Fatalf("got=%d err=%v", got, err)
			}
		})
	}
}

func TestAICodeWithRecordSchemaValidation(t *testing.T) {
	day := recordTestDay()
	for _, name := range []string{"valid", "empty", "missing_records", "null_records", "missing_more", "wrong_key", "wrong_date", "wrong_group", "missing_cursor", "trailing_cursor", "empty_more", "negative_cost", "fraction_tokens", "missing_id", "timezone", "outside_day"} {
		t.Run(name, func(t *testing.T) {
			r := recordTestItem(day, 1, 1, "0.0325", 36173)
			env := recordTestEnvelope(day, 17, []map[string]any{r}, false, "")
			d := env["data"].(map[string]any)
			switch name {
			case "empty":
				d["records"] = []map[string]any{}
			case "missing_records":
				delete(d, "records")
			case "null_records":
				d["records"] = nil
			case "missing_more":
				delete(d, "has_more")
			case "wrong_key":
				d["api_key_id"] = 0
			case "wrong_date":
				d["period"] = map[string]string{"start": "2026-09-05", "end": "2026-09-06"}
			case "wrong_group":
				d["group_by"] = "day"
			case "missing_cursor":
				d["has_more"] = true
			case "trailing_cursor":
				d["next_cursor"] = "cursor"
			case "empty_more":
				d["records"] = []map[string]any{}
				d["has_more"] = true
				d["next_cursor"] = "cursor"
			case "negative_cost":
				r["cost"] = "-1"
			case "fraction_tokens":
				r["total_tokens"] = 1.5
			case "missing_id":
				delete(r, "id")
			case "timezone":
				r["created_at"] = "2026-09-06 01:30:00"
			case "outside_day":
				r["created_at"] = "2026-09-06T23:30:00Z"
			}
			body, _ := json.Marshal(env)
			page, err := decodeAICodeWithRecordPage(body, day)
			valid := name == "valid" || name == "empty"
			if (err == nil) != valid {
				t.Fatalf("page=%+v err=%v", page, err)
			}
			if name == "valid" && (page.Records[0].CostUnits != 32500000 || page.Records[0].CreatedAt != day+5400) {
				t.Fatal(page)
			}
		})
	}
}

func TestAICodeWithRecordPersistedSchemaContainsNoCredentials(t *testing.T) {
	for _, value := range []any{AICodeWithRecordCheckpoint{}, AICodeWithRecordSeen{}} {
		text := strings.ToLower(fmt.Sprintf("%+v", value))
		for _, secret := range []string{"password", "authorization", "apikey", "secret", "rawlog"} {
			if strings.Contains(text, secret) {
				t.Fatalf("unexpected persisted field %s", secret)
			}
		}
	}
}
