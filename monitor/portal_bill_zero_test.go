package monitor

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func zeroBillTestMetadata() string {
	raw, _ := json.Marshal(map[string]any{"billing_mode": "tiered_expr", "expr_b64": base64.StdEncoding.EncodeToString([]byte(`tier("base",p*4+c*20+cr*0.2)`)), "matched_tier": "base", "billing_source": "wallet", "group_ratio": 0.6, "cache_tokens": 0, "request_path": "/v1/responses"})
	return string(raw)
}

func TestPortalBillOmitOnlyProvenAllZeroText(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  func() LogRow
		omit bool
	}{
		{"reported gpt text", func() LogRow {
			r := LogRow{Type: 2, ModelName: "gpt-6-astra", billingFactsValid: true}
			populatePortalLogEvidence(&r, zeroBillTestMetadata())
			return r
		}, true},
		{"anthropic all zero", func() LogRow { return billSampleRow(t, 0, 0, 0, 0, 0, 0, 0) }, true},
		{"charged despite zero tokens", func() LogRow { return billSampleRow(t, 0, 0, 0, 0, 0, 0, 1) }, false},
		{"free output usage", func() LogRow { return billSampleRow(t, 0, 1, 0, 0, 0, 0, 0) }, false},
		{"free input usage", func() LogRow { return billSampleRow(t, 1, 0, 0, 0, 0, 0, 0) }, false},
		{"cache read only", func() LogRow { return billSampleRow(t, 0, 0, 10, 0, 0, 0, 0) }, false},
		{"cache write only", func() LogRow { return billSampleRow(t, 0, 0, 0, 10, 0, 0, 0) }, false},
		{"unknown original quota/tokens", func() LogRow {
			r := billSampleRow(t, 0, 0, 0, 0, 0, 0, 0)
			r.SourceFacts = &portalLogSourceFacts{PromptPresent: false, CompletionPresent: true, QuotaPresent: true}
			return r
		}, false},
		{"unknown pricing", func() LogRow { return LogRow{Type: 2, billingFactsValid: true} }, false},
		{"per request free group", func() LogRow {
			r := LogRow{Type: 2, billingFactsValid: true}
			populatePortalLogEvidence(&r, `{"model_price":1,"group_ratio":0,"cache_tokens":0,"cache_creation_tokens":0,"usage_semantic":"openai","request_path":"/v1/responses"}`)
			return r
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.row()
			before, _ := json.Marshal(r.billEvidence)
			record, err := portalBillRecord(r)
			if err != nil {
				t.Fatal(err)
			}
			if got := portalBillOmitZeroRecord(r, record); got != tc.omit {
				t.Fatalf("omit=%v want=%v record=%v", got, tc.omit, record)
			}
			after, _ := json.Marshal(r.billEvidence)
			if !bytes.Equal(before, after) {
				t.Fatal("export rule mutated original evidence")
			}
		})
	}
}

func TestPortalBillZeroFilteringPreservesSourceProofAndPagination(t *testing.T) {
	zero := billSampleRow(t, 0, 0, 0, 0, 0, 0, 0)
	zero.ID, zero.CreatedAt = 3, 3
	paid := billSampleRow(t, 100, 0, 0, 0, 0, 0, 120)
	paid.ID, paid.CreatedAt = 2, 2
	last := zero
	last.ID, last.CreatedAt = 1, 1
	for _, allZero := range []bool{false, true} {
		rows := []LogRow{zero, paid, last}
		quota := "120"
		if allZero {
			rows[1] = last
			rows[1].ID, rows[1].CreatedAt = 2, 2
			quota = "0"
		}
		var dst bytes.Buffer
		index, checks := 0, 0
		n, err := portalBuildVerifiedBill(context.Background(), &dst, portalBillBuildPlan{3, quota, 3, 1},
			func(_ context.Context, cursor logPageCursor, limit int) ([]LogRow, error) {
				if limit != 1 || (index > 0 && cursor.BeforeID != rows[index-1].ID) {
					t.Fatal("zero rows stopped the source cursor")
				}
				if index == len(rows) {
					return nil, nil
				}
				r := rows[index]
				index++
				return []LogRow{r}, nil
			}, func(context.Context) error { checks++; return nil }, func(scanned int64) error {
				if scanned < 1 || scanned > 3 {
					t.Fatal("source progress invalid")
				}
				return nil
			})
		if err != nil || n != 3 || checks != 2 || index != 3 {
			t.Fatalf("source proof incomplete: n=%d checks=%d index=%d err=%v", n, checks, index, err)
		}
		csvRows, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(dst.String(), "\ufeff"))).ReadAll()
		wantRows := 2
		if allZero {
			wantRows = 1 // header only; no zero rows manufactured or exported
		}
		if err != nil || len(csvRows) != wantRows {
			t.Fatalf("unexpected exported count %d: %v", len(csvRows), err)
		}
		if !allZero && csvRows[1][13] != "¥0.000240" {
			t.Fatal("zero filtering changed actual charged money")
		}
	}
	// Even an omitted zero row must not conceal a repeated source record.
	calls := 0
	_, err := portalBuildVerifiedBill(context.Background(), &bytes.Buffer{}, portalBillBuildPlan{2, "0", 3, 1},
		func(context.Context, logPageCursor, int) ([]LogRow, error) { calls++; return []LogRow{zero}, nil },
		func(context.Context) error { return nil }, func(int64) error { return nil })
	if !errors.Is(err, errPortalBillSourceChanged) || calls != 2 {
		t.Fatal("omitted row bypassed duplicate/source validation", err)
	}
}

func TestPortalBillZeroFilteringHTTPAndRawCSVUnaffected(t *testing.T) {
	for _, bill := range []bool{false, true} {
		t.Run(map[bool]string{false: "raw_csv", true: "bill"}[bill], func(t *testing.T) {
			m, _, portal := newPortalTestMonitor(t)
			m.prodDB, m.usageDayExpr = newFakeProdDB(t), usageDayExprSQLite
			h, _ := hashPassword("password-aaa")
			group := CustomerGroup{Name: "local-export", PortalEmail: "local@example.invalid", PortalPwAdmin: h}
			if m.storeDB.Create(&group).Error != nil || m.storeDB.Create(&TrackedUser{UserID: 101, Username: "alice", GroupID: group.ID}).Error != nil {
				t.Fatal("local source fixture")
			}
			at := time.Date(2026, 10, 21, 20, 4, 0, 0, usageCST).Unix()
			for i, id := range []string{"zero-request", "charged-request"} {
				prompt, quota := int64(0), int64(0)
				if i == 1 {
					prompt, quota = 100, 120
				}
				_, err := m.prodDB.Exec("INSERT INTO logs(id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,username,request_id,other) VALUES(?,101,?,2,'gpt-6-astra',?,?,0,'local','alice',?,?)", i+1, at+int64(i), quota, prompt, id, zeroBillTestMetadata())
				if err != nil {
					t.Fatal(err)
				}
			}
			cookie := portalCookie(portalDo(portal, http.MethodPost, "/login", `{"email":"local@example.invalid","password":"password-aaa"}`))
			if cookie == nil {
				t.Fatal("normal login failed")
			}
			q := "/api/logs/export/prepare?from=2026-10-21&to=2026-10-21"
			if bill {
				q += "&format=bill-v1"
			}
			prepared := portalDo(portal, http.MethodGet, q, "", cookie)
			var ticket struct {
				Ticket string `json:"ticket"`
				Total  int64  `json:"total"`
			}
			if prepared.Code != 200 || json.Unmarshal(prepared.Body.Bytes(), &ticket) != nil || ticket.Total != 2 {
				t.Fatalf("raw-source preflight count changed: %d %s", prepared.Code, prepared.Body.String())
			}
			downloadPath := "/api/logs/export?ticket=" + ticket.Ticket
			if bill {
				downloadPath = "/api/logs/bill/v1?ticket=" + ticket.Ticket
			}
			download := portalDo(portal, http.MethodGet, downloadPath, "", cookie)
			if download.Code != 200 || !strings.Contains(download.Body.String(), "charged-request") || strings.Contains(download.Body.String(), "zero-request") == bill {
				t.Fatalf("incorrect filter scope: %d %s", download.Code, download.Body.String())
			}
		})
	}
}

func TestPortalBillZeroFilteringTaskDownloadKeepsSourceCount(t *testing.T) {
	s, h, cookie, ticket := billTasksFixture(t)
	if _, err := s.m.prodDB.Exec("UPDATE logs SET quota=0,prompt_tokens=0,completion_tokens=0,other=? WHERE id=101", zeroBillTestMetadata()); err != nil {
		t.Fatal(err)
	}
	s.build = func(ctx context.Context, dst io.Writer, scope portalBillScope, total int64, auth func(context.Context) error, progress func(int64) error) (int64, error) {
		if total != 1 {
			t.Error("task no longer validates original source count")
		}
		return portalBuildVerifiedBill(ctx, dst, portalBillBuildPlan{1, "0", 101, 10}, func(ctx context.Context, cursor logPageCursor, limit int) ([]LogRow, error) {
			return s.m.queryGroupLogsForExportCursor(ctx, scope.IDs, scope.From, scope.To, scope.Member, 2, scope.Model, scope.Group, scope.Token, scope.Detail, scope.RequestID, cursor, limit)
		}, auth, progress)
	}
	id := billTaskID(t, billTaskRequest(h, cookie, "POST", "/api/logs/bill/tasks", `{"ticket":"`+ticket+`"}`))
	billTaskWait(t, h, cookie, id, "ready")
	status := billTaskRequest(h, cookie, "GET", "/api/logs/bill/tasks/"+id, "")
	var response struct {
		Task struct {
			Rows  int64 `json:"rows"`
			Total int64 `json:"total"`
		} `json:"task"`
	}
	if status.Code != 200 || json.Unmarshal(status.Body.Bytes(), &response) != nil || response.Task.Rows != 1 || response.Task.Total != 1 {
		t.Fatalf("filtered rows bypassed task source proof: %d %s", status.Code, status.Body.String())
	}
	download := billTaskRequest(h, cookie, "GET", "/api/logs/bill/tasks/"+id+"/download", "")
	rows, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(download.Body.String(), "\ufeff"))).ReadAll()
	if download.Code != 200 || err != nil || len(rows) != 1 || len(rows[0]) != 15 {
		t.Fatalf("all-zero task must produce header only: %d %v %v", download.Code, rows, err)
	}
}
