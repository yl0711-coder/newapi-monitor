package monitor

import (
	"encoding/csv"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Opt-in, offline acceptance only. Private source files never enter git or CI.
func TestPortalBillLocalApprovedCSV(t *testing.T) {
	root, csvPath := os.Getenv("USAGE_BILL_LOCAL_SOURCE"), os.Getenv("USAGE_BILL_LOCAL_CSV")
	if root == "" || csvPath == "" {
		t.Skip("private offline sample not supplied")
	}
	idBytes, err := os.ReadFile(os.Getenv("USAGE_BILL_LOCAL_IDS"))
	if err != nil {
		t.Fatal(err)
	}
	var requestIDs map[string]string
	if err := json.Unmarshal(idBytes, &requestIDs); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "source.json"))
	if err != nil {
		t.Fatal(err)
	}
	var source struct {
		Facts []struct {
			ID         int64                      `json:"id"`
			User       string                     `json:"username"`
			Time       string                     `json:"time"`
			Model      string                     `json:"model"`
			Token      string                     `json:"token"`
			Group      string                     `json:"group"`
			Prompt     int64                      `json:"prompt"`
			Completion int64                      `json:"completion"`
			Quota      int64                      `json:"quota"`
			Evidence   map[string]json.RawMessage `json:"evidence"`
		} `json:"facts"`
	}
	if err := json.Unmarshal(raw, &source); err != nil {
		t.Fatal(err)
	}
	audit := map[int64]map[string]json.RawMessage{}
	files, err := filepath.Glob(filepath.Join(root, "cache-audit-30d", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range files {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var v struct {
			Facts []map[string]json.RawMessage `json:"facts"`
		}
		if err := json.Unmarshal(b, &v); err != nil {
			t.Fatal(err)
		}
		for _, r := range v.Facts {
			var id int64
			if err := json.Unmarshal(r["id"], &id); err != nil {
				t.Fatal(err)
			}
			audit[id] = r
		}
	}
	b, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(string(b), "\ufeff"))).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	// Compare directly with the approved 14-column file, not a synthesized
	// expected schema. This also catches later header/order drift.
	for _, row := range rows {
		if len(row) != 14 {
			t.Fatal("approved source CSV width differs")
		}
	}
	if len(rows) != 11 || !reflect.DeepEqual(rows[0], portalBillHeader()) {
		t.Fatal("approved CSV schema differs")
	}
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	g := CustomerGroup{Name: "offline-bill-acceptance", PortalEmail: "offline-bill@example.invalid"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 148, Username: rows[1][1], GroupID: g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	expectedByRequest := map[string][]string{}
	for _, want := range rows[1:] {
		if want[5] == "" || expectedByRequest[want[5]] != nil {
			t.Fatal("missing or duplicate sample request ID")
		}
		expectedByRequest[want[5]] = want
		matches := 0
		for _, f := range source.Facts {
			if f.Time != want[0] || f.User != want[1] || f.Token != want[2] || f.Group != want[3] || f.Model != want[4] {
				continue
			}
			matches++
			fields := map[string]json.RawMessage{}
			for k, v := range f.Evidence {
				fields[k] = v
			}
			for k, v := range audit[f.ID] {
				fields[k] = v
			}
			if len(audit[f.ID]) == 0 {
				t.Fatalf("missing audit %d", f.ID)
			}
			metadata, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			stamp, err := time.ParseInLocation("2006-01-02 15:04:05", f.Time, usageCST)
			if err != nil {
				t.Fatal(err)
			}
			requestID := requestIDs[strconv.FormatInt(f.ID, 10)]
			if requestID == "" {
				t.Fatal("missing independently retrieved request ID")
			}
			r := LogRow{ID: f.ID, Type: 2, CreatedAt: stamp.Unix(), Member: f.User, TokenName: f.Token, Group: f.Group, ModelName: f.Model, RequestID: requestID, PromptTokens: f.Prompt, CompletionTokens: f.Completion, billingQuota: f.Quota, billingFactsValid: true}
			populatePortalLogEvidence(&r, string(metadata))
			got, err := portalBillRecord(r)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("source row %s differs: got=%v want=%v", strconv.FormatInt(f.ID, 10), got, want)
			}
			// Isolated temporary SQLite only: exercise the real query, signed
			// prepare ticket, authorization, CSV writer and native download.
			_, err = m.prodDB.Exec("INSERT INTO logs(id,user_id,created_at,type,username,token_name,`group`,model_name,request_id,prompt_tokens,completion_tokens,quota,other) VALUES(?,148,?,2,?,?,?,?,?,?,?,?,?)", f.ID, stamp.Unix(), f.User, f.Token, f.Group, f.Model, requestID, f.Prompt, f.Completion, f.Quota, string(metadata))
			if err != nil {
				t.Fatal(err)
			}
		}
		if matches != 1 {
			t.Fatalf("ambiguous sample mapping: %d", matches)
		}
	}
	ck := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, time.Now().Unix())}
	ticket := billPrepare(t, portal, ck)
	w := billGet(portal, ck, "/api/logs/bill/v1?ticket="+ticket)
	if w.Code != http.StatusOK || !strings.HasPrefix(w.Body.String(), "\ufeff") {
		t.Fatalf("download status %d", w.Code)
	}
	actual, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(w.Body.String(), "\ufeff"))).ReadAll()
	if err != nil || len(actual) != len(rows) || !reflect.DeepEqual(actual[0], rows[0]) {
		t.Fatal("download CSV schema/row count differs")
	}
	for _, row := range actual[1:] {
		if len(row) != 14 || !reflect.DeepEqual(row, expectedByRequest[row[5]]) {
			t.Fatal("download row differs from approved sample")
		}
		delete(expectedByRequest, row[5])
	}
	if len(expectedByRequest) != 0 {
		t.Fatal("download omitted sample rows")
	}
}
