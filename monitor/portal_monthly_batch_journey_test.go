package monitor

// Offline acceptance of retained, independently reviewed account-months.
// Always uses a new temporary SQLite, synthetic credentials and in-process HTTP;
// never loads deployment settings, collectors, production DBs or real passwords.
import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestMonthlyReviewedBatchLocalJourney(t *testing.T) {
	input, out := os.Getenv("MONTHLY_BATCH_INPUT"), os.Getenv("MONTHLY_BATCH_OUTPUT")
	if input == "" || out == "" {
		t.Skip("explicit private local batch paths required")
	}
	if !filepath.IsAbs(input) || !filepath.IsAbs(out) {
		t.Fatal("local batch paths must be absolute")
	}
	raw, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	var entries []monthlyBatchEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		t.Fatal(err)
	}
	monthlyBatchLocalJourney(t, entries, out)
}

func monthlyBatchLocalJourney(t *testing.T, entries []monthlyBatchEntry, out string) {
	t.Helper()
	if len(entries) == 0 {
		t.Fatal("empty batch is not acceptance")
	}
	m, _, _ := newPortalTestMonitor(t)
	t.Cleanup(m.Close)
	m.cfg.PortalMonthlyStatementEnabled, m.cfg.LocalSnapshotOnly, m.cfg.AlertsDisabled = true, true, true
	r := gin.New()
	m.RegisterPortalRoutes(r)
	password, err := hashPassword(monthlyFunctionalPassword)
	if err != nil {
		t.Fatal(err)
	}
	groups := map[int64]CustomerGroup{}
	directories := map[int64][]monthlyBatchMember{}
	seen := map[string]bool{}
	for _, entry := range entries {
		var scope struct {
			GroupID int64  `json:"group_id"`
			Month   string `json:"month"`
		}
		if err := json.Unmarshal(entry.Evidence, &scope); err != nil || scope.GroupID <= 0 || scope.Month != entry.Month || entry.Account == "" {
			t.Fatal("invalid batch scope", err)
		}
		key := fmt.Sprintf("%d/%s", scope.GroupID, scope.Month)
		if seen[key] {
			t.Fatal("duplicate organization month", key)
		}
		seen[key] = true
		if existing, ok := groups[scope.GroupID]; ok {
			if existing.Name != entry.Account || !reflect.DeepEqual(directories[scope.GroupID], entry.Members) {
				t.Fatal("member scope changed within batch")
			}
		} else {
			g := CustomerGroup{ID: scope.GroupID, Name: entry.Account, PortalEmail: fmt.Sprintf("local-group-%d@example.invalid", scope.GroupID), PortalPwAdmin: password}
			if err := m.storeDB.Create(&g).Error; err != nil {
				t.Fatal(err)
			}
			groups[g.ID], directories[g.ID] = g, entry.Members
			for _, member := range entry.Members {
				if member.ID <= 0 || member.Username == "" {
					t.Fatal("invalid local account")
				}
				if err := m.storeDB.Create(&TrackedUser{UserID: member.ID, GroupID: g.ID, Username: member.Username}).Error; err != nil {
					t.Fatal(err)
				}
			}
		}
		if len(entry.Issues) != 0 {
			continue // unresolved months are not converted into zero or fake evidence
		}
		if len(entry.Expected) != len(entry.Members) {
			t.Fatal("independent per-account expectations required")
		}
		for _, apply := range []bool{false, true, true} {
			if _, err := ImportMonthlyBill(context.Background(), m.storeDB, entry.Evidence, apply, time.Now()); err != nil {
				t.Fatalf("local import %s: %v", key, err)
			}
		}
	}
	var exported []monthlyStatement
	blocked, memberMonths := 0, 0
	for _, entry := range entries {
		var scope struct {
			GroupID int64 `json:"group_id"`
		}
		if err := json.Unmarshal(entry.Evidence, &scope); err != nil {
			t.Fatal(err)
		}
		g := groups[scope.GroupID]
		body, err := json.Marshal(map[string]string{"email": g.PortalEmail, "password": monthlyFunctionalPassword})
		if err != nil {
			t.Fatal(err)
		}
		login := portalDo(r, "POST", "/login", string(body))
		cookie := portalCookie(login)
		if login.Code != 200 || cookie == nil {
			t.Fatal("local login failed", login.Code)
		}
		path := "/api/monthly-statement?month=" + entry.Month + "&group_id=999999&user_id=999999"
		if anonymous := portalDo(r, "GET", path, ""); anonymous.Code != http.StatusUnauthorized {
			t.Fatal("anonymous statement readable")
		}
		response := portalDo(r, "GET", path, "", cookie)
		if len(entry.Issues) != 0 {
			blocked++
			if response.Code != http.StatusNotFound {
				t.Fatal("unresolved month became readable", response.Code)
			}
			if w := portalDo(r, "GET", "/api/monthly-statement/download?month="+entry.Month, "", cookie); w.Code != 404 {
				t.Fatal("unresolved month downloadable")
			}
			continue
		}
		if response.Code != 200 {
			t.Fatalf("detail failed %s %s: %d %s", entry.Account, entry.Month, response.Code, response.Body.String())
		}
		var reply struct{ Data monthlyStatement }
		if err := json.Unmarshal(response.Body.Bytes(), &reply); err != nil {
			t.Fatal(err)
		}
		p := reply.Data
		expected := append([]monthlyMemberStatement(nil), entry.Expected...)
		sort.Slice(expected, func(i, j int) bool { return expected[i].UserID < expected[j].UserID })
		if p.Account != entry.Account || p.Month != entry.Month || !reflect.DeepEqual(p.Members, expected) || !monthlyStatementAmountsComplete(p) {
			t.Fatalf("HTTP amounts or account scope differ from independent expectations: %s %s", entry.Account, entry.Month)
		}
		list := portalDo(r, "GET", "/api/monthly-statements?month="+entry.Month, "", cookie)
		var listing struct {
			Data struct{ Rows []monthlyStatementRow }
		}
		if err := json.Unmarshal(list.Body.Bytes(), &listing); err != nil || list.Code != 200 || len(listing.Data.Rows) != 1 || !reflect.DeepEqual(listing.Data.Rows[0], monthlySummary(p)) {
			t.Fatal("list differs from detail", err)
		}
		download := "/api/monthly-statement/download?month=" + entry.Month + "&publication=" + p.Publication
		file := portalDo(r, "GET", download, "", cookie)
		if file.Code != 200 || file.Header().Get("Content-Type") != monthlyXLSXMIME {
			t.Fatal("download failed", file.Code)
		}
		generated, err := monthlyStatementXLSX(p)
		if err != nil || !bytes.Equal(file.Body.Bytes(), generated) {
			t.Fatal("download differs from page DTO", err)
		}
		monthlyBatchCheckMemberSheet(t, file.Body.Bytes(), expected)
		var evidence MonthlyBillEvidence
		if err := json.Unmarshal(entry.Evidence, &evidence); err != nil {
			t.Fatal(err)
		}
		monthlyAuditProjection(t, p, evidence, file.Body.Bytes())
		for otherID, other := range groups {
			if otherID == g.ID {
				continue
			}
			otherCookie := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(other.ID, other.PortalAuthVer, time.Now().Unix())}
			if w := portalDo(r, "GET", download+fmt.Sprintf("&group_id=%d", g.ID), "", otherCookie); w.Code == 200 {
				t.Fatal("another organization downloaded this publication")
			}
		}
		if out != "" {
			dir := filepath.Join(out, fmt.Sprintf("group-%d", g.ID))
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := monthlyBatchWrite(filepath.Join(dir, entry.Month+"-local-acceptance.xlsx"), file.Body.Bytes()); err != nil {
				t.Fatal(err)
			}
		}
		exported = append(exported, p)
		memberMonths += len(expected)
	}
	var stored int64
	if err := m.storeDB.Model(&PortalMonthlyBill{}).Count(&stored).Error; err != nil || stored != int64(len(exported)) {
		t.Fatal("local import was not idempotent", err)
	}
	if out != "" {
		raw, err := json.MarshalIndent(struct {
			Mode         string             `json:"mode"`
			Blocked      int                `json:"excluded_months"`
			MemberMonths int                `json:"member_months"`
			Statements   []monthlyStatement `json:"statements"`
		}{"temporary_database_acceptance_only_not_customer_publication", blocked, memberMonths, exported}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := monthlyBatchWrite(filepath.Join(out, "accepted-statements.json"), raw); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("local HTTP/download acceptance: groups=%d bills=%d account-month-rows=%d excluded=%d", len(groups), len(exported), memberMonths, blocked)
}

func monthlyBatchCheckMemberSheet(t *testing.T, raw []byte, expected []monthlyMemberStatement) {
	t.Helper()
	z, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range z.File {
		if f.Name != "xl/worksheets/sheet2.xml" {
			continue
		}
		found = true
		r, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		var sheet struct {
			Cells []struct {
				Ref   string `xml:"r,attr"`
				Value string `xml:"v"`
				Text  string `xml:"is>t"`
			} `xml:"sheetData>row>c"`
		}
		if err := xml.Unmarshal(b, &sheet); err != nil {
			t.Fatal(err)
		}
		values := map[string]string{}
		for _, cell := range sheet.Cells {
			values[cell.Ref] = cell.Value + cell.Text
		}
		for i, member := range expected {
			row := i + 4
			if values[fmt.Sprintf("A%d", row)] != fmt.Sprintf("%s\nID %d", member.Username, member.UserID) {
				t.Fatal("XLSX account attribution differs")
			}
			for j, want := range []string{member.Opening, member.CreditTotal, member.Adjustment, member.Consumption.Amount, member.Consumption.Refund, member.Closing} {
				if values[fmt.Sprintf("%c%d", 'B'+j, row)] != monthlyAuditCents(want) {
					t.Fatal("XLSX exact money differs from independent expectation", member.UserID, j)
				}
			}
		}
	}
	if !found {
		t.Fatal("missing account sheet")
	}
}

func TestMonthlyBatchJourneySynthetic(t *testing.T) {
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	month := start.Format("2006-01")
	var entries []monthlyBatchEntry
	for _, gid := range []int64{1, 2} {
		e := monthlyFlatEvidence(gid, month, -416, now)
		e.Users[0].UserID = gid
		entries = append(entries, monthlyBatchEntry{
			Account: fmt.Sprintf("Synthetic %d", gid), Month: month, Evidence: monthlyTestRaw(t, e), Members: []monthlyBatchMember{{gid, "account"}},
			Expected: []monthlyMemberStatement{{UserID: gid, Username: "account", Opening: "-0.000832", CreditTotal: "0.000000", Adjustment: "0.000000", Consumption: monthlyConsumption{Amount: "0.000000", Refund: "0.000000", Net: "0.000000"}, Closing: "-0.000832"}},
		})
	}
	blocked := entries[0]
	blocked.Month = start.AddDate(0, -1, 0).Format("2006-01")
	blocked.Issues = []string{"synthetic missing anchor"}
	blocked.Evidence = json.RawMessage(fmt.Sprintf(`{"group_id":1,"month":%q}`, blocked.Month))
	entries = append(entries, blocked)
	monthlyBatchLocalJourney(t, entries, t.TempDir())
}
