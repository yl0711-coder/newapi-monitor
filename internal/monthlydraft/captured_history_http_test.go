package monthlydraft

import (
	"archive/zip"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"golang.org/x/crypto/bcrypt"
)

// Actual portal routes without a listener/port; all authority, passwords and
// SQLite files are freshly synthetic. No browser, .env or online service.
func capturedHistoryHTTP(t *testing.T, f capturedHistoryFixture, d *Deployment) (func(string, string, string, []*http.Cookie) *httptest.ResponseRecorder, []*http.Cookie, []*http.Cookie) {
	t.Helper()
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	m, err := monitor.New(monitor.Settings{StorePath: f.cfg.AuthorityPath, LocalSnapshotOnly: true, AlertsDisabled: true, PortalMonthlyStatementEnabled: true, SessionSecret: hex.EncodeToString(secret)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	password := "SyntheticCapturedHistory!2026"
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.auth.Model(&monitor.CustomerGroup{}).Where("id IN ?", []int64{1, 2}).Update("portal_pw_admin", string(hash)).Error; err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	if err := m.RegisterPortalRoutesWithGeneration(router, d); err != nil {
		t.Fatal(err)
	}
	request := func(method, path, body string, cookies []*http.Cookie) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "http://127.0.0.1"+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "http://127.0.0.1")
		req.Header.Set("X-Usage-Bill-Request", "1")
		req.Header.Set("X-Monthly-Statistics", "nullable-v1")
		req.Header.Set("X-Monthly-Accounting", "balance-correction-v1")
		req.Header.Set("X-Monthly-Display", "cents-chain-v1")
		for _, cookie := range cookies {
			req.AddCookie(cookie)
		}
		out := httptest.NewRecorder()
		router.ServeHTTP(out, req)
		return out
	}
	login := func(email string) []*http.Cookie {
		out := request("POST", "/login", `{"email":"`+email+`","password":"`+password+`"}`, nil)
		if out.Code != 200 || len(out.Result().Cookies()) == 0 {
			t.Fatal("synthetic portal login failed", out.Code)
		}
		return out.Result().Cookies()
	}
	return request, login("owner@example.invalid"), login("other@example.invalid")
}

func assertCapturedHistoryCustomerDownload(t *testing.T, request func(string, string, string, []*http.Cookie) *httptest.ResponseRecorder, own, foreign []*http.Cookie) ([]byte, []byte) {
	t.Helper()
	page := request("GET", "/api/monthly-statement?month=2026-09", "", own)
	var view struct {
		Data struct {
			Publication string `json:"publication"`
		} `json:"data"`
	}
	if page.Code != 200 || json.Unmarshal(page.Body.Bytes(), &view) != nil || view.Data.Publication == "" {
		t.Fatal("saved customer statement unavailable", page.Code)
	}
	if out := request("GET", "/api/monthly-statement?month=2026-09", "", foreign); out.Code == 200 {
		t.Fatal("foreign organization read another bill")
	}
	book := request("GET", "/api/monthly-statement/download?month=2026-09&publication="+view.Data.Publication, "", own)
	if book.Code != 200 || !strings.Contains(book.Header().Get("Content-Type"), "spreadsheetml.sheet") {
		t.Fatal("saved bill download failed", book.Code)
	}
	reader, err := zip.NewReader(bytes.NewReader(book.Body.Bytes()), int64(book.Body.Len()))
	if err != nil {
		t.Fatal(err)
	}
	sheets := map[string]bool{}
	for _, part := range reader.File {
		if !strings.HasPrefix(part.Name, "xl/worksheets/sheet") {
			continue
		}
		file, err := part.Open()
		if err != nil {
			t.Fatal(err)
		}
		raw, readErr := io.ReadAll(io.LimitReader(file, (2<<20)+1))
		closeErr := file.Close()
		if readErr != nil || closeErr != nil || len(raw) > 2<<20 {
			t.Fatal("invalid workbook part", readErr, closeErr)
		}
		for _, private := range []string{"captured-scope", "captured-reconstruction", "observation_database", "reconstructions", "source_identity", "candidate_sha256"} {
			if bytes.Contains(page.Body.Bytes(), []byte(private)) || bytes.Contains(raw, []byte(private)) {
				t.Fatal("private proof leaked into customer output", private)
			}
		}
		var sheet struct {
			Rows []struct {
				Cells []struct {
					Value string `xml:"v"`
					Text  string `xml:"is>t"`
				} `xml:"c"`
			} `xml:"sheetData>row"`
		}
		if xml.Unmarshal(raw, &sheet) != nil {
			t.Fatal("invalid worksheet XML")
		}
		if part.Name == "xl/worksheets/sheet1.xml" {
			values := map[string]bool{}
			for _, row := range sheet.Rows {
				for _, cell := range row.Cells {
					values[cell.Value] = true
				}
			}
			if !values["2000.00"] || !values["1996.00"] {
				t.Fatal("workbook summary differs from raw user ledger")
			}
		}
		if part.Name == "xl/worksheets/sheet2.xml" {
			count := 0
			for _, row := range sheet.Rows {
				values := map[string]bool{}
				for _, cell := range row.Cells {
					values[cell.Value] = true
				}
				if values["1000.00"] && values["998.00"] {
					count++
				}
			}
			if count != 2 {
				t.Fatal("workbook member balances differ", count)
			}
		}
		sheets[part.Name] = true
	}
	if !sheets["xl/worksheets/sheet1.xml"] || !sheets["xl/worksheets/sheet2.xml"] || !sheets["xl/worksheets/sheet3.xml"] {
		t.Fatal("summary/user/token workbook sheet missing")
	}
	return bytes.Clone(page.Body.Bytes()), bytes.Clone(book.Body.Bytes())
}
