package monitor

// End-to-end local fixtures use real login, publication, HTTP handlers and XLSX
// generation, but only a fresh synthetic SQLite database. No config loader,
// external DB, source collectors or production credentials are involved.
import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

const monthlyFunctionalPassword = "MonthlyLocalOnly!2026" // synthetic, local-only

type monthlyFunctionalFixture struct {
	m                  *Monitor
	r                  *gin.Engine
	a, b, empty        CustomerGroup
	latest, older, bad string
	evidence           MonthlyBillEvidence
	damaged            PortalMonthlyBill
}

func newMonthlyFunctionalFixture(t *testing.T) monthlyFunctionalFixture {
	t.Helper()
	m, _, _ := newPortalTestMonitor(t)
	t.Cleanup(m.Close)
	m.cfg.PortalMonthlyStatementEnabled = true
	m.cfg.LocalSnapshotOnly = true
	m.cfg.AlertsDisabled = true
	pw, err := hashPassword(monthlyFunctionalPassword)
	if err != nil {
		t.Fatal(err)
	}
	f := monthlyFunctionalFixture{m: m}
	for i, group := range []*CustomerGroup{&f.a, &f.b, &f.empty} {
		*group = CustomerGroup{Name: "月度验收 " + string(rune('A'+i)) + "（合成数据）", PortalEmail: "monthly-" + string(rune('a'+i)) + "@example.invalid", PortalPwAdmin: pw}
		if err := m.storeDB.Create(group).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, member := range []TrackedUser{
		{UserID: 122, GroupID: f.a.ID, Username: "转出用户_A"},
		{UserID: 125, GroupID: f.a.ID, Username: "转入用户_B"},
		{UserID: 244, GroupID: f.b.ID, Username: "其他组织用户"},
		{UserID: 245, GroupID: f.empty.ID, Username: "未发布账单用户"},
	} {
		if err := m.storeDB.Create(&member).Error; err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	last, _, _ := monthlyWindow("", now)
	f.latest, f.older, f.bad = last.Format("2006-01"), last.AddDate(0, -4, 0).Format("2006-01"), last.AddDate(0, -3, 0).Format("2006-01")
	for offset := -4; offset <= 0; offset++ {
		start := last.AddDate(0, offset, 0)
		e := monthlyFlatEvidence(f.a.ID, start.Format("2006-01"), 1000*500000, now)
		e.Users[0].UserID = 122
		v := e.Users[0]
		v.UserID, v.OpeningQuota, v.ClosingQuota, v.ReverseClosingQuota = 125, 200*500000, 200*500000, 200*500000
		e.Users = append(e.Users, v)
		if offset == 0 {
			u := &e.Users[0]
			u.ExpectedCreditEvents, u.ConsumeQuota, u.RefundQuota = 3, 500*500000, 20*500000
			u.Requests, u.Tokens = 500000, 17788983329
			u.Accounting = monthlyAccountingFixture(u.ConsumeQuota, u.RefundQuota)
			u.ClosingQuota, u.ReverseClosingQuota = 1820*500000, 1820*500000
			u.Events = []MonthlyBillCreditEvidence{
				{ID: 1, At: start.Unix() + 10, DeltaQuota: 2000 * 500000, Kind: "recharge", Method: "后台充值"},
				{ID: 2, At: start.Unix() + 20, DeltaQuota: 100 * 500000, Kind: "gift", Method: "活动赠送"},
				{ID: 3, At: start.Unix() + 30, DeltaQuota: -800 * 500000, Kind: "adjustment", Method: "余额扣减"},
			}
			v := &e.Users[1]
			v.ExpectedCreditEvents, v.ConsumeQuota, v.Requests, v.Tokens = 1, 300*500000, 300000, 1000
			v.Accounting = monthlyAccountingFixture(v.ConsumeQuota, 0)
			v.ClosingQuota, v.ReverseClosingQuota = 700*500000, 700*500000
			v.Events = []MonthlyBillCreditEvidence{{ID: 4, At: start.Unix() + 31, DeltaQuota: 800 * 500000, Kind: "recharge", Method: "后台充值"}}
			f.evidence = e
		}
		if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, e), true, now); err != nil {
			t.Fatal(err)
		}
	}
	e := monthlyFlatEvidence(f.b.ID, f.latest, 99*500000, now)
	e.Users[0].UserID = 244
	if _, err := ImportMonthlyBill(context.Background(), m.storeDB, monthlyTestRaw(t, e), true, now); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&f.damaged, "group_id = ? AND month = ?", f.a.ID, f.bad).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id = ? AND month = ?", f.a.ID, f.bad).Update("payload", "deliberately damaged synthetic fixture").Error; err != nil {
		t.Fatal(err)
	}

	r := gin.New()
	r.Use(gin.Recovery())
	if err := r.SetTrustedProxies(nil); err != nil {
		t.Fatal(err)
	}
	r.Use(func(c *gin.Context) {
		if strings.HasPrefix(c.Request.URL.Path, "/api/") && !strings.HasPrefix(c.Request.URL.Path, "/api/monthly-statement") {
			c.AbortWithStatusJSON(503, gin.H{"error": "本机验收仅加载月度账单，未连接线上使用日志"})
			return
		}
		if c.Request.Method == "GET" && c.Request.URL.Path == "/" {
			original := c.Writer
			buffer := newPortalBufferedResponseWriter(original)
			c.Writer = buffer
			defer func() { c.Writer = original }()
			c.Next()
			if buffer.Status() == 200 {
				page := strings.Replace(buffer.body.String(), "\nload();\n", "\nshowView('monthly');\n", 1)
				page = strings.Replace(page, "<body>", `<body><p style="padding:12px;color:#9a6700">本机功能验收 · 全部为合成数据 · 不连接线上</p>`, 1)
				buffer.body.Reset()
				buffer.body.WriteString(page)
			}
			buffer.commitTo(original)
		}
	})
	m.RegisterPortalRoutes(r)
	f.r = r
	return f
}

func TestMonthlyFunctionalHTTPJourney(t *testing.T) {
	f := newMonthlyFunctionalFixture(t)
	srv := httptest.NewServer(f.r)
	defer srv.Close()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Jar: jar, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request := func(path, body string, status int) []byte {
		t.Helper()
		method := "GET"
		if body != "" {
			method = "POST"
		}
		req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Monthly-Display", "cents-chain-v1")
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(res.Body, 3<<20))
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != status {
			t.Fatalf("%s got %d, want %d: %s", path, res.StatusCode, status, raw)
		}
		return raw
	}
	login := func(email string) {
		raw, _ := json.Marshal(map[string]string{"email": email, "password": monthlyFunctionalPassword})
		request("/login", string(raw), 200)
	}
	request("/api/monthly-statements", "", 401)
	request("/login", `{"email":"monthly-a@example.invalid","password":"wrong"}`, 401)
	login(f.a.PortalEmail)
	page := request("/", "", 200)
	if !bytes.Contains(page, []byte("本机功能验收")) || !bytes.Contains(page, []byte(`data-v="monthly"`)) {
		t.Fatal("real page did not mount monthly bill")
	}
	var list struct {
		Data struct {
			Rows        []monthlyStatementRow
			Next        string
			Unavailable []string `json:"unavailable_months"`
		}
	}
	if err := json.Unmarshal(request("/api/monthly-statements", "", 200), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Data.Rows) != 3 || list.Data.Rows[0].Month != f.latest || list.Data.Next != f.bad {
		t.Fatal("wrong default page")
	}
	if err := json.Unmarshal(request("/api/monthly-statements?before="+list.Data.Next, "", 200), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Data.Rows) != 1 || list.Data.Rows[0].Month != f.older || len(list.Data.Unavailable) != 1 || list.Data.Next != "" {
		t.Fatal("bad-month pagination hid usable bills")
	}
	request("/api/monthly-statement?month="+f.bad, "", 503)
	request("/api/monthly-statements?month="+time.Now().In(usageCST).Format("2006-01"), "", 400)
	var detail struct{ Data monthlyStatement }
	raw := request("/api/monthly-statement?month="+f.latest+"&group_id=999&user_id=244", "", 200)
	if err := json.Unmarshal(raw, &detail); err != nil {
		t.Fatal(err)
	}
	p := detail.Data
	if p.Account != f.a.Name || *p.MonthOpeningBalance != "1200.000000" || *p.MonthEndBalance != "2520.000000" || *p.Credits.RechargeTotal != "2800.000000" || *p.Credits.GiftTotal != "100.000000" || p.Credits.Adjustment != "-800.000000" || p.Consumption.Amount != "800.000000" || p.Consumption.Refund != "20.000000" || p.Consumption.Net != "780.000000" || len(p.Credits.Rows) != 4 {
		t.Fatal("functional amounts differ from independent fixture expectations")
	}
	if p.MemberDetailsVersion != 1 || len(p.Members) != 2 || p.Members[0].UserID != 122 || p.Members[0].Username != "转出用户_A" || p.Members[0].Closing != "1820.000000" || p.Members[0].Consumption.Refund != "20.000000" || p.Members[1].UserID != 125 || p.Members[1].Username != "转入用户_B" || p.Members[1].Closing != "700.000000" {
		t.Fatal("HTTP member breakdown incorrect", p.Members)
	}
	download := "/api/monthly-statement/download?month=" + f.latest + "&publication=" + p.Publication
	file := request(download, "", 200)
	z, err := zip.NewReader(bytes.NewReader(file), int64(len(file)))
	if err != nil {
		t.Fatal(err)
	}
	var sheet string
	for _, part := range z.File {
		if part.Name == "xl/worksheets/sheet1.xml" {
			r, err := part.Open()
			if err != nil {
				t.Fatal(err)
			}
			b, err := io.ReadAll(r)
			r.Close()
			if err != nil {
				t.Fatal(err)
			}
			sheet = string(b)
		}
	}
	for _, want := range []string{"转出用户_A", "转入用户_B", "-800.00", "2800.00", "100.00", "20.00", "780.00", "1200.00", "2520.00"} {
		if !strings.Contains(sheet, want) {
			t.Fatal("download differs from page", want)
		}
	}
	if dir := os.Getenv("MONTHLY_FUNCTIONAL_ARTIFACTS"); dir != "" {
		if !filepath.IsAbs(dir) {
			t.Fatal("artifact directory must be absolute")
		}
		if err := os.WriteFile(filepath.Join(dir, "downloaded-monthly.xlsx"), file, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "downloaded-monthly.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	monthlyWorkbookSlots <- struct{}{}
	monthlyWorkbookSlots <- struct{}{}
	func() { defer func() { <-monthlyWorkbookSlots; <-monthlyWorkbookSlots }(); request(download, "", 429) }()
	request(download, "", 200)
	login(f.b.PortalEmail)
	bRaw := request("/api/monthly-statement?month="+f.latest+"&group_id=1&user_id=122", "", 200)
	if strings.Contains(string(bRaw), f.a.Name) || !strings.Contains(string(bRaw), `"month_end_balance":"99.000000"`) {
		t.Fatal("cross-customer data disclosure")
	}
	request(download, "", 409)
	login(f.empty.PortalEmail)
	if data := request("/api/monthly-statements", "", 200); !bytes.Contains(data, []byte(`"rows":[]`)) {
		t.Fatal("empty organization got invented bill")
	}
	request("/api/monthly-statement?month="+f.latest, "", 404)
	login(f.a.PortalEmail)
	if err := f.m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id = ? AND month = ?", f.a.ID, f.bad).Update("payload", f.damaged.Payload).Error; err != nil {
		t.Fatal(err)
	}
	request("/api/monthly-statement?month="+f.bad, "", 200)
	if err := WithdrawMonthlyBill(context.Background(), f.m.storeDB, f.a.ID, f.latest, p.Publication, "synthetic acceptance", "test version recovery", true, time.Now()); err != nil {
		t.Fatal(err)
	}
	request(download, "", 404)
	if _, err := ImportMonthlyBill(context.Background(), f.m.storeDB, monthlyTestRaw(t, f.evidence), true, time.Now()); err != nil {
		t.Fatal(err)
	}
	request(download, "", 409)
	request("/logout", "", 302)
	request("/api/monthly-statements", "", 401)
	t.Log("Functional HTTP journey passed: login, multi-month paging, money parity, Excel download, busy/recovery, customer isolation, empty month, damaged month recovery, withdrawal/reissue, logout")
}

func TestMonthlyFunctionalBrowserServer(t *testing.T) {
	if os.Getenv("MONTHLY_FUNCTIONAL_SERVER") != "synthetic" {
		t.Skip("explicit local synthetic browser acceptance only")
	}
	f := newMonthlyFunctionalFixture(t)
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: f.r, ReadHeaderTimeout: 5 * time.Second}
	defer srv.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); _ = srv.Close() }()
	t.Logf("LOCAL_SYNTHETIC_URL=http://%s/login login=%s password=%s latest=%s damaged=%s", ln.Addr(), f.a.PortalEmail, monthlyFunctionalPassword, f.latest, f.bad)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		t.Fatal(err)
	}
}
