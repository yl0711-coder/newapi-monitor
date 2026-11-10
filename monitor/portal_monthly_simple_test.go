package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

type simpleFakeSource struct {
	reg      map[int64]int64
	snap     simpleSnapshot
	calls    int
	regCalls atomic.Int64
}

func (s *simpleFakeSource) Registrations(ctx context.Context, ids []int64) (map[int64]int64, error) {
	s.regCalls.Add(1)
	out := map[int64]int64{}
	for _, id := range ids {
		if at, ok := s.reg[id]; ok {
			out[id] = at
		}
	}
	return out, nil
}

func (s *simpleFakeSource) Snapshot(ctx context.Context, ids []int64, fundingFrom, tailFrom int64) (simpleSnapshot, error) {
	s.calls++
	out := s.snap
	out.TailFrom = tailFrom
	return out, nil
}

type simpleFakeFacts struct {
	through int64
	days    []simpleFactDay
}

func (f *simpleFakeFacts) VerifiedThrough(ctx context.Context, ids []int64) (int64, map[int64]int64, error) {
	return f.through, map[int64]int64{}, nil
}

func (f *simpleFakeFacts) Days(ctx context.Context, ids []int64, from, to int64) ([]simpleFactDay, error) {
	var out []simpleFactDay
	for _, d := range f.days {
		if d.DateTS >= from && d.DateTS < to {
			out = append(out, d)
		}
	}
	return out, nil
}

func simpleCST(y int, m time.Month, d, h int) int64 {
	return time.Date(y, m, d, h, 0, 0, 0, usageCST).Unix()
}

// simpleScenario builds two accounts with fractional consumption over three
// API tokens, admin credits, a sign-up gift, a refund, and one historical
// log/wallet difference (consumption logged but not charged, +1.819570).
func simpleScenario() (map[int64]int64, simpleSnapshot, []simpleFactDay, int64) {
	rng := rand.New(rand.NewSource(11))
	reg := map[int64]int64{201: simpleCST(2026, 8, 5, 10), 202: simpleCST(2026, 9, 10, 15)}
	tailFrom := simpleCST(2026, 11, 6, 0)
	events := []simpleEvent{
		{ID: 1001, UserID: 201, At: reg[201] + 60, Delta: 500000, Kind: "gift", Method: "注册赠送"},
		{ID: 1002, UserID: 201, At: reg[201] + 600, Delta: 50000 * 500000, Kind: "recharge", Method: "后台充值"},
		{ID: 1003, UserID: 202, At: reg[202] + 60, Delta: 500000, Kind: "gift", Method: "注册赠送"},
		{ID: 1004, UserID: 202, At: reg[202] + 900, Delta: 1000 * 500000, Kind: "recharge", Method: "在线充值"},
		{ID: 1005, UserID: 201, At: simpleCST(2026, 10, 9, 18), Delta: 20000 * 500000, Kind: "recharge", Method: "后台充值"},
		{ID: 1006, UserID: 202, At: simpleCST(2026, 10, 20, 9), Delta: -1 * 500000, Kind: "adjustment", Method: "余额扣减"},
		{ID: 1007, UserID: 201, At: simpleCST(2026, 11, 3, 16), Delta: 20000 * 500000, Kind: "recharge", Method: "后台充值"},
	}
	var days []simpleFactDay
	for day := simpleCST(2026, 8, 5, 0); day < tailFrom; day += 86400 {
		for _, uid := range []int64{201, 202} {
			if day+86400 <= reg[uid] {
				continue
			}
			for tok := int64(1); tok <= 3; tok++ {
				tokenID := uid*10 + tok
				c := rng.Int63n(80_000_000) // up to 160 yuan, arbitrary sub-cent
				if uid == 202 {
					c /= 9
				}
				r := int64(0)
				if rng.Intn(25) == 0 {
					r = rng.Int63n(c/4 + 1)
				}
				days = append(days, simpleFactDay{UserID: uid, TokenID: tokenID, TokenName: "key-" + string(rune('a'+tok-1)), DateTS: day,
					Requests: 10 + rng.Int63n(90), Tokens: 1000 + rng.Int63n(90000), Consume: c, Refund: r})
			}
		}
	}
	quota := map[int64]int64{}
	for _, uid := range []int64{201, 202} {
		var b int64
		for _, e := range events {
			if e.UserID == uid {
				b += e.Delta
			}
		}
		for _, d := range days {
			if d.UserID == uid {
				b -= d.Consume - d.Refund
			}
		}
		quota[uid] = b
	}
	tail := map[int64]int64{201: 12_345_678, 202: 987_654}
	quota[201] -= tail[201]
	quota[202] -= tail[202]
	quota[201] += 909785 // historical difference: balance higher than logs imply
	snap := simpleSnapshot{CapturedAt: simpleCST(2026, 11, 6, 12), Quota: quota, Registered: reg, Events: events, TailNet: tail}
	return reg, snap, days, tailFrom
}

func simpleTestService(t *testing.T) (*Monitor, *SimpleMonthlyGeneration, *gin.Engine, *http.Cookie, CustomerGroup, *simpleFakeSource) {
	t.Helper()
	m, _, _ := newPortalTestMonitor(t)
	t.Cleanup(m.Close)
	m.cfg.PortalMonthlyStatementEnabled = true
	if err := m.storeDB.AutoMigrate(&PortalMonthlySimpleJob{}, &PortalMonthlySimpleBoundary{}, &PortalMonthlySimpleAudit{}, &PortalMonthlySimpleSubscription{}); err != nil {
		t.Fatal(err)
	}
	g := CustomerGroup{Name: "游酷测试", PortalEmail: "org@example.invalid", PortalAuthVer: 3}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	for _, uid := range []int64{201, 202} {
		if err := m.storeDB.Create(&TrackedUser{UserID: uid, GroupID: g.ID, Username: "u" + string(rune('0'+uid%10))}).Error; err != nil {
			t.Fatal(err)
		}
	}
	reg, snap, days, tailFrom := simpleScenario()
	src := &simpleFakeSource{reg: reg, snap: snap}
	svc := newSimpleMonthlyGeneration(m, src, &simpleFakeFacts{through: tailFrom, days: days})
	svc.settle = 0
	svc.now = func() time.Time { return time.Unix(simpleCST(2026, 11, 6, 12), 0) }
	r := gin.New()
	if err := m.RegisterPortalRoutesWithGeneration(r, svc); err != nil {
		t.Fatal(err)
	}
	cookie := &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, time.Now().Unix())}
	return m, svc, r, cookie, g, src
}

type simpleBillJSON struct {
	Month               string  `json:"month"`
	MonthOpeningBalance *string `json:"month_opening_balance"`
	MonthEndBalance     *string `json:"month_end_balance"`
	Credits             struct {
		KnownTotal string `json:"known_total"`
		Adjustment string `json:"adjustment"`
		Rows       []struct {
			UserID int64  `json:"user_id"`
			Amount string `json:"amount"`
		} `json:"rows"`
	} `json:"credits"`
	Consumption struct {
		Amount string `json:"amount"`
		Refund string `json:"refund"`
	} `json:"consumption"`
	Members []struct {
		UserID      int64  `json:"user_id"`
		Opening     string `json:"opening"`
		CreditTotal string `json:"credit_total"`
		Adjustment  string `json:"adjustment"`
		Closing     string `json:"closing"`
		Consumption struct {
			Amount string `json:"amount"`
			Refund string `json:"refund"`
		} `json:"consumption"`
		TokenConsumption []struct {
			TokenID int64  `json:"token_id"`
			Amount  string `json:"amount"`
			Refund  string `json:"refund"`
		} `json:"token_consumption"`
	} `json:"members"`
}

// cents parses a published amount and requires it to be cent-exact.
func simpleCents(t *testing.T, v string) int64 {
	t.Helper()
	q, ok := newMonthlyMoney(v)
	if !ok || q%simpleCent != 0 {
		t.Fatalf("amount %q is not cent-exact", v)
	}
	return q / simpleCent
}

func simpleGetBill(t *testing.T, r *gin.Engine, cookie *http.Cookie, month string) simpleBillJSON {
	t.Helper()
	w := portalDo(r, "GET", "/api/monthly-statement?month="+month, "", cookie)
	if w.Code != 200 {
		t.Fatalf("detail %s: %d %s", month, w.Code, w.Body.String())
	}
	var wrap struct {
		Data simpleBillJSON `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &wrap); err != nil || wrap.Data.MonthEndBalance == nil {
		t.Fatalf("detail %s: %v %s", month, err, w.Body.String())
	}
	return wrap.Data
}

func simpleCheckBill(t *testing.T, b simpleBillJSON) {
	t.Helper()
	var open, closingSum, consume, refund, credit, adjust int64
	for _, mem := range b.Members {
		o, c := simpleCents(t, mem.Opening), simpleCents(t, mem.Closing)
		cr, ad := simpleCents(t, mem.CreditTotal), simpleCents(t, mem.Adjustment)
		x, rf := simpleCents(t, mem.Consumption.Amount), simpleCents(t, mem.Consumption.Refund)
		if o+cr+ad-x+rf != c {
			t.Fatalf("%s user %d does not balance: %d+%d%+d-%d+%d != %d", b.Month, mem.UserID, o, cr, ad, x, rf, c)
		}
		var tx, tr int64
		for _, tok := range mem.TokenConsumption {
			tx += simpleCents(t, tok.Amount)
			tr += simpleCents(t, tok.Refund)
		}
		if tx != x || tr != rf {
			t.Fatalf("%s user %d tokens %d/%d != account %d/%d", b.Month, mem.UserID, tx, tr, x, rf)
		}
		open, closingSum, consume, refund, credit, adjust = open+o, closingSum+c, consume+x, refund+rf, credit+cr, adjust+ad
	}
	if simpleCents(t, *b.MonthOpeningBalance) != open || simpleCents(t, *b.MonthEndBalance) != closingSum ||
		simpleCents(t, b.Consumption.Amount) != consume || simpleCents(t, b.Consumption.Refund) != refund ||
		simpleCents(t, b.Credits.KnownTotal) != credit || simpleCents(t, b.Credits.Adjustment) != adjust {
		t.Fatalf("%s organization totals are not the sum of accounts", b.Month)
	}
	if open+credit+adjust-consume+refund != closingSum {
		t.Fatalf("%s organization does not balance", b.Month)
	}
}

func TestSimpleMonthlyGenerationEndToEnd(t *testing.T) {
	m, svc, r, cookie, g, _ := simpleTestService(t)
	ctx := context.Background()
	submit := func(month string) {
		w := generationHTTPDo(r, "POST", "", `{"month":"`+month+`"}`, cookie, nil)
		if w.Code != http.StatusAccepted {
			t.Fatalf("submit %s: %d %s", month, w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), `"state":"queued"`) {
			t.Fatalf("submit %s state: %s", month, w.Body.String())
		}
		if !svc.runOne(ctx) {
			t.Fatalf("no job ran for %s", month)
		}
		var job PortalMonthlySimpleJob
		m.storeDB.First(&job, "group_id = ? AND month = ?", g.ID, month)
		if job.State != simpleJobDone {
			t.Fatalf("job %s ended %s: %s", month, job.State, job.LastError)
		}
		w = generationHTTPDo(r, "GET", "?month="+month, "", cookie, nil)
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"state":"available"`) {
			t.Fatalf("status %s: %d %s", month, w.Code, w.Body.String())
		}
	}
	// The customer asks for October first, then September.
	submit("2026-10")
	submit("2026-09")
	submit("2026-08")
	oct, sep, aug := simpleGetBill(t, r, cookie, "2026-10"), simpleGetBill(t, r, cookie, "2026-09"), simpleGetBill(t, r, cookie, "2026-08")
	for _, b := range []simpleBillJSON{aug, sep, oct} {
		simpleCheckBill(t, b)
	}
	for _, pair := range [][2]simpleBillJSON{{aug, sep}, {sep, oct}} {
		prev := map[int64]string{}
		for _, mem := range pair[0].Members {
			prev[mem.UserID] = mem.Closing
		}
		for _, mem := range pair[1].Members {
			if prev[mem.UserID] != mem.Opening {
				t.Fatalf("user %d %s closing %s != %s opening %s", mem.UserID, pair[0].Month, prev[mem.UserID], pair[1].Month, mem.Opening)
			}
		}
	}
	// Every displayed amount is within one cent of the true value; every
	// month's consumption is exactly its logged consumption, and the wallet
	// difference is only recorded internally.
	var audits []PortalMonthlySimpleAudit
	m.storeDB.Order("month, user_id").Find(&audits)
	if len(audits) != 6 {
		t.Fatalf("audit rows: %d", len(audits))
	}
	for _, a := range audits {
		if !simpleWithinCent(a.ClosingCents, a.ClosingRaw) || !simpleWithinCent(a.ConsumeCents, a.Consume) || !simpleWithinCent(a.OpeningCents, a.OpeningRaw) {
			t.Fatalf("audit %s user %d over one cent: %+v", a.Month, a.UserID, a)
		}
		if a.Consume != a.LogConsume {
			t.Fatalf("consumption must equal the logs: %+v", a)
		}
		want := int64(0)
		if a.UserID == 201 {
			want = 909785
		}
		if a.WalletResidual != want {
			t.Fatalf("wallet residual %d, want %d: %+v", a.WalletResidual, want, a)
		}
	}
	// The workbook downloads from the same saved version.
	list := portalDo(r, "GET", "/api/monthly-statements?month=2026-10", "", cookie)
	if list.Code != 200 {
		t.Fatalf("list: %d %s", list.Code, list.Body.String())
	}
	var detail struct {
		Data struct {
			Publication string `json:"publication"`
		} `json:"data"`
	}
	if err := json.Unmarshal(portalDo(r, "GET", "/api/monthly-statement?month=2026-10", "", cookie).Body.Bytes(), &detail); err != nil || detail.Data.Publication == "" {
		t.Fatalf("publication missing: %v", err)
	}
	req := httptest.NewRequest("GET", "http://127.0.0.1/api/monthly-statement/download?month=2026-10&publication="+detail.Data.Publication, nil)
	req.AddCookie(cookie)
	req.Header.Set("X-Monthly-Statistics", "nullable-v1")
	req.Header.Set("X-Monthly-Accounting", "balance-correction-v1")
	req.Header.Set("X-Monthly-Display", "cents-chain-v1")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), "spreadsheet") || w.Body.Len() < 1000 {
		t.Fatalf("download: %d %s %d", w.Code, w.Header().Get("Content-Type"), w.Body.Len())
	}
	// Re-submitting a saved month returns the saved bill without a new job run.
	w = generationHTTPDo(r, "POST", "", `{"month":"2026-10"}`, cookie, nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"state":"available"`) || svc.runOne(ctx) {
		t.Fatalf("resubmit: %d %s", w.Code, w.Body.String())
	}
}

func TestSimpleMonthlyGenerationBlocksUnfavourableOrUnknown(t *testing.T) {
	m, svc, r, cookie, g, src := simpleTestService(t)
	ctx := context.Background()
	src.snap.Quota[201] -= 2 * 909785 // balance now LOWER than logs imply
	if w := generationHTTPDo(r, "POST", "", `{"month":"2026-10"}`, cookie, nil); w.Code != http.StatusAccepted {
		t.Fatalf("submit: %d", w.Code)
	}
	svc.runOne(ctx)
	var job PortalMonthlySimpleJob
	m.storeDB.First(&job, "group_id = ? AND month = ?", g.ID, "2026-10")
	if job.State != simpleJobRetry {
		t.Fatalf("customer-unfavourable difference must retry, got %s", job.State)
	}
	if w := generationHTTPDo(r, "GET", "?month=2026-10", "", cookie, nil); !strings.Contains(w.Body.String(), `"state":"processing"`) {
		t.Fatalf("customer must only see processing: %s", w.Body.String())
	}
	var bills int64
	m.storeDB.Model(&PortalMonthlyBill{}).Count(&bills)
	if bills != 0 {
		t.Fatal("no bill may be published")
	}
	src.snap.Quota[201] += 2 * 909785
	src.snap.Review = []int64{999}
	m.storeDB.Model(&PortalMonthlySimpleJob{}).Where("group_id = ?", g.ID).Updates(map[string]any{"next_at": 0})
	svc.runOne(ctx)
	m.storeDB.First(&job, "group_id = ? AND month = ?", g.ID, "2026-10")
	if job.State != simpleJobReview {
		t.Fatalf("unrecognized funding must be held for review, got %s", job.State)
	}
}

func TestSimpleMonthlyGenerationRejectsCurrentAndPreRegistrationMonths(t *testing.T) {
	_, _, r, cookie, _, _ := simpleTestService(t)
	for _, month := range []string{"2026-11", "2026-07"} {
		if w := generationHTTPDo(r, "POST", "", `{"month":"`+month+`"}`, cookie, nil); w.Code != http.StatusBadRequest {
			t.Fatalf("%s must be rejected: %d %s", month, w.Code, w.Body.String())
		}
	}
}

func TestSimpleClassifyFundingRC48(t *testing.T) {
	cases := []struct {
		row          monthlysource.FundingLog
		status, kind string
		delta        int64
	}{
		{monthlysource.FundingLog{ID: 1, UserID: 16, CreatedAt: 1, Type: 1, Content: "Online top-up succeeded, amount added: ＄1000.000000, payment amount: 1000.000000",
			Other: `{"content_parts":[{"key":"Online top-up succeeded, amount added: {{quota}}, payment amount: {{amount}}","params":{"amount":"1000.000000","quota":"＄1000.000000"}}]}`},
			monthlysource.FundingMovement, "recharge", 500000000},
		{monthlysource.FundingLog{ID: 2, UserID: 168, CreatedAt: 1, Type: 1, Content: "Increased user quota by 1000000000",
			Other: `{"admin_info":{"admin_id":1},"op":{"action":"user.quota_add","params":{"from":98730283,"mode":"add","quota":1000000000,"requested_quota":1000000000,"target_user_id":168,"to":1098730283}}}`},
			monthlysource.FundingMovement, "recharge", 1000000000},
		{monthlysource.FundingLog{ID: 3, UserID: 168, CreatedAt: 1, Type: 1, Content: "Decreased user quota by 500000",
			Other: `{"op":{"action":"user.quota_subtract","params":{"from":1500000,"quota":500000,"target_user_id":168,"to":1000000}}}`},
			monthlysource.FundingMovement, "adjustment", -500000},
		{monthlysource.FundingLog{ID: 4, UserID: 168, CreatedAt: 1, Type: 1, Content: "x",
			Other: `{"op":{"action":"user.quota_add","params":{"from":1,"quota":5,"target_user_id":999,"to":6}}}`},
			monthlysource.FundingReview, "", 0}, // beneficiary mismatch
		{monthlysource.FundingLog{ID: 5, UserID: 7, CreatedAt: 1, Type: 4, Content: "New user sign-up bonus ＄1.000000",
			Other: `{"content_parts":[{"key":"New user sign-up bonus {{quota}}","params":{"quota":"＄1.000000"}}]}`},
			monthlysource.FundingMovement, "gift", 500000},
		{monthlysource.FundingLog{ID: 6, UserID: 7, CreatedAt: 1, Type: 1, Content: "Online top-up succeeded, amount added: ¥7.200000, payment amount: 1",
			Other: `{"content_parts":[{"key":"Online top-up succeeded, amount added: {{quota}}, payment amount: {{amount}}","params":{"quota":"¥7.200000"}}]}`},
			monthlysource.FundingReview, "", 0}, // currency needs an exchange rate
		{monthlysource.FundingLog{ID: 7, UserID: 7, CreatedAt: 1, Type: 1, Content: "Subscription purchased with balance, plan: p, payment amount: 1, quota deducted: ＄1.000000",
			Other: `{"content_parts":[{"key":"Subscription purchased with balance, plan: {{plan}}, payment amount: {{amount}}, quota deducted: {{quota}}","params":{"quota":"＄1.000000"}}]}`},
			monthlysource.FundingReview, "", 0},
		{monthlysource.FundingLog{ID: 8, UserID: 7, CreatedAt: 1, Type: 3, Content: "管理员增加用户额度 ＄20000.000000 额度", Other: `{}`},
			monthlysource.FundingMovement, "recharge", 10000000000}, // rc26 still recognized
	}
	for _, c := range cases {
		got := simpleClassifyFunding(c.row)
		if got.Status != c.status || got.Kind != c.kind || (c.delta != 0 && (got.DeltaQuota == nil || *got.DeltaQuota != c.delta)) {
			t.Fatalf("row %d: got %+v (delta %v), want %s %s %d", c.row.ID, got, got.DeltaQuota, c.status, c.kind, c.delta)
		}
	}
}

// Customers may click repeatedly while the worker is claiming or finishing
// jobs in the same SQLite store. Admission must wait for the writer instead of
// failing with an immediate busy error.
func TestSimpleMonthlyGenerationSubmitSurvivesConcurrentWriters(t *testing.T) {
	_, svc, _, _, g, _ := simpleTestService(t)
	ctx := context.Background()
	months := []string{"2026-08", "2026-09", "2026-10"}
	var wg sync.WaitGroup
	errs := make(chan error, 400)
	stop := make(chan struct{})
	wg.Add(1)
	go func() { // worker-side writes: claim and finish without generating
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if job, ok := svc.claim(ctx); ok {
				svc.finish(ctx, job, nil)
			}
		}
	}()
	var submitters sync.WaitGroup
	for i := 0; i < 8; i++ {
		submitters.Add(1)
		go func(i int) {
			defer submitters.Done()
			for n := 0; n < 40; n++ {
				if _, err := svc.SubmitMonthly(ctx, g.ID, g.PortalAuthVer, months[(i+n)%len(months)]); err != nil {
					errs <- err
				}
			}
		}(i)
	}
	submitters.Wait()
	close(stop)
	wg.Wait()
	close(errs)
	failed := 0
	for err := range errs {
		failed++
		if failed == 1 {
			t.Errorf("submit failed under concurrent writers: %v", err)
		}
	}
	if failed > 0 {
		t.Fatalf("%d of 320 submissions failed", failed)
	}
}

func TestSimpleMonthlyGenerationSubmitStateMachine(t *testing.T) {
	m, svc, _, _, g, _ := simpleTestService(t)
	ctx := context.Background()
	set := func(state string, version, updated int64) {
		t.Helper()
		if err := m.storeDB.Save(&PortalMonthlySimpleJob{GroupID: g.ID, Month: "2026-10", AuthVersion: version, State: state, Attempts: 7, NextAt: 99, LeaseUntil: 0, LastError: "x"}).Error; err != nil {
			t.Fatal(err)
		}
		// gorm stamps UpdatedAt with the wall clock; set the test clock's value.
		if err := m.storeDB.Model(&PortalMonthlySimpleJob{}).Where("group_id = ? AND month = ?", g.ID, "2026-10").UpdateColumn("updated_at", updated).Error; err != nil {
			t.Fatal(err)
		}
	}
	read := func() PortalMonthlySimpleJob {
		t.Helper()
		var j PortalMonthlySimpleJob
		if err := m.storeDB.First(&j, "group_id = ? AND month = ?", g.ID, "2026-10").Error; err != nil {
			t.Fatal(err)
		}
		return j
	}
	cases := []struct {
		state      string
		jobVersion int64
		submitVer  int64
		age        int64
		wantState  string
		wantView   string
		reset      bool
	}{
		{simpleJobDone, 3, 3, 0, simpleJobQueued, "queued", true},
		{simpleJobReview, 3, 3, 60, simpleJobReview, "needs_review", false},
		{simpleJobReview, 3, 3, simpleReviewRetryAfter, simpleJobQueued, "queued", true},
		{simpleJobReview, 2, 3, 60, simpleJobQueued, "queued", true},
		{simpleJobRunning, 3, 3, 3600, simpleJobRunning, "processing", false},
		{simpleJobRetry, 3, 3, 3600, simpleJobRetry, "processing", false},
		{simpleJobQueued, 3, 3, 3600, simpleJobQueued, "queued", false},
	}
	for _, c := range cases {
		set(c.state, c.jobVersion, svc.now().Unix()-c.age)
		v, err := svc.SubmitMonthly(ctx, g.ID, c.submitVer, "2026-10")
		if err != nil || v.State != c.wantView {
			t.Fatalf("%s v%d: view %+v err %v", c.state, c.jobVersion, v, err)
		}
		j := read()
		if j.State != c.wantState || j.AuthVersion != c.submitVer {
			t.Fatalf("%s v%d: stored %+v", c.state, c.jobVersion, j)
		}
		if j.State == simpleJobReview && j.UpdatedAt != svc.now().Unix()-c.age {
			t.Fatalf("a click during the cool-down restarted it: %+v", j)
		}
		if c.reset != (j.Attempts == 0 && j.LastError == "" && j.NextAt != 99) {
			t.Fatalf("%s v%d: reset mismatch %+v", c.state, c.jobVersion, j)
		}
	}
	if err := m.storeDB.Delete(&PortalMonthlySimpleJob{}, "group_id = ?", g.ID).Error; err != nil {
		t.Fatal(err)
	}
	if v, err := svc.SubmitMonthly(ctx, g.ID, 3, "2026-10"); err != nil || v.State != "queued" || read().State != simpleJobQueued {
		t.Fatalf("new request: %+v %v", v, err)
	}
	job, ok := svc.claim(ctx)
	if !ok || job.GroupID != g.ID || job.Month != "2026-10" || job.State != simpleJobRunning || job.LeaseUntil <= svc.now().Unix() {
		t.Fatalf("claim: %+v %v", job, ok)
	}
	if _, ok := svc.claim(ctx); ok {
		t.Fatal("leased job claimed twice")
	}
}

// A wallet difference larger than a month's consumption changes nothing on the
// bills: generating a later month first and then earlier ones reuses the frozen
// log-derived balances, and every month still equals its logs.
func TestSimpleMonthlyGenerationEarlierMonthAfterLaterMonth(t *testing.T) {
	m, svc, _, _, g, src := simpleTestService(t)
	ctx := context.Background()
	facts := svc.facts.(*simpleFakeFacts)
	sepStart := simpleCST(2026, 9, 1, 0)
	for i := range facts.days {
		d := &facts.days[i]
		if d.UserID == 201 && d.DateTS < sepStart {
			src.snap.Quota[201] += d.Consume - d.Refund - 10
			d.Consume, d.Refund = 10, 0 // August total far below the 909785 difference
		}
	}
	run := func(month string) {
		t.Helper()
		if _, err := svc.SubmitMonthly(ctx, g.ID, g.PortalAuthVer, month); err != nil {
			t.Fatal(err)
		}
		if !svc.runOne(ctx) {
			t.Fatalf("no job ran for %s", month)
		}
		var job PortalMonthlySimpleJob
		m.storeDB.First(&job, "group_id = ? AND month = ?", g.ID, month)
		if job.State != simpleJobDone {
			t.Fatalf("job %s ended %s: %s", month, job.State, job.LastError)
		}
	}
	run("2026-10")
	run("2026-08")
	run("2026-09")
	var audits []PortalMonthlySimpleAudit
	m.storeDB.Where("user_id = ?", 201).Order("month").Find(&audits)
	if len(audits) != 3 {
		t.Fatalf("audit rows: %d", len(audits))
	}
	for i, a := range audits {
		if a.WalletResidual != 909785 {
			t.Fatalf("wallet residual %s: %d", a.Month, a.WalletResidual)
		}
		if a.Consume != a.LogConsume || !simpleWithinCent(a.ClosingCents, a.ClosingRaw) || !simpleWithinCent(a.ConsumeCents, a.Consume) {
			t.Fatalf("audit %s: %+v", a.Month, a)
		}
		if i > 0 && audits[i-1].ClosingCents != a.OpeningCents {
			t.Fatalf("%s opening %d != previous closing %d", a.Month, a.OpeningCents, audits[i-1].ClosingCents)
		}
	}
}

// The difference belongs to an account that registered after the requested
// month: it is recorded against that account only, never another member.
func TestSimpleMonthlyGenerationEarlierMonthBeforeMemberRegistered(t *testing.T) {
	m, svc, _, _, g, src := simpleTestService(t)
	ctx := context.Background()
	src.snap.Quota[201] -= 909785 // move the historical difference ...
	src.snap.Quota[202] += 40000  // ... to the account registered on 10 Sep
	for _, month := range []string{"2026-10", "2026-08", "2026-09"} {
		if _, err := svc.SubmitMonthly(ctx, g.ID, g.PortalAuthVer, month); err != nil {
			t.Fatal(err)
		}
		if !svc.runOne(ctx) {
			t.Fatalf("no job ran for %s", month)
		}
		var job PortalMonthlySimpleJob
		m.storeDB.First(&job, "group_id = ? AND month = ?", g.ID, month)
		if job.State != simpleJobDone {
			t.Fatalf("job %s ended %s: %s", month, job.State, job.LastError)
		}
	}
	var audits []PortalMonthlySimpleAudit
	m.storeDB.Find(&audits)
	for _, a := range audits {
		want := map[int64]int64{201: 0, 202: 40000}[a.UserID]
		if a.WalletResidual != want || a.Consume != a.LogConsume {
			t.Fatalf("residual on the wrong account or consumption changed: %+v", a)
		}
	}
}

// Randomized generation order: differences on random accounts, months without
// consumption, and months requested in any order. Every bill must be issued,
// adjacent bills must chain, consumption always equals the logs, and each
// difference is recorded against its own account only.
func TestSimpleMonthlyGenerationRandomOrderProperties(t *testing.T) {
	rng := rand.New(rand.NewSource(20261106))
	orders := [][]string{
		{"2026-08", "2026-09", "2026-10"}, {"2026-08", "2026-10", "2026-09"},
		{"2026-09", "2026-08", "2026-10"}, {"2026-09", "2026-10", "2026-08"},
		{"2026-10", "2026-08", "2026-09"}, {"2026-10", "2026-09", "2026-08"},
	}
	for c := 0; c < 36; c++ {
		m, svc, _, _, g, src := simpleTestService(t)
		ctx := context.Background()
		facts := svc.facts.(*simpleFakeFacts)
		src.snap.Quota[201] -= 909785 // start from no difference
		want := map[int64]int64{201: 0, 202: 0}
		for _, uid := range []int64{201, 202} {
			if rng.Intn(3) > 0 {
				want[uid] = 1 + rng.Int63n(2_000_000) // up to 4 yuan
				src.snap.Quota[uid] += want[uid]
			}
		}
		// Drop consumption of random August/September days per account so the
		// difference moves to a later month.
		octStart := simpleCST(2026, 10, 1, 0)
		quiet := map[int64]bool{201: rng.Intn(2) == 0, 202: rng.Intn(2) == 0}
		for i := range facts.days {
			d := &facts.days[i]
			if d.DateTS < octStart && quiet[d.UserID] && rng.Intn(10) > 0 {
				src.snap.Quota[d.UserID] += d.Consume - d.Refund
				d.Consume, d.Refund = 0, 0
			}
		}
		order := orders[c%len(orders)]
		for _, month := range order {
			if _, err := svc.SubmitMonthly(ctx, g.ID, g.PortalAuthVer, month); err != nil {
				t.Fatal(err)
			}
			if !svc.runOne(ctx) {
				t.Fatalf("case %d: no job ran for %s", c, month)
			}
			var job PortalMonthlySimpleJob
			m.storeDB.First(&job, "group_id = ? AND month = ?", g.ID, month)
			if job.State != simpleJobDone {
				t.Fatalf("case %d order %v: job %s ended %s: %s (want %v quiet %v)", c, order, month, job.State, job.LastError, want, quiet)
			}
		}
		var audits []PortalMonthlySimpleAudit
		m.storeDB.Order("user_id, month").Find(&audits)
		prev := map[int64]PortalMonthlySimpleAudit{}
		for _, a := range audits {
			if a.WalletResidual != want[a.UserID] {
				t.Fatalf("case %d order %v: user %d %s residual %d, want %d", c, order, a.UserID, a.Month, a.WalletResidual, want[a.UserID])
			}
			if a.Consume != a.LogConsume || a.Consume < 0 || !simpleWithinCent(a.ClosingCents, a.ClosingRaw) ||
				!simpleWithinCent(a.OpeningCents, a.OpeningRaw) || !simpleWithinCent(a.ConsumeCents, a.Consume) {
				t.Fatalf("case %d: audit %+v", c, a)
			}
			if p, ok := prev[a.UserID]; ok && (p.ClosingCents != a.OpeningCents || p.ClosingRaw != a.OpeningRaw) {
				t.Fatalf("case %d: user %d %s opening does not continue %s", c, a.UserID, a.Month, p.Month)
			}
			prev[a.UserID] = a
		}
	}
}

func TestSimpleMonthlyGenerationRestartRetriesHeldRequests(t *testing.T) {
	m, svc, _, _, g, _ := simpleTestService(t)
	for _, j := range []PortalMonthlySimpleJob{
		{GroupID: g.ID, Month: "2026-08", AuthVersion: g.PortalAuthVer, State: simpleJobReview, Attempts: 40, NextAt: 1, LastError: "held"},
		{GroupID: g.ID, Month: "2026-09", AuthVersion: g.PortalAuthVer, State: simpleJobDone, NextAt: 1},
		{GroupID: g.ID, Month: "2026-10", AuthVersion: g.PortalAuthVer, State: simpleJobRunning, Attempts: 2, NextAt: 1, LeaseUntil: 1 << 40},
	} {
		if err := m.storeDB.Create(&j).Error; err != nil {
			t.Fatal(err)
		}
	}
	svc.requeueHeld(context.Background())
	var held, done PortalMonthlySimpleJob
	m.storeDB.First(&held, "group_id = ? AND month = ?", g.ID, "2026-08")
	m.storeDB.First(&done, "group_id = ? AND month = ?", g.ID, "2026-09")
	if held.State != simpleJobQueued || held.Attempts != 0 || held.NextAt != svc.now().Unix() {
		t.Fatalf("held request not requeued: %+v", held)
	}
	if done.State != simpleJobDone {
		t.Fatalf("done request changed: %+v", done)
	}
	var interrupted PortalMonthlySimpleJob
	m.storeDB.First(&interrupted, "group_id = ? AND month = ?", g.ID, "2026-10")
	if interrupted.State != simpleJobQueued || interrupted.LeaseUntil != 0 || interrupted.Attempts != 2 {
		t.Fatalf("interrupted request not resumed: %+v", interrupted)
	}
	for i := 0; i < 2; i++ {
		if !svc.runOne(context.Background()) {
			t.Fatal("requeued request did not run")
		}
	}
	m.storeDB.First(&held, "group_id = ? AND month = ?", g.ID, "2026-08")
	if held.State != simpleJobDone {
		t.Fatalf("requeued request ended %s: %s", held.State, held.LastError)
	}
}

// Customer clicks and calendar reads must not wait on the replica once the
// members' registration times are known.
func TestSimpleMonthlyGenerationAdmissionUsesCachedRegistrations(t *testing.T) {
	_, svc, r, cookie, g, src := simpleTestService(t)
	svc.warmRegistrations(context.Background())
	if src.regCalls.Load() != 1 {
		t.Fatalf("warm-up registration reads: %d", src.regCalls.Load())
	}
	for i := 0; i < 20; i++ {
		if w := generationHTTPDo(r, "POST", "", `{"month":"2026-10"}`, cookie, nil); w.Code != http.StatusAccepted {
			t.Fatalf("submit: %d %s", w.Code, w.Body.String())
		}
		if _, err := svc.ReadMonthlyCalendar(context.Background(), g.ID, g.PortalAuthVer); err != nil {
			t.Fatal(err)
		}
	}
	if src.regCalls.Load() != 1 {
		t.Fatalf("admission read the replica again: %d", src.regCalls.Load())
	}
}

// A burst of simultaneous clicks from many organizations is admitted rather
// than turned away as busy.
func TestSimpleMonthlyGenerationBurstOfClicksIsAdmitted(t *testing.T) {
	_, svc, r, cookie, _, _ := simpleTestService(t)
	svc.warmRegistrations(context.Background())
	var wg sync.WaitGroup
	codes := make(chan int, 60)
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes <- generationHTTPDo(r, "POST", "", `{"month":"2026-10"}`, cookie, nil).Code
		}()
	}
	wg.Wait()
	close(codes)
	for code := range codes {
		if code != http.StatusAccepted {
			t.Fatalf("burst click answered %d", code)
		}
	}
}

func TestSimpleWeekStartAlignsToSharedGrid(t *testing.T) {
	monday := simpleCST(2026, 6, 1, 0) // 2026-06-01 is a Monday
	for _, at := range []int64{monday, monday + 1, monday + simpleWeek - 1, simpleCST(2026, 6, 3, 15)} {
		if got := simpleWeekStart(at); got != monday {
			t.Fatalf("week start of %d = %d, want %d", at, got, monday)
		}
	}
	if got := simpleWeekStart(monday + simpleWeek); got != monday+simpleWeek {
		t.Fatalf("next week start = %d", got)
	}
	early := simpleCST(2025, 12, 31, 12) // before the grid epoch
	if got := simpleWeekStart(early); got > early || early-got >= simpleWeek || (simpleCST(2026, 1, 5, 0)-got)%simpleWeek != 0 {
		t.Fatalf("week start before epoch = %d", got)
	}
}

// A release stops the service mid-job: the request goes straight back to the
// queue without a penalty, whatever error the cancellation produced.
func TestSimpleMonthlyGenerationShutdownRequeuesWithoutPenalty(t *testing.T) {
	m, svc, _, _, g, _ := simpleTestService(t)
	if err := m.storeDB.Create(&PortalMonthlySimpleJob{GroupID: g.ID, Month: "2026-10", AuthVersion: g.PortalAuthVer, State: simpleJobRunning, Attempts: 3, LeaseUntil: 1 << 40}).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc.finish(ctx, PortalMonthlySimpleJob{GroupID: g.ID, Month: "2026-10", Attempts: 3}, errSimpleReview("publication: context canceled"))
	var job PortalMonthlySimpleJob
	m.storeDB.First(&job, "group_id = ? AND month = ?", g.ID, "2026-10")
	if job.State != simpleJobQueued || job.Attempts != 3 || job.LeaseUntil != 0 || job.NextAt != svc.now().Unix() {
		t.Fatalf("interrupted job: %+v", job)
	}
}

// Small credits on several accounts: every account's credit total (not only the
// organization's) must be shown within one cent.
func TestSimpleMonthlyGenerationCreditTotalsPerAccountWithinCent(t *testing.T) {
	m, svc, _, _, g, src := simpleTestService(t)
	at := simpleCST(2026, 10, 12, 9)
	id := int64(5000)
	for _, c := range []struct{ uid, q int64 }{{201, 7500}, {201, 7500}, {201, 7500}, {202, 12000}, {202, 12000}, {202, 12000}} {
		id++
		at += 60
		src.snap.Events = append(src.snap.Events, simpleEvent{ID: id, UserID: c.uid, At: at, Delta: c.q, Kind: "gift", Method: "签到赠送"})
		src.snap.Quota[c.uid] += c.q
	}
	if _, err := svc.SubmitMonthly(context.Background(), g.ID, g.PortalAuthVer, "2026-10"); err != nil {
		t.Fatal(err)
	}
	svc.runOne(context.Background())
	var job PortalMonthlySimpleJob
	m.storeDB.First(&job, "group_id = ? AND month = ?", g.ID, "2026-10")
	if job.State != simpleJobDone {
		t.Fatalf("job ended %s: %s", job.State, job.LastError)
	}
	var audits []PortalMonthlySimpleAudit
	m.storeDB.Where("month = ?", "2026-10").Find(&audits)
	for _, a := range audits {
		if !simpleWithinCent(a.CreditCents, a.CreditRaw) {
			t.Fatalf("user %d credit shown %d cents, true %d quota", a.UserID, a.CreditCents, a.CreditRaw)
		}
	}
}

// Publication failures have their own budget: waiting retries earlier in the
// same request (facts not ready, balance moving) do not use it up.
func TestSimpleMonthlyGenerationPublicationRetriesOwnBudget(t *testing.T) {
	m, svc, _, _, g, _ := simpleTestService(t)
	ctx := context.Background()
	job := PortalMonthlySimpleJob{GroupID: g.ID, Month: "2026-10", AuthVersion: g.PortalAuthVer, State: simpleJobRunning, Attempts: 7, CreatedAt: 1, UpdatedAt: 1}
	if err := m.storeDB.Create(&job).Error; err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= simplePublishAttempts; i++ {
		svc.finish(ctx, job, errSimplePublication("publication: busy"))
		if err := m.storeDB.First(&job, "group_id = ? AND month = ?", g.ID, "2026-10").Error; err != nil {
			t.Fatal(err)
		}
		want := simpleJobRetry
		if i == simplePublishAttempts {
			want = simpleJobReview
		}
		if job.State != want || job.PublishAttempts != i {
			t.Fatalf("publication failure %d: state %s publish_attempts %d (want %s)", i, job.State, job.PublishAttempts, want)
		}
	}
}

// Clicking again while a request runs keeps its progress clock, and an attempt
// that still holds the previous organization version runs again with the new
// one instead of being held for review.
func TestSimpleMonthlyGenerationResubmitWhileRunning(t *testing.T) {
	m, svc, _, _, g, _ := simpleTestService(t)
	ctx := context.Background()
	old := PortalMonthlySimpleJob{GroupID: g.ID, Month: "2026-10", AuthVersion: g.PortalAuthVer - 1, State: simpleJobRunning, LeaseUntil: svc.now().Unix() + 600, CreatedAt: 100, UpdatedAt: 200}
	if err := m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SubmitMonthly(ctx, g.ID, g.PortalAuthVer, "2026-10"); err != nil {
		t.Fatal(err)
	}
	var job PortalMonthlySimpleJob
	m.storeDB.First(&job, "group_id = ? AND month = ?", g.ID, "2026-10")
	if job.State != simpleJobRunning || job.UpdatedAt != 200 || job.AuthVersion != g.PortalAuthVer {
		t.Fatalf("resubmit while running: %+v", job)
	}
	err := svc.generate(ctx, old)
	var retry errSimpleRetry
	if !errors.As(err, &retry) {
		t.Fatalf("stale-version attempt: %v (want retry)", err)
	}
}
