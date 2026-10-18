package monitor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

// Explicit opt-in, test-only snapshot viewer. No database, credentials, sync,
// production routes, or outbound clients are created here.
func TestFinanceLocalSnapshotPreview(t *testing.T) {
	if os.Getenv("MONITOR_LOCAL_SNAPSHOT_PREVIEW") != "1" {
		t.Skip("manual local snapshot display acceptance only")
	}
	dir := os.Getenv("MONITOR_CUR_LOCAL_EVIDENCE_DIR")
	raw, err := os.ReadFile(filepath.Join(dir, financeCURLocalPolicyName(t)))
	if err != nil {
		t.Fatal(err)
	}
	var rules []financecur.AllocationRule
	if err := json.Unmarshal(raw, &rules); err != nil {
		t.Fatal(err)
	}
	policyHash, err := financecur.AllocationPolicyHash(rules)
	if err != nil {
		t.Fatal(err)
	}
	var artifacts []financecur.Artifact
	for _, name := range []string{"2026-09.parquet", "2026-10.parquet"} {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			f.Close()
			t.Fatal(err)
		}
		if _, err := f.Seek(0, 0); err != nil {
			f.Close()
			t.Fatal(err)
		}
		audit, err := financecur.ReadParquet(f, financecur.Limits{})
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		allocated, err := financecur.Allocate(audit.CostEntries, rules)
		if err != nil {
			t.Fatal(err)
		}
		s, timeline, err := financecur.BuildStatement(audit, allocated, hex.EncodeToString(h.Sum(nil)), policyHash, "Asia/Shanghai", false)
		if err != nil {
			t.Fatal(err)
		}
		a, err := financecur.NewArtifact(s, timeline)
		if err != nil {
			t.Fatal(err)
		}
		artifacts = append(artifacts, a)
	}
	path := filepath.Join(t.TempDir(), "source-set.json")
	financeCURWriteSet(t, path, artifacts...)
	withCE := os.Getenv("MONITOR_LOCAL_CE_MONTHS") == "1"
	if withCE {
		if financeCURLocalPolicyName(t) != "confirmed-account-wide-policy.json" {
			t.Fatal("historical monthly preview requires confirmed whole-account scope")
		}
		if err := os.WriteFile(path, financeAWSLocalAccountingBundle(t, dir, artifacts), 0600); err != nil {
			t.Fatal(err)
		}
	}
	raw, err = os.ReadFile(os.Getenv("MONITOR_LOCAL_SNAPSHOT_REPORT"))
	if err != nil {
		t.Fatal(err)
	}
	var saved struct {
		Report *financeOperatingReport `json:"after_rebuild"`
	}
	if err := json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	r := saved.Report
	if r == nil || !r.Enabled || r.From != 1777564800 || (r.To != 1791475200 && r.To != 1790784000) {
		t.Fatal("unexpected snapshot range")
	}
	m := &Monitor{cfg: Settings{FinanceCURArtifactEnabled: true, FinanceCURArtifactPath: path}}
	m.applyFinanceCURArtifactToReport(r)
	if r.CURCost.Status != "draft" || r.Statement.OperatingProfit != nil {
		t.Fatal("preview must retain incomplete evidence gate")
	}
	r.Sources = financeSourceViews(r.UserCoverage, r.UpstreamCoverage, r.Statement, r.GiftCoverage, r.CURCost)
	var notices []string
	for _, notice := range r.Notices {
		if !strings.Contains(notice, "AWS") && !strings.Contains(notice, "CUR") {
			notices = append(notices, notice)
		}
	}
	scopeNotice := "AWS 使用留存账单及选定归属规则；未完成同期间核验，不代表最终毛利。"
	if financeCURLocalPolicyName(t) == "confirmed-account-wide-policy.json" {
		scopeNotice = "AWS 整账号费用全部计入，含实际抵扣，不再按业务拆分；账单完整性与最终毛利仍需单独核验。"
	}
	if withCE {
		if len(r.AWSMonthlyBills) != 4 {
			t.Fatal("historical monthly preview is missing evidence")
		}
		scopeNotice += "5—8 月 CE、9—10 月 CUR 按 AWS 账单所属月份用于下方月度经营核算；不摊入北京时间日成本。非完整月份不扣整月账单，暂不计算月账口径利润。"
	}
	notices = append(notices, "本机只读快照展示验收；"+scopeNotice)
	r.Notices = notices
	reportJSON, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	start := strings.Index(pageHTML, `<div class="tabpane finance-page" id="tab-finance" hidden>`)
	end := strings.Index(pageHTML, `</div><!-- /tab-finance -->`)
	styleStart := strings.Index(pageHTML, "<style>")
	styleEnd := strings.Index(pageHTML, "</style>")
	if start < 0 || end < start || styleStart < 0 || styleEnd < styleStart {
		t.Fatal("production markup changed")
	}
	fragment := strings.Replace(pageHTML[start:end+len(`</div><!-- /tab-finance -->`)], `id="tab-finance" hidden`, `id="tab-finance"`, 1)
	loc, _ := time.LoadLocation("Asia/Shanghai")
	fromDate := time.Unix(r.From, 0).In(loc).Format("2006-01-02")
	toDate := time.Unix(r.To-1, 0).In(loc).Format("2006-01-02")
	banner := fmt.Sprintf("本机只读展示验收 · 当前开发代码 · 非线上实时数据<br>固定区间：%s 至 %s（北京时间）；业务快照生成于 %s。%s<br>日期、配置与同步操作已禁用；本页不连接数据库或上游。", fromDate, toDate, time.Unix(r.GeneratedAt, 0).In(loc).Format("2006-01-02 15:04:05"), scopeNotice)
	html := `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>本机经营核算只读验收</title>` + pageHTML[styleStart:styleEnd+len("</style>")] + `<link rel="stylesheet" href="/stability.css"><link rel="stylesheet" href="/finance.css"><style>body{max-width:none;margin:0;padding:0;background:#10131b;color:#e3e8f2}.preview-shell{display:grid;grid-template-columns:220px minmax(0,1fr);min-height:100vh}.preview-side{padding:28px 20px;background:#161b27;border-right:1px solid #30394c}.preview-side p{color:#9da9bd;line-height:2}.preview-main{padding:24px;min-width:0}.preview-banner{padding:16px 20px;border:1px solid #8d7134;border-radius:10px;background:#30291b;color:#f4dc9f;line-height:1.7;margin-bottom:20px}.preview-side b{color:#a7a1ff}button:disabled,input:disabled,textarea:disabled{cursor:not-allowed;opacity:.5}@media(max-width:760px){.preview-shell{grid-template-columns:1fr}.preview-side{display:none}.preview-main{padding:12px}}</style></head><body theme-mode="dark"><div class="preview-shell"><aside class="preview-side"><h3>NexusAPI Monitor</h3><p>本机隔离验收<br>用户用量 · 未载入<br>渠道管理 · 未载入<br>稳定性报表 · 未载入</p><b>经营核算</b></aside><main class="preview-main"><div class="preview-banner">` + banner + `</div>` + fragment + `</main></div><script src="/echarts.js"></script><script src="/finance.js"></script><script src="/preview.js"></script></body></html>`
	bootstrap := fmt.Sprintf(`document.getElementById('finFrom').value=%q;document.getElementById('finTo').value=%q;`, fromDate, toDate) + `for(const id of ['finSinceLaunch','finLastMonth','finThisMonth','finFrom','finTo','finInternalSave','finInternalAccounts']){document.getElementById(id).disabled=true;}window.financeActivate();new MutationObserver(()=>{document.querySelectorAll('[data-fin-cost-source]').forEach(b=>{b.disabled=true;b.title='本次只验收经营核算展示，未载入渠道配置';});}).observe(document.getElementById('finReportContent'),{childList:true,subtree:true});`
	// The configuration editor is outside this snapshot acceptance. Do not
	// fabricate an empty account list or manufacture a production API failure.
	initCall := "    loadInternalAccounts();\n    $('finUnallocatedSourceRows')"
	if strings.Count(string(financeJS), initCall) != 1 {
		t.Fatal("preview configuration boundary changed")
	}
	previewJS := strings.Replace(string(financeJS), initCall, "    $('finInternalSummary').textContent = '本机预览未载入配置目录';\n    $('finInternalState').textContent = '计算沿用快照中的内部账号规则；此处不加载、也不能修改配置。';\n    $('finUnallocatedSourceRows')", 1)
	assets := map[string]struct {
		kind string
		body []byte
	}{
		"/":               {"text/html; charset=utf-8", []byte(html)},
		"/finance.css":    {"text/css; charset=utf-8", financeCSS},
		"/stability.css":  {"text/css; charset=utf-8", stabilityCSS},
		"/finance.js":     {"application/javascript", []byte(previewJS)},
		"/echarts.js":     {"application/javascript", echartsJS},
		"/preview.js":     {"application/javascript", []byte(bootstrap)},
		"/finance/report": {"application/json", reportJSON},
	}
	port := 8156
	if value := os.Getenv("MONITOR_LOCAL_PREVIEW_PORT"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1024 || parsed > 65535 {
			t.Fatal("invalid preview port")
		}
		port = parsed
	}
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	listener, err := net.Listen("tcp4", address)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server := &http.Server{ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 30 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Host != address || (req.Header.Get("Origin") != "" && req.Header.Get("Origin") != "http://"+address) || req.Header.Get("Sec-Fetch-Site") == "cross-site" {
			http.Error(w, "local origin only", http.StatusForbidden)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; font-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
		if req.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, "read-only preview", http.StatusMethodNotAllowed)
			return
		}
		if req.URL.Path == "/finance/report" {
			q := req.URL.Query()
			if (q.Get("from") != "" && q.Get("from") != fromDate) || (q.Get("to") != "" && q.Get("to") != toDate) {
				http.Error(w, "fixed snapshot range only", http.StatusBadRequest)
				return
			}
		}
		asset, ok := assets[req.URL.Path]
		if !ok {
			http.NotFound(w, req)
			return
		}
		w.Header().Set("Content-Type", asset.kind)
		if _, err := w.Write(asset.body); err != nil {
			t.Logf("local preview response interrupted: %v", err)
		}
	})}
	t.Logf("READY http://%s/#tab=finance — fixed local snapshot, no database/outbound sync; GET only", address)
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		t.Fatal(err)
	}
}
