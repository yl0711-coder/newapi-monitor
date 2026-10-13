package monitor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// Explicitly opt-in loopback fixture. All accounts/channels are synthetic,
// and only token config/check routes exist: no finance/upstream sync writes.
func TestUpstreamTokenUIBrowserFixture(t *testing.T) {
	if os.Getenv("MONITOR_TOKEN_UI_ACCEPTANCE") != "1" {
		t.Skip("manual loopback browser fixture")
	}
	var m *Monitor
	var recoveryFixture *tokenReconcileFixture
	var protectedBefore string
	if os.Getenv("MONITOR_TOKEN_UI_RECOVERY") == "1" {
		f, now := tokenRecoveryStatusFixture(t)
		f.m.syncDueUpstreamTokenWorkAt(context.Background(), now)
		m, recoveryFixture = f.m, &f
		protectedBefore = tokenPublishedSnapshot(t, f)
		job := ChannelUpstreamTokenWorkState{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), Kind: "scan_day", DayTs: f.day, Status: "reconnect", InputRevision: m.sub2TokenRecoveryCredentialRevision(f.account), NextAttemptAt: upstreamAccountIsolatedUntil, UpdatedAt: time.Now().Unix()}
		if err := m.storeDB.Create(&job).Error; err != nil {
			t.Fatal(err)
		}
	} else {
		m, _ = tokenConfigFixture(t)
	}
	m.cfg.SessionSecret = "synthetic-loopback-acceptance-only"
	router := tokenConfigRouter(m)
	start := strings.Index(pageHTML, `  <div class="cm-finance-mask"`)
	end := strings.Index(pageHTML, `  <div class="cm-upstream-mask"`)
	if start < 0 || end <= start {
		t.Fatal("finance fragment not found")
	}
	js := string(channelManagementJS)
	insert := strings.LastIndex(js, "})();")
	js = js[:insert] + `
document.addEventListener('DOMContentLoaded',()=>{
  init();
  cm.report={finance:{can_edit:true},domains:[{key:'fixture',domain:'sub2.example',configured:true,finance:{},vendors:[{name:'fixture',channels:[
  {id:185,name:'codex-0.15 · 本机合成渠道',current:true,status:1,groups:[],configured_groups:['group-a','group-b']},
  {id:186,name:'codex-0.15x-plus · 本机合成渠道',current:true,status:1,groups:[],configured_groups:['group-c']}
  ]}]}]};
  document.getElementById('fixtureOpen').addEventListener('click',()=>openFinance('fixture'));
  document.getElementById('fixtureRecovery').addEventListener('change',async(event)=>{
    const response=await fetch('/fixture/recovery',{method:'POST',headers:{'Content-Type':'text/plain'},body:event.target.value});
    document.getElementById('fixtureHint').textContent=response.ok?'合成状态已切换，请点击渠道中的“重新读取”。':'状态切换失败';
  });
  openFinance('fixture');
});
` + js[insert:]
	done := make(chan struct{})
	var once sync.Once
	// Cookies are shared across localhost ports. Keep this synthetic login
	// separate so acceptance cannot overwrite another local Monitor session.
	const fixtureCookie = "monitor_token_ui_acceptance"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		switch r.URL.Path {
		case "/":
			http.SetCookie(w, &http.Cookie{Name: fixtureCookie, Value: m.signSession("fixture", roleRoot, time.Now().Unix()), Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(`<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>本机令牌配置验收 · 合成数据</title><link rel="stylesheet" href="/fixture.css"><body style="background:#10131c;color:#dce4f2;font-family:sans-serif"><p>仅本机合成数据，无线上连接；这里允许测试保存。</p><button id="fixtureOpen">打开倍率配置</button><label>合成补扫状态 <select id="fixtureRecovery"><option value="reconnect">鉴权暂停</option><option value="retry">限频重试</option><option value="observed">该日扫描核验完成</option></select></label><span id="fixtureHint"></span><div class="channel-management-page">` + pageHTML[start:end] + `</div><script src="/fixture.js"></script></body></html>`))
		case "/fixture/recovery":
			if r.Method != http.MethodPost || recoveryFixture == nil {
				http.NotFound(w, r)
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, 32)
			b, err := io.ReadAll(r.Body)
			status := string(b)
			if err != nil || !(status == "reconnect" || status == "retry" || status == "observed") {
				http.Error(w, "invalid synthetic state", 400)
				return
			}
			f := *recoveryFixture
			next := time.Now().Unix() + 300
			if status == "reconnect" {
				next = upstreamAccountIsolatedUntil
			}
			if err := upstreamTokenWorkScope(m.storeDB.Model(&ChannelUpstreamTokenWorkState{}), ChannelUpstreamTokenWorkState{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), Kind: "scan_day", DayTs: f.day}).UpdateColumns(map[string]any{"status": status, "next_attempt_at": next, "updated_at": time.Now().Unix()}).Error; err != nil {
				http.Error(w, "synthetic update failed", 500)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case "/fixture.css":
			w.Header().Set("Content-Type", "text/css")
			_, _ = w.Write(stabilityCSS)
		case "/fixture.js":
			w.Header().Set("Content-Type", "application/javascript")
			_, _ = w.Write([]byte(js))
		case "/finish":
			once.Do(func() { close(done) })
			_, _ = w.Write([]byte("finished"))
		case "/channels/upstream/token-config", "/channels/upstream/token-check":
			local := r.Clone(r.Context())
			local.Header.Del("Cookie")
			if cookie, err := r.Cookie(fixtureCookie); err == nil {
				local.AddCookie(&http.Cookie{Name: sessionCookie, Value: cookie.Value})
			}
			router.ServeHTTP(w, local)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	t.Log("LOCAL_SYNTHETIC_TOKEN_UI", server.URL)
	select {
	case <-done:
	case <-time.After(30 * time.Minute):
		t.Fatal("manual fixture expired")
	}
	if recoveryFixture != nil {
		if tokenPublishedSnapshot(t, *recoveryFixture) != protectedBefore {
			t.Fatal("browser modified protected money")
		}
		return
	}
	for _, model := range []any{&ChannelFinanceVersion{}, &ChannelDomainCost{}, &ChannelFinanceChannelCost{}, &ChannelCostSourceBinding{}, &ChannelEconomicsDirtyHour{}} {
		var count int64
		if err := m.storeDB.Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("UI modified protected model %T: %v", model, err)
		}
	}
}
