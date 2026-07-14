package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// 验证报警配置接口:默认值、保存、密码不回显、留空保留原密码。不连生产库。
func TestAlertConfigRoundtrip(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := &Monitor{cfg: Settings{SessionSecret: "test-secret"}, chNames: map[string]string{}}
	if err := m.openStore(t.TempDir() + "/t.db"); err != nil {
		t.Fatalf("openStore: %v", err)
	}
	r := gin.New()
	m.RegisterRoutes(r)

	// 超级管理员会话 cookie(配置接口需 root)
	rootCookie := &http.Cookie{Name: sessionCookie, Value: m.signSession("root-tester", roleRoot, time.Now().Unix())}

	get := func() map[string]any {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/alert/config", nil)
		req.AddCookie(rootCookie)
		r.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("GET /alert/config = %d", w.Code)
		}
		var out map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	post := func(body string) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/alert/config", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(rootCookie)
		r.ServeHTTP(w, req)
		if w.Code != 200 {
			t.Fatalf("POST /alert/config = %d: %s", w.Code, w.Body.String())
		}
	}

	// 1) 未存过 → 返回建议默认值(err_rate_pct=20),密码未设置
	d := get()
	cfg := d["config"].(map[string]any)
	if cfg["err_rate_pct"].(float64) != 20 {
		t.Errorf("默认 err_rate_pct 应为 20,实际 %v", cfg["err_rate_pct"])
	}
	if d["smtp_password_set"].(bool) {
		t.Error("未存过时 smtp_password_set 应为 false")
	}

	// 2) 保存含密码的完整配置
	post(`{"enabled":true,"smtp_host":"smtp.x.com","smtp_port":465,"smtp_ssl":true,
		"smtp_user":"a@b.com","smtp_password":"secret123","smtp_from":"a@b.com",
		"recipients":"x@y.com, z@w.com","eval_window_min":15,"err_rate_pct":30,
		"err_min_count":5,"err_burst_count":10,"anomaly_burst_buckets":3,
		"anomaly_min_count":8,"sampler_down_enabled":true,"cooldown_min":30}`)

	d = get()
	cfg = d["config"].(map[string]any)
	if cfg["smtp_password"].(string) != "" {
		t.Error("GET 不应回显密码明文")
	}
	if !d["smtp_password_set"].(bool) {
		t.Error("保存密码后 smtp_password_set 应为 true")
	}
	if cfg["err_rate_pct"].(float64) != 30 {
		t.Errorf("保存后 err_rate_pct 应为 30,实际 %v", cfg["err_rate_pct"])
	}

	// 3) 密码留空再存 → 应保留原密码(底层仍是 secret123)
	post(`{"enabled":true,"smtp_host":"smtp.x.com","recipients":"x@y.com","smtp_password":"","err_rate_pct":25}`)
	if got := m.loadAlertConfig().SMTPPassword; got != "secret123" {
		t.Errorf("留空保存应保留原密码,实际 %q", got)
	}
	if m.loadAlertConfig().ErrRatePct != 25 {
		t.Errorf("err_rate_pct 应更新为 25")
	}
}

func TestRecipientList(t *testing.T) {
	got := recipientList("a@x.com, b@y.com\nc@z.com; d@w.com  e@v.com")
	if len(got) != 5 {
		t.Fatalf("应解析出 5 个收件人,实际 %d: %v", len(got), got)
	}
}

// 验证三栏目邮件开关:关闭的栏目 fire 不发邮件但仍记「最近告警」;打开的栏目会尝试发送(无收件人→记 _FAILED)。
func TestAlertCategoryGate(t *testing.T) {
	m := &Monitor{cfg: Settings{SessionSecret: "test-secret"}, chNames: map[string]string{}}
	if err := m.openStore(t.TempDir() + "/t.db"); err != nil {
		t.Fatalf("openStore: %v", err)
	}
	now := time.Now().Unix()
	c := defaultAlertConfig()
	c.ServerAlertsEnabled = false // 关服务端栏目
	c.ModelAlertsEnabled = true   // 开模型栏目(无收件人,发送会失败→_FAILED)

	m.fire(c, "infra_db_mem", "DB-X", "数据库可用内存告急", "b", now)
	m.fire(c, "error_rate", "ch1", "错误率超阈值", "b", now)

	var logs []AlertLog
	if err := m.storeDB.Order("id").Find(&logs).Error; err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 {
		t.Fatalf("应记 2 条告警,实际 %d: %+v", len(logs), logs)
	}
	// 服务端栏目关:kind 原样、注明未发邮件
	if logs[0].Kind != "infra_db_mem" || !strings.Contains(logs[0].Detail, "未发邮件") {
		t.Errorf("关栏目应静音入日志 = %+v", logs[0])
	}
	// 模型栏目开:走发送,无收件人失败 → kind_FAILED
	if logs[1].Kind != "error_rate_FAILED" {
		t.Errorf("开栏目应尝试发送(无收件人→_FAILED) = %+v", logs[1])
	}
	// 归类函数本身(仅 模型/服务端 两栏目)
	if alertCategory("infra_probe_cert") != "server" || alertCategory("burn_fast") != "model" || alertCategory("sampler_down") != "model" {
		t.Error("alertCategory 归类不对")
	}
}

// 回归:栏目开关保存 false 必须真的持久化为 false(曾因 gorm default:true 把零值顶回 true——"勾掉保存又勾上")。
func TestAlertCategoryTogglePersistsFalse(t *testing.T) {
	m := &Monitor{cfg: Settings{SessionSecret: "test-secret"}, chNames: map[string]string{}}
	if err := m.openStore(t.TempDir() + "/t.db"); err != nil {
		t.Fatalf("openStore: %v", err)
	}
	c := defaultAlertConfig()
	c.ServerAlertsEnabled = false // 勾掉服务端
	c.ModelAlertsEnabled = true
	if err := m.saveAlertConfig(c); err != nil {
		t.Fatal(err)
	}
	got := m.loadAlertConfig()
	if got.ServerAlertsEnabled {
		t.Fatal("服务端开关保存 false 后被顶回 true(default 零值坑复发)")
	}
	if !got.ModelAlertsEnabled {
		t.Fatal("模型开关应保持 true")
	}
	// 再保存一次全 false,同样要持久化
	c.ModelAlertsEnabled = false
	if err := m.saveAlertConfig(c); err != nil {
		t.Fatal(err)
	}
	if got := m.loadAlertConfig(); got.ModelAlertsEnabled || got.ServerAlertsEnabled {
		t.Fatalf("全关未持久化 = %+v", got)
	}
}

// 回归:老库(无栏目开关列)升级后,存量配置行两开关自动补 true(行为不变),而非静默全关。
func TestAlertCategoryToggleMigrationDefaultsOn(t *testing.T) {
	dir := t.TempDir()
	m1 := &Monitor{cfg: Settings{SessionSecret: "s"}, chNames: map[string]string{}}
	if err := m1.openStore(dir + "/t.db"); err != nil {
		t.Fatal(err)
	}
	// 模拟老库:存一行配置,然后删掉两列(仿佛旧版本建的库)
	if err := m1.saveAlertConfig(defaultAlertConfig()); err != nil {
		t.Fatal(err)
	}
	if err := m1.storeDB.Exec("ALTER TABLE alert_configs DROP COLUMN model_alerts_enabled").Error; err != nil {
		t.Skipf("sqlite 不支持 DROP COLUMN: %v", err)
	}
	if err := m1.storeDB.Exec("ALTER TABLE alert_configs DROP COLUMN server_alerts_enabled").Error; err != nil {
		t.Fatal(err)
	}
	// 用新代码重新打开(=升级):列新建 + 存量行补 true
	m2 := &Monitor{cfg: Settings{SessionSecret: "s"}, chNames: map[string]string{}}
	if err := m2.openStore(dir + "/t.db"); err != nil {
		t.Fatal(err)
	}
	got := m2.loadAlertConfig()
	if !got.ModelAlertsEnabled || !got.ServerAlertsEnabled {
		t.Fatalf("老库升级后开关应默认开 = %+v", got)
	}
}
