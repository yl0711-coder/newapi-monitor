package monitor

import (
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/wneessen/go-mail"
	"gorm.io/gorm/clause"
)

// alert.go:报警配置 + 规则评估 + 邮件发送。配置存本地库独立表(alert_config 单行 + alert_log)。
// 触发规则全部可配,默认值见 defaultAlertConfig()——为通用建议值,请按自身基线调整。

// AlertConfig 报警配置(单行,ID 固定 1)。所有阈值可在配置页调整。
type AlertConfig struct {
	ID       int    `gorm:"primaryKey" json:"-"`
	Enabled  bool   `json:"enabled"`
	SiteName string `json:"site_name"` // 站点显示名(默认取 new-api system_name,超管可改)

	// 分类邮件开关:模型监控 / 服务端监控 两栏目各自独立(用户用量无邮件报警,不设开关)。
	// 关=该栏目命中规则时【不发邮件】,页面「最近告警」仍记录;老库经 AutoMigrate 补列默认开,行为不变。
	ModelAlertsEnabled  bool `gorm:"default:true" json:"model_alerts_enabled"`
	ServerAlertsEnabled bool `gorm:"default:true" json:"server_alerts_enabled"`

	// 发件 SMTP
	SMTPHost     string `json:"smtp_host"`
	SMTPPort     int    `json:"smtp_port"`
	SMTPUser     string `json:"smtp_user"`
	SMTPPassword string `json:"smtp_password"` // 存库,GET 时不回显
	SMTPFrom     string `json:"smtp_from"`
	SMTPSSL      bool   `json:"smtp_ssl"`   // 465 隐式 TLS=true;587 STARTTLS=false
	Recipients   string `json:"recipients"` // 收件人,逗号/换行分隔(支持多个)

	// 触发阈值(全部可配)
	EvalWindowMin       int     `json:"eval_window_min"`
	ErrRatePct          float64 `json:"err_rate_pct"`
	ErrMinCount         int     `json:"err_min_count"`
	ErrBurstCount       int     `json:"err_burst_count"`
	AnomalyBurstBuckets int     `json:"anomaly_burst_buckets"`
	AnomalyMinCount     int     `json:"anomaly_min_count"`
	SamplerDownEnabled  bool    `json:"sampler_down_enabled"`
	CooldownMin         int     `json:"cooldown_min"`

	// SLO / 错误预算 / 燃烧告警(SLI = 非错误率,全部可配)
	SLOEnabled        bool    `json:"slo_enabled"`
	SLOTargetPct      float64 `json:"slo_target_pct"`  // 目标成功率,如 99
	SLOWindowDays     int     `json:"slo_window_days"` // SLO 窗口(天,≤ 留存天数)
	BurnFastEnabled   bool    `json:"burn_fast_enabled"`
	BurnFastRate      float64 `json:"burn_fast_rate"`       // 快烧倍数阈值,如 14
	BurnFastWindowMin int     `json:"burn_fast_window_min"` // 快烧观察窗(分钟),如 60
	BurnSlowEnabled   bool    `json:"burn_slow_enabled"`
	BurnSlowRate      float64 `json:"burn_slow_rate"`       // 慢烧倍数阈值,如 3
	BurnSlowWindowMin int     `json:"burn_slow_window_min"` // 慢烧观察窗(分钟),如 360

	// 被拒请求面板(前置拒绝统计):开启后内部监控页显示「被拒请求」面板。
	// 数据需在各 new-api 节点安装采集器 newapi-reject-collector 才有;默认关。
	RejectPanelEnabled bool `json:"reject_panel_enabled"`

	UpdatedAt int64 `json:"-"`
}

// defaultAlertConfig 建议配置(预填到页面,用户可改)。
func defaultAlertConfig() AlertConfig {
	return AlertConfig{
		ID:                  1,
		Enabled:             false, // 配好 SMTP 前默认关闭,避免空发
		ModelAlertsEnabled:  true,
		ServerAlertsEnabled: true,
		SMTPPort:            465,
		SMTPSSL:             true,
		EvalWindowMin:       15,
		ErrRatePct:          20,
		ErrMinCount:         5,
		ErrBurstCount:       10,
		AnomalyBurstBuckets: 3,
		AnomalyMinCount:     8,
		SamplerDownEnabled:  true,
		CooldownMin:         30,
		SLOEnabled:          false, // 配好目标后再开
		SLOTargetPct:        99,
		SLOWindowDays:       7,
		BurnFastEnabled:     true,
		BurnFastRate:        14,
		BurnFastWindowMin:   60,
		BurnSlowEnabled:     true,
		BurnSlowRate:        3,
		BurnSlowWindowMin:   360,
	}
}

// AlertLog 已发报警记录(冷却判断 + 审计)。
type AlertLog struct {
	ID     int64  `gorm:"primaryKey"`
	Ts     int64  `gorm:"index"`
	Kind   string // error_rate / error_burst / anomaly_burst / sampler_down
	Target string // 渠道/模型标识;sampler_down 为空
	Detail string
}

func (m *Monitor) loadAlertConfig() AlertConfig {
	var c AlertConfig
	if err := m.storeDB.First(&c, 1).Error; err != nil {
		return defaultAlertConfig() // 没存过 → 返回建议默认
	}
	return c
}

func (m *Monitor) saveAlertConfig(c AlertConfig) error {
	c.ID = 1
	c.UpdatedAt = time.Now().Unix()
	return m.storeDB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		UpdateAll: true,
	}).Create(&c).Error
}

// inCooldown 判断 (kind,target) 是否在冷却期内(冷却内不重发)。
func (m *Monitor) inCooldown(kind, target string, cooldownMin int, now int64) bool {
	var cnt int64
	m.storeDB.Model(&AlertLog{}).
		Where("kind = ? AND target = ? AND ts > ?", kind, target, now-int64(cooldownMin)*60).
		Count(&cnt)
	return cnt > 0
}

func (m *Monitor) logAlert(kind, target, detail string, now int64) {
	m.storeDB.Create(&AlertLog{Ts: now, Kind: kind, Target: target, Detail: detail})
}

func recipientList(s string) []string {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == '\n' || r == ';' || r == ' ' })
	var out []string
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// sendMail 用 go-mail 发送一封 HTML 报警邮件(465 隐式 TLS 或 587 STARTTLS)。
func sendMail(c AlertConfig, subject, body string) error {
	to := recipientList(c.Recipients)
	if len(to) == 0 {
		return fmt.Errorf("无收件人")
	}
	from := c.SMTPFrom
	if from == "" {
		from = c.SMTPUser
	}
	msg := mail.NewMsg()
	if err := msg.From(from); err != nil {
		return fmt.Errorf("发件人无效: %w", err)
	}
	if err := msg.To(to...); err != nil {
		return fmt.Errorf("收件人无效: %w", err)
	}
	msg.Subject(subject)
	msg.SetBodyString(mail.TypeTextHTML, htmlWrap(subject, body))

	opts := []mail.Option{
		mail.WithPort(c.SMTPPort),
		mail.WithSMTPAuth(mail.SMTPAuthPlain),
		mail.WithUsername(c.SMTPUser),
		mail.WithPassword(c.SMTPPassword),
		mail.WithTimeout(15 * time.Second),
	}
	if c.SMTPSSL || c.SMTPPort == 465 {
		// 465 一律隐式 TLS(与 new-api 行为一致:见 465 强制 TLS,不看开关)。
		// 教训:2026-07 曾因"镜像主站没勾的 SSL 开关"在 465 上走 STARTTLS,对 Resend 干等超时。
		opts = append(opts, mail.WithSSL())
	} else {
		opts = append(opts, mail.WithTLSPolicy(mail.TLSMandatory)) // 587 STARTTLS
	}
	cl, err := mail.NewClient(c.SMTPHost, opts...)
	if err != nil {
		return fmt.Errorf("创建邮件客户端失败: %w", err)
	}
	return cl.DialAndSend(msg)
}

// htmlWrap 把纯文本报警内容包成一封简洁好看的 HTML 邮件(深色卡片 + 红色顶边)。
func htmlWrap(subject, body string) string {
	b := strings.ReplaceAll(html.EscapeString(body), "\n", "<br>")
	return fmt.Sprintf(`<div style="margin:0;padding:24px;background:#0f1117;font-family:-apple-system,'Segoe UI',Roboto,sans-serif">
  <div style="max-width:560px;margin:0 auto;background:#1a1d27;border:1px solid #2a2d3e;border-top:4px solid #ef4444;border-radius:12px;overflow:hidden">
    <div style="padding:18px 22px;font-size:17px;font-weight:700;color:#e2e8f0">%s</div>
    <div style="padding:4px 22px 18px;font-size:14px;line-height:1.7;color:#cbd5e1">%s</div>
    <div style="padding:12px 22px;border-top:1px solid #2a2d3e;font-size:12px;color:#94a3b8">new-api 上游监控 · 自动报警(阈值可在「报警设置」页调整)</div>
  </div>
</div>`, html.EscapeString(subject), b)
}

// evaluateAlerts 每个采样周期调用:按配置评估规则,命中且过冷却则发邮件。
func (m *Monitor) evaluateAlerts(nowUnix int64) {
	c := m.loadAlertConfig()
	if !c.Enabled || c.SMTPHost == "" || c.Recipients == "" {
		return
	}

	m.evaluateBurn(c, nowUnix) // SLO 错误预算燃烧告警(快烧/慢烧)

	// 采样器掉线
	if c.SamplerDownEnabled {
		if m.LastSampleRun() > 0 && m.LastSampleRun() < nowUnix-int64(m.cfg.SampleSeconds)*3 {
			m.fire(c, "sampler_down", "", "采样器掉线", "监控采样器超过 3 个周期未成功运行,可能已停止或连不上数据库。", nowUnix)
		}
	}

	snap, err := m.GetSnapshot(c.EvalWindowMin, nowUnix)
	if err != nil {
		return
	}
	rows := append(append([]Row{}, snap.ByChannel...), snap.ByModel...)
	for _, r := range rows {
		// 错误·突发(优先,阈值更明确)
		if c.ErrBurstCount > 0 && r.Failed >= int64(c.ErrBurstCount) {
			m.fire(c, "error_burst", r.Label,
				fmt.Sprintf("错误突发:%s", r.Label),
				alertBody(r, c, fmt.Sprintf("近%d分钟错误 %d 条(突发阈值 %d)", c.EvalWindowMin, r.Failed, c.ErrBurstCount)), nowUnix)
			continue
		}
		// 错误·渠道异常(错误率 + 最小样本)
		if c.ErrRatePct > 0 && r.ErrorRate >= c.ErrRatePct && r.Failed >= int64(c.ErrMinCount) {
			m.fire(c, "error_rate", r.Label,
				fmt.Sprintf("错误率告警:%s", r.Label),
				alertBody(r, c, fmt.Sprintf("近%d分钟错误率 %.1f%%(阈值 %.0f%%)、错误 %d/%d", c.EvalWindowMin, r.ErrorRate, c.ErrRatePct, r.Failed, r.Total)), nowUnix)
			continue
		}
		// 异常成簇
		if anomalyBurst(r.Spark, c.AnomalyBurstBuckets) && r.Anomaly >= int64(c.AnomalyMinCount) {
			m.fire(c, "anomaly_burst", r.Label,
				fmt.Sprintf("异常成簇:%s", r.Label),
				alertBody(r, c, fmt.Sprintf("近%d分钟客户端断开成簇 %d 次(连续≥%d桶),多为上游变慢导致放弃", c.EvalWindowMin, r.Anomaly, c.AnomalyBurstBuckets)), nowUnix)
		}
	}
}

// alertCategory 按 kind 归两栏目:infra_*=服务端,其余(error_*/anomaly_*/sampler_down/burn_*)=模型。
func alertCategory(kind string) string {
	if strings.HasPrefix(kind, "infra_") {
		return "server"
	}
	return "model"
}

// categoryEmailEnabled 该 kind 所属栏目的邮件开关是否打开。
func categoryEmailEnabled(c AlertConfig, kind string) bool {
	if alertCategory(kind) == "server" {
		return c.ServerAlertsEnabled
	}
	return c.ModelAlertsEnabled
}

func (m *Monitor) fire(c AlertConfig, kind, target, subject, body string, now int64) {
	if m.inCooldown(kind, target, c.CooldownMin, now) {
		return
	}
	if !categoryEmailEnabled(c, kind) { // 栏目邮件开关关:不发邮件,仍记入「最近告警」供页面查看
		m.logAlert(kind, target, subject+"(未发邮件:该栏目报警邮件已关闭)", now)
		return
	}
	if err := sendMail(c, "[new-api监控] "+subject, body); err != nil {
		// 发送失败也记一条,避免反复重试刷屏(并暴露问题)
		m.logAlert(kind+"_FAILED", target, err.Error(), now)
		return
	}
	m.logAlert(kind, target, subject, now)
}

func alertBody(r Row, c AlertConfig, head string) string {
	em := []string{}
	if r.Err5xx > 0 {
		em = append(em, fmt.Sprintf("5xx %d", r.Err5xx))
	}
	if r.ErrTimeout > 0 {
		em = append(em, fmt.Sprintf("超时 %d", r.ErrTimeout))
	}
	if r.Err4xx > 0 {
		em = append(em, fmt.Sprintf("4xx %d", r.Err4xx))
	}
	if r.ErrOther > 0 {
		em = append(em, fmt.Sprintf("其它 %d", r.ErrOther))
	}
	return fmt.Sprintf(`%s

对象:%s
窗口:近 %d 分钟
成功率:%.1f%%  |  请求:%d  成功:%d  异常:%d  错误:%d
错误构成:%s
延迟 p95:%.0fs  最大:%ds  首字p95:%.1fs

(new-api 上游监控自动报警;阈值可在监控"报警设置"页调整)`,
		head, r.Label, c.EvalWindowMin, r.SuccessRate, r.Total, r.Success, r.Anomaly, r.Failed,
		strings.Join(em, " / "), r.P95, r.MaxLatency, r.TtftP95)
}
