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
	// 关=该栏目命中规则时【不发邮件】,页面「最近告警」仍记录。
	// ⚠️ 不能加 gorm:"default:true":布尔零值(false)会被 gorm 当"未设置"而不写列,被库默认 true 顶回——
	// 曾导致"勾掉保存又自动勾上"的 bug。老库升级的默认开由 loadAlertConfig 的 migrated 判断兜底。
	ModelAlertsEnabled  bool `json:"model_alerts_enabled"`
	ServerAlertsEnabled bool `json:"server_alerts_enabled"`
	// 渠道余额预警独立开关。它只读取 Monitor 本地小时汇总和本地保存的余额，
	// 不会因为报警评估而查询 NewAPI 生产库或上游面板。
	UpstreamBalanceAlertsEnabled bool    `json:"upstream_balance_alerts_enabled"`
	UpstreamBalanceRunwayDays    float64 `json:"upstream_balance_runway_days"`
	UpstreamBalanceLookbackDays  int     `json:"upstream_balance_lookback_days"`
	UpstreamBalanceMinCoverage   float64 `json:"upstream_balance_min_coverage_pct"`
	UpstreamBalanceCooldownMin   int     `json:"upstream_balance_cooldown_min"`

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

	// 交付异常(B 类)告警:与错误告警【完全分开控制】。
	// 分开的理由:两者要人做的事不同——错误要立刻止损/联系上游;交付异常是观察类,
	// 动作是攒证据找上游或人工调权重,不需要 30 分钟就催一次。
	// 冷却、阈值、邮件开关都独立;栏目见 alertCategory 的 "anomaly"。
	AnomalyAlertsEnabled bool    `json:"anomaly_alerts_enabled"` // 交付异常邮件总开关(独立于 ModelAlertsEnabled)
	AnomalyRatePct       float64 `json:"anomaly_rate_pct"`       // 交付异常率阈值(%),0=不启用该规则
	AnomalyCooldownMin   int     `json:"anomaly_cooldown_min"`   // 交付异常专用冷却(分钟)
	AnomalyBilledUSD     float64 `json:"anomaly_billed_usd"`     // 单窗口 B1 已扣费超此金额即报(钱不等成簇),0=不启用

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
		ID:                           1,
		Enabled:                      false, // 配好 SMTP 前默认关闭,避免空发
		ModelAlertsEnabled:           true,
		ServerAlertsEnabled:          true,
		UpstreamBalanceAlertsEnabled: true,
		UpstreamBalanceRunwayDays:    1,
		UpstreamBalanceLookbackDays:  7,
		UpstreamBalanceMinCoverage:   95,
		UpstreamBalanceCooldownMin:   720,
		SMTPPort:                     465,
		SMTPSSL:                      true,
		EvalWindowMin:                15,
		ErrRatePct:                   20,
		ErrMinCount:                  5,
		ErrBurstCount:                10,
		AnomalyBurstBuckets:          3,
		// 阈值按真实数据标定:7 天内按 15 分钟窗口(渠道×模型)切,有交付异常的窗口 130 个,
		// 其中 ≥5 条的仅 21 个(约每天 3 次)。故 8% / ≥5 条不会造成告警洪水。
		// 旧默认 AnomalyMinCount=8 是按旧口径(含 219 条误报)定的,新口径下改 5。
		AnomalyMinCount:      5,
		SamplerDownEnabled:   true,
		CooldownMin:          30,
		AnomalyAlertsEnabled: true,
		AnomalyRatePct:       8,
		AnomalyCooldownMin:   60, // 观察类,不需要和错误一样 30 分钟就催
		AnomalyBilledUSD:     5,
		SLOEnabled:           false, // 配好目标后再开
		SLOTargetPct:         99,
		SLOWindowDays:        7,
		BurnFastEnabled:      true,
		BurnFastRate:         14,
		BurnFastWindowMin:    60,
		BurnSlowEnabled:      true,
		BurnSlowRate:         3,
		BurnSlowWindowMin:    360,
	}
}

// AlertLog 已发报警记录(冷却判断 + 审计)。
type AlertLog struct {
	ID     int64  `gorm:"primaryKey"`
	Ts     int64  `gorm:"index"`
	Kind   string // error_rate / error_burst / anomaly_rate / anomaly_billed / anomaly_burst / sampler_down / infra_* / burn_*
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
	policy := upstreamBalancePolicyFor(c)
	c.UpstreamBalanceRunwayDays = policy.RunwayDays
	c.UpstreamBalanceLookbackDays = policy.Lookback
	c.UpstreamBalanceMinCoverage = policy.MinCoverage
	if c.UpstreamBalanceCooldownMin < 60 || c.UpstreamBalanceCooldownMin > 10080 {
		c.UpstreamBalanceCooldownMin = defaultAlertConfig().UpstreamBalanceCooldownMin
	}
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
		Where("kind IN ? AND target = ? AND ts > ?", []string{kind, kind + "_FAILED"}, target, now-int64(cooldownMin)*60).
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
	if m.cfg.AlertsDisabled { // 断路器:本地/测试实例一封都不发,不依赖库里的配置
		return
	}
	c := m.loadAlertConfig()
	if !c.Enabled || c.SMTPHost == "" || c.Recipients == "" {
		return
	}

	m.evaluateBurn(c, nowUnix) // SLO 错误预算燃烧告警(快烧/慢烧)
	m.evaluateUpstreamBalanceAlerts(c, nowUnix)

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
		// ---- 交付异常(B 类)走 anomaly 栏目:独立开关 + 独立冷却 ----
		// 选规则与发信分开:anomalyRuleFor 只返回一个 kind,所以"一行最多一封"
		// 是结构保证,不再依赖每条规则后面记得写 continue。
		switch kind := anomalyRuleFor(c, r); kind {
		case "anomaly_billed":
			// B1:上游收了钱没给东西。钱的事不等成簇、不看占比。
			m.fire(c, kind, r.Label,
				fmt.Sprintf("交付异常·已扣费:%s", r.Label),
				alertBody(r, c, fmt.Sprintf("近%d分钟有 %d 次请求已扣费但零输出,合计 $%.2f(阈值 $%.2f);用户平均白等 %.0f 秒",
					c.EvalWindowMin, r.AnomalyBilled, r.AnomalyCostUSD, c.AnomalyBilledUSD, r.AnomalyAvgWait)), nowUnix)
		case "anomaly_rate":
			// 持续性的交付质量下降,供人工判断降权。
			m.fire(c, kind, r.Label,
				fmt.Sprintf("交付异常率告警:%s", r.Label),
				alertBody(r, c, fmt.Sprintf("近%d分钟交付异常率 %.1f%%(阈值 %.0f%%)、%d/%d 次用户没拿到内容;其中已扣费 %d 次、未扣费 %d 次,平均白等 %.0f 秒",
					c.EvalWindowMin, r.AnomalyRate, c.AnomalyRatePct, r.Anomaly, r.Total,
					r.AnomalyBilled, r.AnomalyFree, r.AnomalyAvgWait)), nowUnix)
		case "anomaly_burst":
			// 量不大但连续多桶出现,形态上是持续故障而非抖动。
			m.fire(c, kind, r.Label,
				fmt.Sprintf("交付异常成簇:%s", r.Label),
				alertBody(r, c, fmt.Sprintf("近%d分钟交付异常成簇 %d 次(连续≥%d桶),用户平均白等 %.0f 秒,多为上游静默断流",
					c.EvalWindowMin, r.Anomaly, c.AnomalyBurstBuckets, r.AnomalyAvgWait)), nowUnix)
		}
	}
}

// anomalyRuleFor 返回该行应触发的交付异常规则(""=不触发)。
// 优先级即返回顺序:钱 > 占比 > 成簇。
//   - 钱(B1 已扣费)不看占比也不等成簇:扣了钱没交付,一次就该知道。
//   - 占比与成簇都要过 AnomalyMinCount,避免小样本抖动刷屏。
func anomalyRuleFor(c AlertConfig, r Row) string {
	if c.AnomalyBilledUSD > 0 && r.AnomalyCostUSD >= c.AnomalyBilledUSD {
		return "anomaly_billed"
	}
	if r.Anomaly < int64(c.AnomalyMinCount) {
		return ""
	}
	if c.AnomalyRatePct > 0 && r.AnomalyRate >= c.AnomalyRatePct {
		return "anomaly_rate"
	}
	if anomalyBurst(r.Spark, c.AnomalyBurstBuckets) {
		return "anomaly_burst"
	}
	return ""
}

// alertCategory 按 kind 归四栏目:infra_*=服务端、anomaly_*=交付异常、
// upstream_balance*=渠道余额，其余=模型(错误/采样器/燃烧)。
// 交付异常单列一栏是刻意的:它必须能和错误告警分开开关、分开冷却。
// 若并回 "model",就会被 ModelAlertsEnabled 一起管——那正是改造前的问题。
func alertCategory(kind string) string {
	switch {
	case strings.HasPrefix(kind, "infra_"):
		return "server"
	case strings.HasPrefix(kind, "anomaly_"):
		return "anomaly"
	case strings.HasPrefix(kind, "upstream_balance"):
		return "upstream"
	default:
		return "model"
	}
}

// categoryEmailEnabled 该 kind 所属栏目的邮件开关是否打开。
func categoryEmailEnabled(c AlertConfig, kind string) bool {
	switch alertCategory(kind) {
	case "server":
		return c.ServerAlertsEnabled
	case "anomaly":
		return c.AnomalyAlertsEnabled
	case "upstream":
		return c.UpstreamBalanceAlertsEnabled
	default:
		return c.ModelAlertsEnabled
	}
}

// categoryCooldownMin 返回该 kind 的独立冷却；渠道余额、交付异常各自配置，
// 其他规则沿用通用冷却。
func categoryCooldownMin(c AlertConfig, kind string) int {
	if alertCategory(kind) == "upstream" && c.UpstreamBalanceCooldownMin > 0 {
		return c.UpstreamBalanceCooldownMin
	}
	if alertCategory(kind) == "anomaly" && c.AnomalyCooldownMin > 0 {
		return c.AnomalyCooldownMin
	}
	return c.CooldownMin
}

func (m *Monitor) fire(c AlertConfig, kind, target, subject, body string, now int64) {
	if m.inCooldown(kind, target, categoryCooldownMin(c, kind), now) {
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
