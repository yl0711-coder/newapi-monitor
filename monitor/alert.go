package monitor

import (
	"crypto/tls"
	"fmt"
	"mime"
	"net/smtp"
	"strings"
	"time"

	"gorm.io/gorm/clause"
)

// alert.go:报警配置 + 规则评估 + 邮件发送。配置存本地库独立表(alert_config 单行 + alert_log)。
// 触发规则全部可配,默认值见 defaultAlertConfig()——为通用建议值,请按自身基线调整。

// AlertConfig 报警配置(单行,ID 固定 1)。所有阈值可在配置页调整。
type AlertConfig struct {
	ID       int    `gorm:"primaryKey" json:"-"`
	Enabled  bool   `json:"enabled"`
	SiteName string `json:"site_name"` // 站点显示名(默认取 new-api system_name,超管可改)

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

	UpdatedAt int64 `json:"-"`
}

// defaultAlertConfig 建议配置(预填到页面,用户可改)。
func defaultAlertConfig() AlertConfig {
	return AlertConfig{
		ID:                  1,
		Enabled:             false, // 配好 SMTP 前默认关闭,避免空发
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

// sendMail 发送一封邮件(支持 465 隐式 TLS 与 587/25 STARTTLS)。
func sendMail(c AlertConfig, subject, body string) error {
	to := recipientList(c.Recipients)
	if len(to) == 0 {
		return fmt.Errorf("无收件人")
	}
	from := c.SMTPFrom
	if from == "" {
		from = c.SMTPUser
	}
	addr := fmt.Sprintf("%s:%d", c.SMTPHost, c.SMTPPort)
	msg := buildMessage(from, to, subject, body)
	auth := smtp.PlainAuth("", c.SMTPUser, c.SMTPPassword, c.SMTPHost)

	if c.SMTPSSL { // 隐式 TLS(465)
		conn, err := tls.Dial("tcp", addr, &tls.Config{ServerName: c.SMTPHost})
		if err != nil {
			return fmt.Errorf("TLS 连接失败: %w", err)
		}
		cl, err := smtp.NewClient(conn, c.SMTPHost)
		if err != nil {
			return err
		}
		defer cl.Close()
		if err := cl.Auth(auth); err != nil {
			return fmt.Errorf("认证失败: %w", err)
		}
		if err := cl.Mail(from); err != nil {
			return err
		}
		for _, t := range to {
			if err := cl.Rcpt(t); err != nil {
				return err
			}
		}
		w, err := cl.Data()
		if err != nil {
			return err
		}
		if _, err := w.Write(msg); err != nil {
			return err
		}
		if err := w.Close(); err != nil {
			return err
		}
		return cl.Quit()
	}
	// 587 STARTTLS / 25:smtp.SendMail 会自动尝试 STARTTLS
	return smtp.SendMail(addr, auth, from, to, msg)
}

func buildMessage(from string, to []string, subject, body string) []byte {
	var b strings.Builder
	b.WriteString("From: " + from + "\r\n")
	b.WriteString("To: " + strings.Join(to, ", ") + "\r\n")
	// UTF-8 主题用标准库做 MIME encoded-word(=?UTF-8?B?...?=)
	b.WriteString("Subject: " + mime.BEncoding.Encode("UTF-8", subject) + "\r\n")
	b.WriteString("MIME-Version: 1.0\r\n")
	b.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	b.WriteString("\r\n")
	b.WriteString(body)
	return []byte(b.String())
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

func (m *Monitor) fire(c AlertConfig, kind, target, subject, body string, now int64) {
	if m.inCooldown(kind, target, c.CooldownMin, now) {
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
