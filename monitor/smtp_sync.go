package monitor

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// smtp_sync.go:发件邮箱默认复用主站(new-api)。SMTP 配置就存在 new-api 数据库的
// `options` 表(明文键值),监控用只读连接(只读账号)直接读出来——一次拿全、含凭证,
// 全程只读、不需要 admin token、不碰主站。前端永不回显凭证。
//
// 默认与 new-api 主站【完全一致】——主站有什么设置,这里就同步什么,含 SSL 开关
// (读主站显式的 SMTPSSLEnabled,不靠端口猜)。用户可在配置页改;改后点「使用主站配置」再同步回来。

// siteSMTP 主站(new-api)的 SMTP 设置快照。
type siteSMTP struct {
	host string
	port int
	user string
	from string
	pass string
	ssl  bool
}

// mainSiteSMTP 从 new-api options 表读取 SMTP 配置(只读)。
// 键:SMTPServer / SMTPPort / SMTPAccount / SMTPFrom / SMTPToken / SMTPSSLEnabled。
func (m *Monitor) mainSiteSMTP() (siteSMTP, error) {
	var s siteSMTP
	if m.prodDB == nil {
		return s, fmt.Errorf("生产库未连接")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	rows, err := m.prodDB.QueryContext(ctx,
		"SELECT `key`,`value` FROM options WHERE `key` IN "+
			"('SMTPServer','SMTPPort','SMTPAccount','SMTPFrom','SMTPToken','SMTPSSLEnabled')")
	if err != nil {
		return s, fmt.Errorf("读取主站 SMTP 配置失败: %w", err)
	}
	defer rows.Close()
	sslSet := false
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return s, err
		}
		switch k {
		case "SMTPServer":
			s.host = v
		case "SMTPPort":
			s.port, _ = strconv.Atoi(v)
		case "SMTPAccount":
			s.user = v
		case "SMTPFrom":
			s.from = v
		case "SMTPToken":
			s.pass = v
		case "SMTPSSLEnabled":
			s.ssl = v == "true" || v == "1" // new-api 显式 SSL 开关
			sslSet = true
		}
	}
	if err := rows.Err(); err != nil {
		return s, err
	}
	if s.from == "" {
		s.from = s.user
	}
	if !sslSet {
		s.ssl = s.port == 465 // 主站未显式设置时兜底:465=隐式 TLS
	}
	return s, nil
}

// syncSMTPFromMainSite 把主站 SMTP 配置(含 SSL 开关、凭证)同步进监控本地配置并保存。
// 凭证一并同步,但不返回给前端。
func (m *Monitor) syncSMTPFromMainSite() (AlertConfig, error) {
	s, err := m.mainSiteSMTP()
	if err != nil {
		return AlertConfig{}, err
	}
	if s.host == "" {
		return AlertConfig{}, fmt.Errorf("主站未配置 SMTP 服务器")
	}
	c := m.loadAlertConfig()
	c.SMTPHost = s.host
	c.SMTPPort = s.port
	c.SMTPUser = s.user
	c.SMTPFrom = s.from
	c.SMTPSSL = s.ssl || s.port == 465 // 465 一律隐式 TLS(new-api 见 465 也强制 TLS、不看它自己的开关,镜像其真实行为而非存的标志)
	if s.pass != "" {
		c.SMTPPassword = s.pass
	}
	if err := m.saveAlertConfig(c); err != nil {
		return AlertConfig{}, err
	}
	return c, nil
}

// ensureSMTPDefault 首次/未配置 SMTP 时,默认从主站同步一次(尽力而为,失败不阻塞)。
func (m *Monitor) ensureSMTPDefault() {
	if m.loadAlertConfig().SMTPHost != "" {
		return // 已配置过(无论是同步来的还是手填的),不覆盖
	}
	_, _ = m.syncSMTPFromMainSite()
}
