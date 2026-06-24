package monitor

import (
	"strings"
	"time"
)

// infra_alerts.go:服务端监控的「最近告警」读取(复用既有 alert_log 表)。

// recentInfraAlerts 取最近 N 条【服务端监控】告警(kind 前缀 infra_),按时间倒序。
// 复用既有 alert_log 表(fire 触发时已写入);_FAILED 记为 warn,其余记 bad。
func (m *Monitor) recentInfraAlerts(nowUnix int64, limit int) []InfraAlert {
	if limit <= 0 {
		limit = 20
	}
	var logs []AlertLog
	m.storeDB.Where("kind LIKE ?", "infra\\_%").Order("ts DESC").Limit(limit).Find(&logs)
	out := make([]InfraAlert, 0, len(logs))
	for _, l := range logs {
		st := "bad"
		kind := l.Kind
		if strings.HasSuffix(kind, "_FAILED") {
			st = "warn"
			kind = strings.TrimSuffix(kind, "_FAILED")
		}
		out = append(out, InfraAlert{
			Ts:     l.Ts,
			When:   time.Unix(l.Ts, 0).Format("01-02 15:04"),
			Kind:   kind,
			Target: l.Target,
			Detail: l.Detail,
			Status: st,
		})
	}
	return out
}
