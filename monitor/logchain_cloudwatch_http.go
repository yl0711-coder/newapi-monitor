package monitor

// logchain_cloudwatch_http.go：按需证据查询的 HTTP 边界。
//
// ★ 用 POST 而不是 GET ★
// Request ID 属敏感标识。放进 GET query 会进浏览器历史、代理日志和
// Nginx access log —— 排障工具本身不该制造新的敏感数据副本。
//
// ★ 前端不得提交日志组、区域、日志流前缀或查询语句 ★
// 请求体是闭集：只有 Request ID、时间点和一个敏感开关。日志组与区域
// 由后端从白名单反查，前端连字段都没有。

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// logChainCloudWatchLimit 是单来源单次事件上限。
// 一条请求在一个来源里通常只有个位数事件；给 50 是为容纳重试与多行栈，
// 同时远低于阶段 1 的硬上限，避免一次点击翻很多页。
const logChainCloudWatchLimit = 50

// logChainCloudWatchMaxSkew 限制 at_unix 相对当前时间的偏移。
// 上界给 5 分钟容忍客户端时钟快；下界按 CloudFront 保留期（30 天）给，
// 超出保留期的窗口查了也只能是空，白花一次调用。
const (
	logChainCloudWatchMaxFuture = 5 * time.Minute
	logChainCloudWatchMaxPast   = 30 * 24 * time.Hour
)

type logChainCloudWatchRequest struct {
	RequestID string `json:"request_id"`
	AtUnix    int64  `json:"at_unix"`
	// IncludeSensitive 请求读取 CloudFront 客户诊断日志（含 IP、User-Agent）。
	// 当前单人管理部署不再区分普通/超级管理员；路由仍要求管理端登录，
	// 返回值仍只包含脱敏结构化字段与 HMAC，不回传原始 IP/User-Agent。
	IncludeSensitive bool `json:"include_sensitive"`
}

// serveLogChainCloudWatchEvidence POST /logchain/cloudwatch/evidence
func (m *Monitor) serveLogChainCloudWatchEvidence(c *gin.Context) {
	if m.cloudWatchLogs == nil || !m.cloudWatchLogs.enabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "cloudwatch logs disabled", "enabled": false})
		return
	}
	var in logChainCloudWatchRequest
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid cloudwatch evidence payload"})
		return
	}
	requestID, at, err := parseLogChainCloudWatchInput(in, time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// 本系统由单一管理员自用；路由外层已要求管理员登录，敏感诊断不再额外
	// 区分普通管理员与超级管理员。解析层仍只回传脱敏摘要与 HMAC。
	allowSensitive := m.allowsSensitiveCloudWatchDiagnostics(c)
	result := m.lookupLogChainCloudWatchEvidence(c.Request.Context(), requestID, at, in.IncludeSensitive, allowSensitive)
	c.JSON(http.StatusOK, result)
}

// allowsSensitiveCloudWatchDiagnostics 判断本次请求能否读取客户网络诊断日志。
// 路由已经位于 requireRole(roleAdmin) 组内；当前部署只有系统所有者使用，
// 因此不再制造第二套普通/超级管理员分支。LocalAuthBypass 也用于本机验收，
// 与登录态一样只会拿到脱敏结构化字段，不会返回原始 IP/User-Agent。
func (m *Monitor) allowsSensitiveCloudWatchDiagnostics(c *gin.Context) bool {
	return c != nil
}

// parseLogChainCloudWatchInput 校验闭集输入。任何不合法都在发出 AWS 调用前拒绝。
func parseLogChainCloudWatchInput(in logChainCloudWatchRequest, now time.Time) (string, time.Time, error) {
	requestID := strings.TrimSpace(in.RequestID)
	if requestID == "" {
		return "", time.Time{}, errors.New("request_id 必填：按需证据只支持按精确 Request ID 查询")
	}
	if _, err := validateCloudWatchFilterToken(requestID); err != nil {
		return "", time.Time{}, errors.New("request_id 含不支持的字符")
	}
	if in.AtUnix <= 0 {
		return "", time.Time{}, errors.New("at_unix 必填：需要请求发生时间以生成窄查询窗口")
	}
	at := time.Unix(in.AtUnix, 0).UTC()
	if at.After(now.Add(logChainCloudWatchMaxFuture)) {
		return "", time.Time{}, errors.New("at_unix 不能是未来时间")
	}
	if at.Before(now.Add(-logChainCloudWatchMaxPast)) {
		return "", time.Time{}, errors.New("at_unix 超出 CloudWatch 保留范围，无法查询")
	}
	return requestID, at, nil
}
