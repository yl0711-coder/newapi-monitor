package monitor

import (
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	cloudWatchLogsEastRegion = "us-east-1"
	cloudWatchLogsWestRegion = "us-west-2"

	cloudWatchLogsDefaultLimit = 100
	cloudWatchLogsHardLimit    = 500
	// 仅供后台固定前置拒绝查询使用。按需排障和 Shadow 仍保持 500；后台
	// 若也固定为 500，会在拒绝风暴中反复扫描同一小时，成本和追赶时间都放大。
	cloudWatchLogsPreRouteLimit = 10_000
	// Nginx 连续采集同样是固定后台查询。高流量窗口由调用方继续拆分，
	// 任何仍然截断的单秒窗口都会停住水位，不能把前 500 条当成完整一分钟。
	cloudWatchLogsNginxLimit = 10_000
	cloudWatchLogsMaxWindow  = 2 * time.Hour
)

type cloudWatchLogSourceID string

const (
	cwSourceCloudFrontAccess     cloudWatchLogSourceID = "cloudfront_access"
	cwSourceCloudFrontDiagnostic cloudWatchLogSourceID = "cloudfront_diagnostic"
	cwSourceWorkerNginx          cloudWatchLogSourceID = "worker_nginx"
	cwSourceWorkerNewAPI         cloudWatchLogSourceID = "worker_newapi"
	cwSourceMaster               cloudWatchLogSourceID = "master"
	cwSourceRDSError             cloudWatchLogSourceID = "rds_error"
	cwSourceRDSSlowQuery         cloudWatchLogSourceID = "rds_slowquery"
)

type cloudWatchLogSource struct {
	ID           cloudWatchLogSourceID
	Region       string
	LogGroup     string
	StreamPrefix string
	Sensitive    bool
}

var cloudWatchLogSourceCatalog = map[cloudWatchLogSourceID]cloudWatchLogSource{
	cwSourceCloudFrontAccess:     {ID: cwSourceCloudFrontAccess, Region: cloudWatchLogsEastRegion, LogGroup: "nexusapi-cloudfront-access"},
	cwSourceCloudFrontDiagnostic: {ID: cwSourceCloudFrontDiagnostic, Region: cloudWatchLogsEastRegion, LogGroup: "nexusapi-cloudfront-client-diagnostic", Sensitive: true},
	cwSourceWorkerNginx:          {ID: cwSourceWorkerNginx, Region: cloudWatchLogsWestRegion, LogGroup: "/ecs/nexusapi-prod-worker", StreamPrefix: "nginx/"},
	cwSourceWorkerNewAPI:         {ID: cwSourceWorkerNewAPI, Region: cloudWatchLogsWestRegion, LogGroup: "/ecs/nexusapi-prod-worker", StreamPrefix: "new-api/"},
	cwSourceMaster:               {ID: cwSourceMaster, Region: cloudWatchLogsWestRegion, LogGroup: "/ecs/nexusapi-prod-master"},
	cwSourceRDSError:             {ID: cwSourceRDSError, Region: cloudWatchLogsWestRegion, LogGroup: "/aws/rds/instance/nexusapi-mysql-prod/error"},
	cwSourceRDSSlowQuery:         {ID: cwSourceRDSSlowQuery, Region: cloudWatchLogsWestRegion, LogGroup: "/aws/rds/instance/nexusapi-mysql-prod/slowquery"},
}

func cloudWatchLogSourceByID(id cloudWatchLogSourceID) (cloudWatchLogSource, error) {
	source, ok := cloudWatchLogSourceCatalog[id]
	if !ok {
		return cloudWatchLogSource{}, newCloudWatchLogsError(cwLogsErrInvalid, "source", nil)
	}
	return source, nil
}

func validateCloudWatchLogRange(from, to time.Time, limit int) (int, error) {
	return validateCloudWatchLogRangeLimit(from, to, limit, cloudWatchLogsHardLimit)
}

func validateCloudWatchLogRangeLimit(from, to time.Time, limit, maximum int) (int, error) {
	if from.IsZero() || to.IsZero() || !from.Before(to) || to.Sub(from) > cloudWatchLogsMaxWindow {
		return 0, newCloudWatchLogsError(cwLogsErrInvalid, "range", nil)
	}
	if limit == 0 {
		limit = cloudWatchLogsDefaultLimit
	}
	if limit < 1 || limit > maximum {
		return 0, newCloudWatchLogsError(cwLogsErrInvalid, "limit", nil)
	}
	return limit, nil
}

func validateCloudWatchQueryValue(value string, maxBytes int) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxBytes || !utf8.ValidString(value) {
		return "", newCloudWatchLogsError(cwLogsErrInvalid, "query_value", nil)
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return "", newCloudWatchLogsError(cwLogsErrInvalid, "query_value", nil)
		}
	}
	return value, nil
}

func validateCloudWatchLogsSettings(s Settings) error {
	if !s.CloudWatchLogsEnabled {
		return nil
	}
	if s.LocalSnapshotOnly {
		return errors.New("MONITOR_CLOUDWATCH_LOGS_ENABLED 不能在本地快照只读模式开启")
	}
	// fail closed：密钥不合规时宁可不启动，也不能退化成回传原始 Request ID/IP。
	// 强度与既有 Nginx 证据密钥一致（见 validateNginxSettings）。
	if len(s.CloudWatchEvidenceHMACKey) < 32 || !cwValidKeyID(s.CloudWatchEvidenceHMACKeyID) {
		return errors.New("开启 MONITOR_CLOUDWATCH_LOGS_ENABLED 必须配置至少 32 字节的 MONITOR_CLOUDWATCH_EVIDENCE_HMAC_KEY 与合法的 MONITOR_CLOUDWATCH_EVIDENCE_HMAC_KEY_ID")
	}
	if s.CloudWatchEvidenceHMACKey == s.SessionSecret || s.CloudWatchEvidenceHMACKey == s.IngestToken ||
		s.CloudWatchEvidenceHMACKey == s.UpstreamCredentialSecret {
		return errors.New("MONITOR_CLOUDWATCH_EVIDENCE_HMAC_KEY 不能复用会话密钥、采集令牌或上游凭据密钥")
	}
	return nil
}

func validateCloudWatchShadowSettings(s Settings) error {
	if !s.CloudWatchShadowEnabled {
		return nil
	}
	if !s.CloudWatchLogsEnabled {
		return errors.New("MONITOR_CLOUDWATCH_SHADOW_ENABLED 必须同时开启 MONITOR_CLOUDWATCH_LOGS_ENABLED")
	}
	if s.LocalSnapshotOnly {
		return errors.New("MONITOR_CLOUDWATCH_SHADOW_ENABLED 不能在本地快照只读模式开启")
	}
	if !s.CloudWatchShadowNginxContractReady {
		return errors.New("MONITOR_CLOUDWATCH_SHADOW_ENABLED 必须先显式确认 Nginx access 结构化日志投递已验收")
	}
	if s.CloudWatchShadowIntervalMinutes < 5 || s.CloudWatchShadowIntervalMinutes > 60 {
		return errors.New("MONITOR_CLOUDWATCH_SHADOW_INTERVAL_MINUTES 必须在 5～60 之间")
	}
	if s.CloudWatchShadowLookbackMinutes < 1 || s.CloudWatchShadowLookbackMinutes > 120 {
		return errors.New("MONITOR_CLOUDWATCH_SHADOW_LOOKBACK_MINUTES 必须在 1～120 之间")
	}
	if s.CloudWatchShadowRetentionDays < 7 || s.CloudWatchShadowRetentionDays > 31 {
		return errors.New("MONITOR_CLOUDWATCH_SHADOW_RETENTION_DAYS 必须在 7～31 之间")
	}
	return nil
}

// cloudWatchEvidenceAvailable 只回答"按需证据入口是否可用"，不触发任何 AWS 调用。
func (m *Monitor) cloudWatchEvidenceAvailable() bool {
	return m != nil && m.cloudWatchLogs != nil && m.cloudWatchLogs.enabled
}

// cloudWatchEvidenceParser 构造脱敏解析器。allowSensitive 只由管理端显式勾选
// 控制；解析层对敏感来源仍会二次 fail closed（见 parse 的 sensitive 分支）。
func (m *Monitor) cloudWatchEvidenceParser(allowSensitive bool) (*cloudWatchEvidenceParser, error) {
	return newCloudWatchEvidenceParser(m.cfg.CloudWatchEvidenceHMACKey, m.cfg.CloudWatchEvidenceHMACKeyID, allowSensitive)
}
