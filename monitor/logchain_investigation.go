package monitor

// logchain_investigation.go：阶段三的异步排障任务、短缓存与追加式审计。
//
// 任务结果只保存在进程内；CloudWatch 仍是原始日志权威源。本地库只追加
// 不含原始 Request ID、IP、User-Agent 和日志正文的审计摘要。

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	logChainInvestigationMaxWindow = 2 * time.Hour
	logChainInvestigationTimeout   = 30 * time.Second
	logChainInvestigationExactTTL  = 10 * time.Minute
	logChainInvestigationWindowTTL = 5 * time.Minute
	logChainInvestigationKeep      = 2 * time.Hour
	logChainInvestigationMaxTasks  = 256
)

type logChainInvestigationCreateRequest struct {
	From                        string `json:"from"`
	To                          string `json:"to"`
	Timezone                    string `json:"timezone"`
	AtUnix                      int64  `json:"at_unix"`
	UserID                      int64  `json:"user_id"`
	NewAPIRequestID             string `json:"newapi_request_id"`
	CloudFrontRequestID         string `json:"cloudfront_request_id"`
	Model                       string `json:"model"`
	Group                       string `json:"group"`
	Path                        string `json:"path"`
	Status                      int    `json:"status"`
	ClientIP                    string `json:"client_ip"`
	IncludeSensitiveDiagnostics bool   `json:"include_sensitive_diagnostics"`
	Purpose                     string `json:"purpose"`
}

type logChainInvestigationInput struct {
	From                        time.Time
	To                          time.Time
	Timezone                    string
	UserID                      int64
	NewAPIRequestID             string
	CloudFrontRequestID         string
	Model                       string
	Group                       string
	Path                        string
	Status                      int
	ClientIP                    string
	IncludeSensitiveDiagnostics bool
	Purpose                     string
}

type logChainInvestigationScopeView struct {
	FromUTC              string `json:"from_utc"`
	ToUTC                string `json:"to_utc"`
	Timezone             string `json:"timezone"`
	UserID               int64  `json:"user_id,omitempty"`
	NewAPIRequestRef     string `json:"newapi_request_ref,omitempty"`
	CloudFrontRef        string `json:"cloudfront_request_ref,omitempty"`
	ClientIPRef          string `json:"client_ip_ref,omitempty"`
	Model                string `json:"model,omitempty"`
	Group                string `json:"group,omitempty"`
	Path                 string `json:"path,omitempty"`
	Status               int    `json:"status,omitempty"`
	SensitiveDiagnostics bool   `json:"sensitive_diagnostics"`
}

type logChainInvestigationSummary struct {
	Classification string `json:"classification"`
	EvidenceLevel  string `json:"evidence_level"`
	Conclusion     string `json:"conclusion"`
	CustomerImpact string `json:"customer_impact"`
}

type logChainInvestigationTimelineEvent struct {
	EventMS       int64                 `json:"event_ms"`
	Node          string                `json:"node"`
	Source        cloudWatchLogSourceID `json:"source"`
	Fact          string                `json:"fact"`
	EvidenceRef   string                `json:"evidence_ref"`
	RequestRef    string                `json:"request_ref,omitempty"`
	EvidenceLevel string                `json:"evidence_level"`
	Complete      bool                  `json:"complete"`
}

type logChainInvestigationCost struct {
	Queries      int    `json:"queries"`
	BytesScanned uint64 `json:"bytes_scanned"`
	CacheHit     bool   `json:"cache_hit"`
}

type logChainInvestigationResult struct {
	OK                       bool                                 `json:"ok"`
	InvestigationID          string                               `json:"investigation_id"`
	Status                   string                               `json:"status"`
	Scope                    logChainInvestigationScopeView       `json:"scope"`
	Summary                  logChainInvestigationSummary         `json:"summary"`
	Requests                 []LogChainRow                        `json:"requests"`
	Timeline                 []logChainInvestigationTimelineEvent `json:"timeline"`
	SourceStatus             []logChainCloudWatchSourceStatus     `json:"source_status"`
	Evidence                 []logChainCloudWatchEvidence         `json:"evidence"`
	BlindSpots               []string                             `json:"blind_spots"`
	Cost                     logChainInvestigationCost            `json:"cost"`
	SensitiveDiagnosticsRead bool                                 `json:"sensitive_diagnostics_read"`
	CandidateTruncated       bool                                 `json:"candidate_truncated,omitempty"`
	AuditRecorded            bool                                 `json:"audit_recorded"`
	StartedAt                int64                                `json:"started_at"`
	CompletedAt              int64                                `json:"completed_at,omitempty"`
}

type logChainInvestigationTask struct {
	ID          string
	Owner       string
	Input       logChainInvestigationInput
	ScopeDigest string
	CacheKey    string
	Status      string
	Result      *logChainInvestigationResult
	Cancel      context.CancelFunc
	CreatedAt   time.Time
	CompletedAt time.Time
}

type logChainInvestigationCacheEntry struct {
	Result    logChainInvestigationResult
	ExpiresAt time.Time
}

// CloudWatchInvestigationAudit 是追加式事件账本；每个状态变化新增一行，
// 不更新旧行。OperatorHMAC/ScopeHMAC 均不可反查原始用户名、Request ID 或 IP。
type CloudWatchInvestigationAudit struct {
	ID              uint   `gorm:"primaryKey"`
	InvestigationID string `gorm:"size:64;index;not null"`
	Event           string `gorm:"size:24;index;not null"`
	Status          string `gorm:"size:24;not null"`
	OperatorHMAC    string `gorm:"size:64;index;not null"`
	ScopeHMAC       string `gorm:"size:64;index;not null"`
	HMACKeyID       string `gorm:"size:64;not null"`
	FilterType      string `gorm:"size:32;not null"`
	PurposeClass    string `gorm:"size:32;not null"`
	FromUnix        int64  `gorm:"not null"`
	ToUnix          int64  `gorm:"not null"`
	Sensitive       bool   `gorm:"not null"`
	CacheHit        bool   `gorm:"not null"`
	Queries         int    `gorm:"not null"`
	BytesScanned    uint64 `gorm:"not null"`
	Sources         string `gorm:"size:256;not null"`
	Hits            uint64 `gorm:"not null"`
	ParseFailures   uint64 `gorm:"not null"`
	SourceFailures  int    `gorm:"not null"`
	Truncated       bool   `gorm:"not null"`
	DurationMS      int64  `gorm:"not null"`
	CreatedAtUnix   int64  `gorm:"index;not null"`
}

func parseLogChainInvestigationInput(in logChainInvestigationCreateRequest, now time.Time) (logChainInvestigationInput, error) {
	out := logChainInvestigationInput{
		Timezone: strings.TrimSpace(in.Timezone), UserID: in.UserID,
		NewAPIRequestID: strings.TrimSpace(in.NewAPIRequestID), CloudFrontRequestID: strings.TrimSpace(in.CloudFrontRequestID),
		Model: strings.TrimSpace(in.Model), Group: strings.TrimSpace(in.Group), Path: strings.TrimSpace(in.Path),
		Status: in.Status, ClientIP: strings.TrimSpace(in.ClientIP),
		IncludeSensitiveDiagnostics: in.IncludeSensitiveDiagnostics, Purpose: strings.TrimSpace(in.Purpose),
	}
	if out.Timezone == "" {
		out.Timezone = "Asia/Shanghai"
	}
	if out.Timezone != "Asia/Shanghai" && out.Timezone != "UTC" {
		return out, errors.New("timezone 只支持 Asia/Shanghai 或 UTC")
	}
	if out.Purpose == "" {
		out.Purpose = "客户排障"
	}
	if len(out.Purpose) > 80 || !utf8.ValidString(out.Purpose) || hasControlRune(out.Purpose) {
		return out, errors.New("purpose 不合法")
	}
	if out.UserID < 0 {
		return out, errors.New("user_id 不能为负数")
	}
	for name, value := range map[string]string{"newapi_request_id": out.NewAPIRequestID, "cloudfront_request_id": out.CloudFrontRequestID} {
		if value == "" {
			continue
		}
		if _, err := validateCloudWatchFilterToken(value); err != nil {
			return out, fmt.Errorf("%s 含不支持的字符", name)
		}
	}
	// Model/group are business identifiers from the NewAPI logs.  They are used
	// only as parameterized local-database predicates and as an in-memory match
	// against structured CloudWatch evidence; they are never interpolated into
	// a Logs Insights query.  Do not reuse cwBusinessLabel here: that helper is
	// deliberately a *log parser redaction* gate and rejects perfectly valid
	// customer-owned names such as `Claude max (distributor)` or `foo#bar` (and
	// names containing the word "secret").  Reusing it made clicking a single
	// row fail with "group 不合法" even though the row came from our own logs.
	if value, ok := cwInvestigationScopeLabel(out.Model, 128); !ok {
		return out, errors.New("model 不合法")
	} else {
		out.Model = value
	}
	if value, ok := cwInvestigationScopeLabel(out.Group, 64); !ok {
		return out, errors.New("group 不合法")
	} else {
		out.Group = value
	}
	if out.Path != "" && (!strings.HasPrefix(out.Path, "/") || len(out.Path) > 1024 || !utf8.ValidString(out.Path) || hasControlRune(out.Path)) {
		return out, errors.New("path 必须是合法的绝对 API 路径")
	}
	if out.Status != 0 && (out.Status < 100 || out.Status > 599) {
		return out, errors.New("status 必须是 100 到 599")
	}
	if out.ClientIP != "" {
		ip := net.ParseIP(out.ClientIP)
		if ip == nil {
			return out, errors.New("client_ip 不合法")
		}
		out.ClientIP = ip.String()
		out.IncludeSensitiveDiagnostics = true
	}

	if strings.TrimSpace(in.From) != "" || strings.TrimSpace(in.To) != "" {
		if strings.TrimSpace(in.From) == "" || strings.TrimSpace(in.To) == "" {
			return out, errors.New("from 和 to 必须同时提供")
		}
		var err error
		out.From, err = time.Parse(time.RFC3339, strings.TrimSpace(in.From))
		if err != nil {
			return out, errors.New("from 必须是 RFC3339 时间")
		}
		out.To, err = time.Parse(time.RFC3339, strings.TrimSpace(in.To))
		if err != nil {
			return out, errors.New("to 必须是 RFC3339 时间")
		}
	} else {
		if in.AtUnix <= 0 {
			return out, errors.New("必须提供 from/to 或 at_unix")
		}
		at := time.Unix(in.AtUnix, 0).UTC()
		window := 5 * time.Minute
		if out.NewAPIRequestID != "" || out.CloudFrontRequestID != "" {
			window = logChainCloudWatchWindow
		} else if out.ClientIP != "" || out.Path != "" {
			window = 10 * time.Minute
		}
		out.From, out.To = at.Add(-window), at.Add(window)
	}
	out.From, out.To = out.From.UTC(), out.To.UTC()
	if !out.From.Before(out.To) || out.To.Sub(out.From) > logChainInvestigationMaxWindow {
		return out, errors.New("排障时间范围必须大于 0 且不超过 2 小时")
	}
	if out.To.After(now.Add(logChainCloudWatchMaxFuture)) || out.From.Before(now.Add(-logChainCloudWatchMaxPast)) {
		return out, errors.New("排障时间范围超出 CloudWatch 可查询边界")
	}
	if out.NewAPIRequestID == "" && out.CloudFrontRequestID == "" && out.ClientIP == "" && out.UserID == 0 && out.Model == "" && out.Path == "" {
		return out, errors.New("至少提供 Request ID、客户 ID、模型、路径或客户 IP 之一")
	}
	return out, nil
}

// cwInvestigationScopeLabel validates a value supplied as a business-scope
// predicate.  It intentionally has a different contract from
// cwBusinessLabel: the latter sanitizes values extracted from untrusted log
// text and therefore rejects punctuation/secret-looking fragments.  A group
// name is configured by the customer/admin and may legitimately contain those
// characters.  Scope values are passed to SQL as bind parameters and compared
// to parsed structured fields, so the boundary needed here is UTF-8, length,
// and control-rune safety only.
func cwInvestigationScopeLabel(raw string, maximum int) (string, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", true
	}
	if maximum <= 0 || len(value) > maximum || !utf8.ValidString(value) {
		return "", false
	}
	if hasControlRune(value) {
		return "", false
	}
	return value, true
}

func hasControlRune(value string) bool {
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

func (m *Monitor) investigationDigest(domain string, values ...string) string {
	mac := hmac.New(sha256.New, []byte(m.cfg.CloudWatchEvidenceHMACKey))
	_, _ = mac.Write([]byte(domain))
	for _, value := range values {
		_, _ = mac.Write([]byte{0})
		_, _ = mac.Write([]byte(value))
	}
	return hex.EncodeToString(mac.Sum(nil))
}

func (m *Monitor) investigationScopeDigest(in logChainInvestigationInput) string {
	encoded, _ := json.Marshal([]any{in.From.Unix(), in.To.Unix(), in.UserID, in.NewAPIRequestID,
		in.CloudFrontRequestID, in.Model, in.Group, in.Path, in.Status, in.ClientIP, in.IncludeSensitiveDiagnostics, in.Purpose})
	return m.investigationDigest("cloudwatch-investigation-scope", string(encoded))
}

func (m *Monitor) investigationScopeView(in logChainInvestigationInput) logChainInvestigationScopeView {
	view := logChainInvestigationScopeView{
		FromUTC: in.From.Format(time.RFC3339), ToUTC: in.To.Format(time.RFC3339), Timezone: in.Timezone,
		UserID: in.UserID, Model: in.Model, Group: in.Group, Path: in.Path, Status: in.Status,
		SensitiveDiagnostics: in.IncludeSensitiveDiagnostics,
	}
	if in.NewAPIRequestID != "" {
		view.NewAPIRequestRef = m.investigationDigest("oneapi-request-id", in.NewAPIRequestID)
	}
	if in.CloudFrontRequestID != "" {
		view.CloudFrontRef = m.investigationDigest("cloudfront-request-id", in.CloudFrontRequestID)
	}
	if in.ClientIP != "" {
		view.ClientIPRef = m.investigationDigest("client-ip", in.ClientIP)
	}
	return view
}

func newLogChainInvestigationID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "inv_" + hex.EncodeToString(raw), nil
}

func investigationFilterType(in logChainInvestigationInput) string {
	switch {
	case in.NewAPIRequestID != "":
		return "newapi_request_id"
	case in.CloudFrontRequestID != "":
		return "cloudfront_request_id"
	case in.ClientIP != "":
		return "client_ip"
	case in.UserID > 0:
		return "user_id_time"
	case in.Path != "":
		return "path_time"
	default:
		return "model_time"
	}
}

func investigationPurposeClass(purpose string) string {
	lower := strings.ToLower(strings.TrimSpace(purpose))
	switch {
	case strings.Contains(lower, "渠道测试"):
		return "channel_test"
	case strings.Contains(lower, "订阅"):
		return "subscription"
	case strings.Contains(lower, "邮件"):
		return "email"
	case strings.Contains(lower, "后台") || strings.Contains(lower, "master"):
		return "background_task"
	case strings.Contains(lower, "客户") || strings.Contains(lower, "排障"):
		return "customer_troubleshooting"
	default:
		return "other"
	}
}

func (m *Monitor) appendInvestigationAudit(task *logChainInvestigationTask, event, status string, result *logChainInvestigationResult) error {
	if m.storeDB == nil {
		return errors.New("Monitor 审计库不可用")
	}
	row := CloudWatchInvestigationAudit{
		InvestigationID: task.ID, Event: event, Status: status, OperatorHMAC: task.Owner,
		ScopeHMAC: task.ScopeDigest, HMACKeyID: m.cfg.CloudWatchEvidenceHMACKeyID,
		FilterType: investigationFilterType(task.Input), PurposeClass: investigationPurposeClass(task.Input.Purpose),
		FromUnix: task.Input.From.Unix(), ToUnix: task.Input.To.Unix(),
		Sensitive: task.Input.IncludeSensitiveDiagnostics, CreatedAtUnix: time.Now().Unix(),
	}
	if result != nil {
		row.CacheHit, row.Queries, row.BytesScanned = result.Cost.CacheHit, result.Cost.Queries, result.Cost.BytesScanned
		row.DurationMS = time.Since(task.CreatedAt).Milliseconds()
		if row.DurationMS < 0 {
			row.DurationMS = 0
		}
		sources := make([]string, 0, len(result.SourceStatus))
		for _, source := range result.SourceStatus {
			if source.Status == "skipped" {
				continue
			}
			sources = append(sources, string(source.Source))
			row.Hits += source.Parsed
			row.ParseFailures += source.ParseFailed
			row.Truncated = row.Truncated || source.Truncated
			if source.Status != "found" && source.Status != "empty" || source.Partial {
				row.SourceFailures++
			}
		}
		row.Sources = strings.Join(sources, ",")
	}
	return m.storeDB.Create(&row).Error
}

type logChainInvestigationActiveError struct {
	ID     string
	Status string
}

func (e *logChainInvestigationActiveError) Error() string {
	return "当前操作者已有排障任务正在运行"
}

func (m *Monitor) createLogChainInvestigation(ownerName string, in logChainInvestigationInput) (*logChainInvestigationResult, error) {
	if !m.cloudWatchEvidenceAvailable() {
		return nil, errors.New("CloudWatch Logs 排障未启用")
	}
	if m.shuttingDown.Load() {
		return nil, errors.New("Monitor 正在停止，暂不能创建排障任务")
	}
	id, err := newLogChainInvestigationID()
	if err != nil {
		return nil, errors.New("生成排障任务编号失败")
	}
	now := time.Now()
	owner := m.investigationDigest("cloudwatch-investigation-operator", ownerName)
	digest := m.investigationScopeDigest(in)
	cacheKey := m.investigationDigest("cloudwatch-investigation-cache", owner, digest)

	m.investigationMu.Lock()
	if m.shuttingDown.Load() {
		m.investigationMu.Unlock()
		return nil, errors.New("Monitor 正在停止，暂不能创建排障任务")
	}
	m.initInvestigationStateLocked()
	m.cleanupInvestigationsLocked(now)
	if activeID := m.investigationByOwner[owner]; activeID != "" {
		if active := m.investigationTasks[activeID]; active != nil && (active.Status == "queued" || active.Status == "running") {
			m.investigationMu.Unlock()
			return nil, &logChainInvestigationActiveError{ID: activeID, Status: active.Status}
		}
		delete(m.investigationByOwner, owner)
	}
	if cached, ok := m.investigationCache[cacheKey]; ok && now.Before(cached.ExpiresAt) {
		result := cached.Result
		result.InvestigationID, result.Status, result.Cost.CacheHit = id, "complete", true
		result.StartedAt, result.CompletedAt, result.AuditRecorded = now.Unix(), now.Unix(), true
		task := &logChainInvestigationTask{ID: id, Owner: owner, Input: in, ScopeDigest: digest, CacheKey: cacheKey, Status: "complete", Result: &result, CreatedAt: now, CompletedAt: now}
		m.investigationTasks[id] = task
		// 缓存命中仍要落审计；把这段同步写入也纳入关闭等待，避免 Close
		// 在审计写入过程中先关闭 SQLite。
		m.investigationWG.Add(1)
		m.investigationMu.Unlock()
		defer m.investigationWG.Done()
		if err := m.appendInvestigationAudit(task, "cache_hit", "complete", &result); err != nil {
			m.investigationMu.Lock()
			result.AuditRecorded = false
			result.BlindSpots = append(result.BlindSpots, "本次缓存命中结果可用，但审计写入失败。")
			task.Result = &result
			m.investigationMu.Unlock()
		}
		return &result, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	task := &logChainInvestigationTask{ID: id, Owner: owner, Input: in, ScopeDigest: digest, CacheKey: cacheKey, Status: "queued", Cancel: cancel, CreatedAt: now}
	m.investigationTasks[id] = task
	m.investigationByOwner[owner] = id
	// Add 必须发生在任务对 Close 可见之前。否则 Close 可能在 unlock 与 Add
	// 之间看到任务、调用 Wait 并提前释放 DB/HTTP 资源。
	m.investigationWG.Add(1)
	m.investigationMu.Unlock()
	if err := m.appendInvestigationAudit(task, "created", "queued", nil); err != nil {
		cancel()
		m.investigationMu.Lock()
		delete(m.investigationTasks, id)
		delete(m.investigationByOwner, owner)
		m.investigationMu.Unlock()
		m.investigationWG.Done()
		return nil, errors.New("排障任务审计写入失败，已拒绝启动")
	}
	go func() {
		defer m.investigationWG.Done()
		m.runLogChainInvestigationTask(ctx, task)
	}()
	return &logChainInvestigationResult{
		OK: true, InvestigationID: id, Status: "queued", Scope: m.investigationScopeView(in),
		AuditRecorded: true, StartedAt: now.Unix(),
	}, nil
}

func (m *Monitor) initInvestigationStateLocked() {
	if m.investigationTasks == nil {
		m.investigationTasks = make(map[string]*logChainInvestigationTask)
	}
	if m.investigationCache == nil {
		m.investigationCache = make(map[string]logChainInvestigationCacheEntry)
	}
	if m.investigationByOwner == nil {
		m.investigationByOwner = make(map[string]string)
	}
}

func (m *Monitor) cleanupInvestigationsLocked(now time.Time) {
	for key, entry := range m.investigationCache {
		if !now.Before(entry.ExpiresAt) {
			delete(m.investigationCache, key)
		}
	}
	for id, task := range m.investigationTasks {
		if !task.CompletedAt.IsZero() && now.Sub(task.CompletedAt) > logChainInvestigationKeep {
			delete(m.investigationTasks, id)
		}
	}
	if len(m.investigationTasks) <= logChainInvestigationMaxTasks {
		return
	}
	for id, task := range m.investigationTasks {
		if task.Status != "queued" && task.Status != "running" {
			delete(m.investigationTasks, id)
			if len(m.investigationTasks) <= logChainInvestigationMaxTasks {
				break
			}
		}
	}
}

func (m *Monitor) runLogChainInvestigationTask(parent context.Context, task *logChainInvestigationTask) {
	m.investigationMu.Lock()
	if current := m.investigationTasks[task.ID]; current == nil || current.Status == "cancelled" {
		m.investigationMu.Unlock()
		return
	}
	task.Status = "running"
	m.investigationMu.Unlock()
	startedAuditErr := m.appendInvestigationAudit(task, "started", "running", nil)

	ctx, cancel := context.WithTimeout(parent, logChainInvestigationTimeout)
	result := m.executeLogChainInvestigation(ctx, task.ID, task.Input)
	cancel()
	now := time.Now()

	m.investigationMu.Lock()
	current := m.investigationTasks[task.ID]
	if current == nil {
		m.investigationMu.Unlock()
		return
	}
	if current.Status == "cancelled" {
		delete(m.investigationByOwner, task.Owner)
		current.CompletedAt = now
		m.investigationMu.Unlock()
		return
	}
	result.InvestigationID, result.CompletedAt = task.ID, now.Unix()
	result.OK = result.Status != "failed"
	if result.StartedAt == 0 {
		result.StartedAt = task.CreatedAt.Unix()
	}
	result.AuditRecorded = startedAuditErr == nil
	if startedAuditErr != nil {
		result.BlindSpots = append(result.BlindSpots, "任务已执行，但开始事件的审计写入失败。")
	}
	current.Status, current.Result, current.CompletedAt = result.Status, &result, now
	delete(m.investigationByOwner, task.Owner)
	if result.Status == "complete" || result.Status == "partial" {
		ttl := logChainInvestigationWindowTTL
		if task.Input.NewAPIRequestID != "" || task.Input.CloudFrontRequestID != "" {
			ttl = logChainInvestigationExactTTL
		}
		m.investigationCache[task.CacheKey] = logChainInvestigationCacheEntry{Result: result, ExpiresAt: now.Add(ttl)}
	}
	m.investigationMu.Unlock()

	if err := m.appendInvestigationAudit(task, "finished", result.Status, &result); err != nil {
		m.investigationMu.Lock()
		if current := m.investigationTasks[task.ID]; current != nil && current.Result != nil {
			current.Result.AuditRecorded = false
			current.Result.BlindSpots = append(current.Result.BlindSpots, "排障结果已生成，但完成事件的审计写入失败。")
			if cached, ok := m.investigationCache[task.CacheKey]; ok {
				cached.Result = *current.Result
				m.investigationCache[task.CacheKey] = cached
			}
		}
		m.investigationMu.Unlock()
	}
	if result.Status == "pending_delivery" {
		m.schedulePendingDeliveryRechecks(parent, task)
	}
}

// schedulePendingDeliveryRechecks 按文档约定在 5、15、60 分钟后复查仍在投递期的
// CloudFront 证据。任务条件保留在内存，不把原始 Request ID/IP 写进 SQLite。
func (m *Monitor) schedulePendingDeliveryRechecks(parent context.Context, task *logChainInvestigationTask) {
	m.investigationWG.Add(1)
	go func() {
		defer m.investigationWG.Done()
		previous := time.Duration(0)
		rechecks := []time.Duration{5 * time.Minute, 15 * time.Minute, 60 * time.Minute}
		for index, after := range rechecks {
			timer := time.NewTimer(after - previous)
			select {
			case <-parent.Done():
				timer.Stop()
				return
			case <-m.shutdownSignal():
				timer.Stop()
				return
			case <-timer.C:
			}
			previous = after

			m.investigationMu.Lock()
			current := m.investigationTasks[task.ID]
			if current == nil || current.Status != "pending_delivery" {
				m.investigationMu.Unlock()
				return
			}
			m.investigationMu.Unlock()

			ctx, cancel := context.WithTimeout(parent, logChainInvestigationTimeout)
			result := m.recheckPendingCloudFront(ctx, task)
			cancel()
			result = finalizePendingDeliveryRecheck(result, index == len(rechecks)-1)
			result.InvestigationID = task.ID
			result.OK = result.Status != "failed"
			result.CompletedAt = time.Now().Unix()

			m.investigationMu.Lock()
			current = m.investigationTasks[task.ID]
			if current == nil || current.Status == "cancelled" {
				m.investigationMu.Unlock()
				return
			}
			current.Status, current.Result, current.CompletedAt = result.Status, &result, time.Now()
			if result.Status == "complete" || result.Status == "partial" {
				ttl := logChainInvestigationWindowTTL
				if task.Input.NewAPIRequestID != "" || task.Input.CloudFrontRequestID != "" {
					ttl = logChainInvestigationExactTTL
				}
				m.investigationCache[task.CacheKey] = logChainInvestigationCacheEntry{Result: result, ExpiresAt: time.Now().Add(ttl)}
			}
			m.investigationMu.Unlock()
			if err := m.appendInvestigationAudit(task, "rechecked", result.Status, &result); err != nil {
				m.investigationMu.Lock()
				if current := m.investigationTasks[task.ID]; current != nil && current.Result != nil {
					current.Result.AuditRecorded = false
					current.Result.BlindSpots = append(current.Result.BlindSpots, "CloudFront 延迟复查完成，但复查事件的审计写入失败。")
					if cached, ok := m.investigationCache[task.CacheKey]; ok {
						cached.Result = *current.Result
						m.investigationCache[task.CacheKey] = cached
					}
				}
				m.investigationMu.Unlock()
			}
			if result.Status != "pending_delivery" {
				return
			}
		}
	}()
}

func (m *Monitor) getLogChainInvestigation(ownerName, id string) (logChainInvestigationResult, bool) {
	owner := m.investigationDigest("cloudwatch-investigation-operator", ownerName)
	m.investigationMu.Lock()
	defer m.investigationMu.Unlock()
	m.initInvestigationStateLocked()
	task := m.investigationTasks[id]
	if task == nil || task.Owner != owner {
		return logChainInvestigationResult{}, false
	}
	if task.Result != nil {
		result := *task.Result
		result.Status = task.Status
		return result, true
	}
	return logChainInvestigationResult{
		OK: true, InvestigationID: task.ID, Status: task.Status, Scope: m.investigationScopeView(task.Input),
		AuditRecorded: true, StartedAt: task.CreatedAt.Unix(),
	}, true
}

func (m *Monitor) cancelLogChainInvestigation(ownerName, id string) (logChainInvestigationResult, bool) {
	owner := m.investigationDigest("cloudwatch-investigation-operator", ownerName)
	m.investigationMu.Lock()
	m.initInvestigationStateLocked()
	task := m.investigationTasks[id]
	if task == nil || task.Owner != owner {
		m.investigationMu.Unlock()
		return logChainInvestigationResult{}, false
	}
	if task.Status == "queued" || task.Status == "running" || task.Status == "pending_delivery" {
		task.Status, task.CompletedAt = "cancelled", time.Now()
		delete(m.investigationByOwner, task.Owner)
		if task.Result == nil {
			task.Result = &logChainInvestigationResult{
				OK: true, InvestigationID: id, Status: "cancelled", Scope: m.investigationScopeView(task.Input),
				Summary:       logChainInvestigationSummary{Classification: "cancelled", EvidenceLevel: "unavailable", Conclusion: "排障任务已取消", CustomerImpact: "unknown"},
				AuditRecorded: true, StartedAt: task.CreatedAt.Unix(), CompletedAt: task.CompletedAt.Unix(),
			}
		} else {
			// pending_delivery 已经有一份结果；取消时保留现有证据，但立即把
			// 响应状态改成 cancelled，不能让取消接口继续回 pending_delivery。
			task.Result.Status = "cancelled"
			task.Result.CompletedAt = task.CompletedAt.Unix()
		}
		cancel := task.Cancel
		result := *task.Result
		m.investigationMu.Unlock()
		cancel()
		if err := m.appendInvestigationAudit(task, "cancelled", "cancelled", &result); err != nil {
			m.investigationMu.Lock()
			if current := m.investigationTasks[id]; current != nil && current.Result != nil {
				current.Result.AuditRecorded = false
				current.Result.BlindSpots = append(current.Result.BlindSpots, "任务已取消，但取消事件的审计写入失败。")
				result = *current.Result
			}
			m.investigationMu.Unlock()
		}
		return result, true
	}
	result := *task.Result
	m.investigationMu.Unlock()
	return result, true
}
