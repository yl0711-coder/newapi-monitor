// Command nginxcollector tails a dedicated, sanitized Nginx JSON access log and
// sends only bounded minute aggregates to Monitor. It never sends raw log lines.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type config struct {
	node               string
	logPath            string
	cursorPath         string
	sinkURL            string
	token              string
	interval           time.Duration
	maxLines           int
	retentionDays      int
	allowHTTP          bool
	evidenceMode       string
	evidenceSinkURL    string
	evidenceHMACKey    []byte
	evidenceHMACKeyID  string
	evidenceOutboxPath string
	evidenceOutboxMax  int64
	evidenceFSMu       *sync.Mutex
	errorEnabled       bool
	errorLogPath       string
	errorCursorPath    string
	errorSinkURL       string
	errorTimezone      string
	// sourceV2Prepare enables only the v1 boundary proof used before a v2
	// cutover. It is default-off so a collector-only rolling upgrade remains
	// byte-for-byte compatible with an older/default-off Monitor.
	sourceV2Prepare bool
	sourceV2Access  bool
	sourceV2Error   bool
	sourceV2BaseURL string
	sourceV2Cursor  string
	errorV2Cursor   string
	// sourceV2* are runtime-only parser identity. They are set by the v2
	// runner after a manifest-bound file is selected and are never read from
	// environment variables.
	sourceV2Epoch  string
	sourceV2FileID string
}

func loadConfig() (config, error) {
	intervalSeconds, err := boundedEnvInt("NGINXCOLLECTOR_INTERVAL_SECONDS", 5, 1, 3600)
	if err != nil {
		return config{}, err
	}
	maxLines, err := boundedEnvInt("NGINXCOLLECTOR_MAX_LINES", 1000, 1, 2000)
	if err != nil {
		return config{}, err
	}
	retentionDays, err := boundedEnvInt("NGINXCOLLECTOR_RETENTION_DAYS", 7, 1, 90)
	if err != nil {
		return config{}, err
	}
	evidenceOutboxMiB, err := boundedEnvInt("NGINXCOLLECTOR_EVIDENCE_OUTBOX_MAX_MIB", 256, 8, 2048)
	if err != nil {
		return config{}, err
	}
	c := config{
		node: strings.TrimSpace(os.Getenv("NGINXCOLLECTOR_NODE")), logPath: env("NGINXCOLLECTOR_LOG_PATH", "/logs/nexusapi_access.jsonl"),
		cursorPath: env("NGINXCOLLECTOR_CURSOR_PATH", "/data/cursor.json"), sinkURL: strings.TrimSpace(os.Getenv("NGINXCOLLECTOR_SINK_URL")),
		token: os.Getenv("NGINXCOLLECTOR_TOKEN"), interval: time.Duration(intervalSeconds) * time.Second,
		maxLines: maxLines, retentionDays: retentionDays,
		allowHTTP:          strings.EqualFold(strings.TrimSpace(os.Getenv("NGINXCOLLECTOR_ALLOW_INSECURE_HTTP")), "true"),
		evidenceMode:       strings.ToLower(strings.TrimSpace(env("NGINXCOLLECTOR_EVIDENCE_MODE", "off"))),
		evidenceSinkURL:    strings.TrimSpace(os.Getenv("NGINXCOLLECTOR_EVIDENCE_SINK_URL")),
		evidenceHMACKey:    []byte(os.Getenv("NGINXCOLLECTOR_EVIDENCE_HMAC_KEY")),
		evidenceHMACKeyID:  strings.TrimSpace(os.Getenv("NGINXCOLLECTOR_EVIDENCE_HMAC_KEY_ID")),
		evidenceOutboxPath: env("NGINXCOLLECTOR_EVIDENCE_OUTBOX_PATH", "/data/evidence-outbox"),
		evidenceOutboxMax:  int64(evidenceOutboxMiB) << 20,
		evidenceFSMu:       &sync.Mutex{},
		errorEnabled:       strings.EqualFold(strings.TrimSpace(os.Getenv("NGINXCOLLECTOR_ERROR_ENABLED")), "true"),
		errorLogPath:       env("NGINXCOLLECTOR_ERROR_LOG_PATH", "/logs/error.log"),
		errorCursorPath:    env("NGINXCOLLECTOR_ERROR_CURSOR_PATH", "/data/error-cursor.json"),
		errorSinkURL:       strings.TrimSpace(os.Getenv("NGINXCOLLECTOR_ERROR_SINK_URL")),
		errorTimezone:      strings.TrimSpace(env("NGINXCOLLECTOR_ERROR_TIMEZONE", "UTC")),
		sourceV2Prepare:    strings.EqualFold(strings.TrimSpace(os.Getenv("NGINXCOLLECTOR_SOURCE_V2_PREPARE")), "true"),
		sourceV2BaseURL:    strings.TrimRight(strings.TrimSpace(os.Getenv("NGINXCOLLECTOR_SOURCE_V2_BASE_URL")), "/"),
		sourceV2Cursor:     env("NGINXCOLLECTOR_SOURCE_V2_ACCESS_CURSOR_PATH", "/data/access-source-v2.json"),
		errorV2Cursor:      env("NGINXCOLLECTOR_SOURCE_V2_ERROR_CURSOR_PATH", "/data/error-source-v2.json"),
	}
	lanes := map[string]bool{}
	for _, raw := range strings.Split(strings.TrimSpace(os.Getenv("NGINXCOLLECTOR_SOURCE_V2_LANES")), ",") {
		lane := strings.ToLower(strings.TrimSpace(raw))
		if lane == "" {
			continue
		}
		if lane != "access" && lane != "error" || lanes[lane] {
			return config{}, fmt.Errorf("NGINXCOLLECTOR_SOURCE_V2_LANES only accepts unique access,error lanes")
		}
		lanes[lane] = true
	}
	c.sourceV2Access, c.sourceV2Error = lanes["access"], lanes["error"]
	if c.node == "" || c.sinkURL == "" || c.token == "" {
		return config{}, fmt.Errorf("NGINXCOLLECTOR_NODE, NGINXCOLLECTOR_SINK_URL and NGINXCOLLECTOR_TOKEN are required")
	}
	if !validNodeName(c.node) {
		return config{}, fmt.Errorf("NGINXCOLLECTOR_NODE must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}")
	}
	if err := validateSinkURL(c.sinkURL, c.allowHTTP); err != nil {
		return config{}, err
	}
	if c.evidenceMode != "off" && c.evidenceMode != "pilot" && c.evidenceMode != "verified" {
		return config{}, fmt.Errorf("NGINXCOLLECTOR_EVIDENCE_MODE must be off, pilot or verified")
	}
	if c.evidenceMode != "off" {
		if err := validateSinkURL(c.evidenceSinkURL, c.allowHTTP); err != nil {
			return config{}, fmt.Errorf("invalid evidence sink: %w", err)
		}
		if len(c.evidenceHMACKey) < 32 || !validEvidenceKeyID(c.evidenceHMACKeyID) {
			return config{}, fmt.Errorf("evidence HMAC key must be at least 32 bytes and key id must be valid")
		}
		if string(c.evidenceHMACKey) == c.token {
			return config{}, fmt.Errorf("evidence HMAC key must differ from ingest token")
		}
		if c.evidenceOutboxMax < 8<<20 || c.evidenceOutboxMax > 2<<30 {
			return config{}, fmt.Errorf("NGINXCOLLECTOR_EVIDENCE_OUTBOX_MAX_MIB must be between 8 and 2048")
		}
	}
	if c.errorEnabled {
		if err := validateSinkURL(c.errorSinkURL, c.allowHTTP); err != nil {
			return config{}, fmt.Errorf("invalid error sink: %w", err)
		}
		if c.errorLogPath == c.logPath || c.errorCursorPath == c.cursorPath {
			return config{}, fmt.Errorf("Nginx error log must use an independent log and cursor")
		}
		if _, err := time.LoadLocation(c.errorTimezone); err != nil {
			return config{}, fmt.Errorf("NGINXCOLLECTOR_ERROR_TIMEZONE is invalid: %w", err)
		}
	}
	if c.sourceV2Access || c.sourceV2Error {
		if !c.sourceV2Prepare {
			return config{}, fmt.Errorf("source v2 lanes require NGINXCOLLECTOR_SOURCE_V2_PREPARE=true and an acknowledged v1 boundary")
		}
		if err := validateSourceV2BaseURL(c.sourceV2BaseURL, c.allowHTTP); err != nil {
			return config{}, err
		}
		if !filepath.IsAbs(c.sourceV2Cursor) || !filepath.IsAbs(c.errorV2Cursor) || c.sourceV2Cursor == c.cursorPath || c.errorV2Cursor == c.errorCursorPath || c.sourceV2Cursor == c.errorV2Cursor {
			return config{}, errors.New("source v2 access/error cursors must use independent absolute paths")
		}
		if c.sourceV2Error && !c.errorEnabled {
			return config{}, errors.New("source v2 error lane requires NGINXCOLLECTOR_ERROR_ENABLED=true")
		}
	}
	return c, nil
}

func validateSourceV2BaseURL(raw string, allowHTTP bool) error {
	if err := validateSinkURL(raw, allowHTTP); err != nil {
		return fmt.Errorf("invalid source v2 base URL: %w", err)
	}
	u, _ := url.ParseRequestURI(raw)
	if u.RawQuery != "" || u.Path != "" && u.Path != "/" {
		return errors.New("NGINXCOLLECTOR_SOURCE_V2_BASE_URL must contain only scheme and authority")
	}
	return nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func boundedEnvInt(key string, fallback, min, max int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < min || value > max {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", key, min, max)
	}
	return value, nil
}

func validNodeName(value string) bool {
	if len(value) < 1 || len(value) > 64 {
		return false
	}
	for i, r := range value {
		letter := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
		digit := r >= '0' && r <= '9'
		if letter || digit || i > 0 && (r == '.' || r == '_' || r == '-') {
			continue
		}
		return false
	}
	return true
}

func validateSinkURL(raw string, allowHTTP bool) error {
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Host == "" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return fmt.Errorf("NGINXCOLLECTOR_SINK_URL must be an absolute HTTP(S) URL without credentials or fragment")
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return fmt.Errorf("NGINXCOLLECTOR_SINK_URL must use HTTPS")
	}
	if u.Scheme == "http" && !allowHTTP {
		return fmt.Errorf("NGINXCOLLECTOR_SINK_URL uses HTTP; set NGINXCOLLECTOR_ALLOW_INSECURE_HTTP=true only for a verified private path")
	}
	return nil
}

const cursorVersion = 1

type cursor struct {
	Version                      int    `json:"version"`
	Device                       uint64 `json:"device,omitempty"`
	Inode                        uint64 `json:"inode"`
	Offset                       int64  `json:"offset"`
	Discontinuities              int64  `json:"discontinuities,omitempty"`
	LastDiscontinuityAt          int64  `json:"last_discontinuity_at,omitempty"`
	DiscardedLines               int64  `json:"discarded_lines,omitempty"`
	LastDiscardedAt              int64  `json:"last_discarded_at,omitempty"`
	LastLogSchema                int    `json:"last_log_schema,omitempty"`
	EvidenceEligible             int64  `json:"evidence_eligible,omitempty"`
	EvidenceParseRejected        int64  `json:"evidence_parse_rejected,omitempty"`
	LastEvidenceParseRejectedAt  int64  `json:"last_evidence_parse_rejected_at,omitempty"`
	EvidencePersistFailures      int64  `json:"evidence_persist_failures,omitempty"`
	EvidenceDroppedEvents        int64  `json:"evidence_dropped_events,omitempty"`
	LastEvidencePersistFailureAt int64  `json:"last_evidence_persist_failure_at,omitempty"`
	LastAckedBatchID             string `json:"last_acked_batch_id,omitempty"`
	LastAckedDevice              uint64 `json:"last_acked_device,omitempty"`
	LastAckedInode               uint64 `json:"last_acked_inode,omitempty"`
	LastAckedOffset              int64  `json:"last_acked_offset,omitempty"`
}

type sourceBoundaryV1 struct {
	Device      uint64 `json:"device"`
	Inode       uint64 `json:"inode"`
	StartOffset int64  `json:"start_offset"`
	EndOffset   int64  `json:"end_offset"`
	Checkpoint  bool   `json:"checkpoint,omitempty"`
}

type sourceBoundaryAckV1 struct {
	Protocol    int    `json:"protocol"`
	BatchID     string `json:"batch_id"`
	Device      uint64 `json:"device"`
	Inode       uint64 `json:"inode"`
	StartOffset int64  `json:"start_offset"`
	EndOffset   int64  `json:"end_offset"`
	Checkpoint  bool   `json:"checkpoint,omitempty"`
}

func validSourceBoundaryAckV1(ack *sourceBoundaryAckV1, payload batch) bool {
	boundary := payload.SourceBoundary
	return boundary == nil && ack == nil || boundary != nil && ack != nil && ack.Protocol == 1 && ack.BatchID == payload.BatchID &&
		ack.Device == boundary.Device && ack.Inode == boundary.Inode && ack.StartOffset == boundary.StartOffset && ack.EndOffset == boundary.EndOffset && ack.Checkpoint == boundary.Checkpoint
}

type flexNumber float64

func (n *flexNumber) UnmarshalJSON(data []byte) error {
	text := strings.TrimSpace(string(data))
	if text == "" || text == "null" || text == `""` || text == `"-"` {
		*n = 0
		return nil
	}
	if strings.HasPrefix(text, `"`) {
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		value = strings.TrimSpace(strings.SplitN(value, ",", 2)[0])
		if value == "" || value == "-" {
			*n = 0
			return nil
		}
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return err
		}
		*n = flexNumber(parsed)
		return nil
	}
	parsed, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return err
	}
	*n = flexNumber(parsed)
	return nil
}

// upstreamNumber 解析 Nginx upstream 变量。Nginx 在请求重试或内部
// 跳转时会用逗号或冒号连接多个值；报表的单值口径明确取最后
// 一次 upstream 响应。Present 用来区分“无 upstream(-)”和“有 upstream
// 但耗时为 0.000”，避免把后者丢掉。
type upstreamNumber struct {
	Value   float64
	Present bool
}

func (n *upstreamNumber) UnmarshalJSON(data []byte) error {
	text := strings.TrimSpace(string(data))
	if text == "" || text == "null" || text == `""` || text == `"-"` {
		*n = upstreamNumber{}
		return nil
	}
	if !strings.HasPrefix(text, `"`) {
		parsed, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return err
		}
		*n = upstreamNumber{Value: parsed, Present: true}
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	parts := strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ':' })
	if len(parts) == 0 {
		return fmt.Errorf("empty upstream number sequence")
	}
	var final float64
	found := false
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "-" {
			continue
		}
		if part == "" {
			return fmt.Errorf("invalid upstream number sequence")
		}
		parsed, err := strconv.ParseFloat(part, 64)
		if err != nil {
			return err
		}
		final = parsed
		found = true
	}
	if !found {
		*n = upstreamNumber{}
		return nil
	}
	*n = upstreamNumber{Value: final, Present: true}
	return nil
}

type rawLine struct {
	Timestamp      flexNumber     `json:"ts"`
	Msec           flexNumber     `json:"msec"`
	Method         string         `json:"method"`
	RequestMethod  string         `json:"request_method"`
	URI            string         `json:"uri"`
	Path           string         `json:"path"`
	Status         flexNumber     `json:"status"`
	RequestTime    flexNumber     `json:"request_time"`
	UpstreamStatus upstreamNumber `json:"upstream_status"`
	UpstreamTime   upstreamNumber `json:"upstream_response_time"`
	BytesSent      flexNumber     `json:"bytes_sent"`
	RequestID      string         `json:"request_id"`
}

type sample struct {
	BucketTs          int64  `json:"bucket_ts"`
	Route             string `json:"route"`
	Method            string `json:"method"`
	Status            int    `json:"status"`
	UpstreamStatus    int    `json:"upstream_status"`
	Count             int64  `json:"count"`
	RequestTimeSumMS  int64  `json:"request_time_sum_ms"`
	RequestTimeMaxMS  int64  `json:"request_time_max_ms"`
	UpstreamTimeSumMS int64  `json:"upstream_time_sum_ms"`
	UpstreamTimeCount int64  `json:"upstream_time_count"`
	BytesSent         int64  `json:"bytes_sent"`
	RequestIDPresent  int64  `json:"request_id_present"`
	LatencyCount      int64  `json:"latency_count"`
	Latency0To1s      int64  `json:"latency_0_1s"`
	Latency1To5s      int64  `json:"latency_1_5s"`
	Latency5To15s     int64  `json:"latency_5_15s"`
	Latency15To30s    int64  `json:"latency_15_30s"`
	Latency30To60s    int64  `json:"latency_30_60s"`
	LatencyOver60s    int64  `json:"latency_over_60s"`
}

type batch struct {
	Node                      string            `json:"node"`
	BatchID                   string            `json:"batch_id"`
	Samples                   []sample          `json:"samples"`
	BacklogBytes              int64             `json:"backlog_bytes,omitempty"`
	BacklogKnown              bool              `json:"backlog_known,omitempty"`
	CursorDiscontinuities     int64             `json:"cursor_discontinuities,omitempty"`
	LastCursorDiscontinuityAt int64             `json:"last_cursor_discontinuity_at,omitempty"`
	DiscardedLines            int64             `json:"discarded_lines,omitempty"`
	LastDiscardedAt           int64             `json:"last_discarded_at,omitempty"`
	EvidencePersistFailures   int64             `json:"evidence_persist_failures,omitempty"`
	EvidenceDroppedEvents     int64             `json:"evidence_dropped_events,omitempty"`
	Evidence                  *evidenceBatch    `json:"-"`
	SourceBoundary            *sourceBoundaryV1 `json:"source_boundary,omitempty"`
	SourceRangeV2             *sourceRangeV2    `json:"source_range_v2,omitempty"`
}

const (
	accessInflightVersionV1 = 1
	accessInflightMaxBytes  = 8 << 20
	accessInflightPrepared  = "prepared"
	accessInflightAcked     = "minute_acked"
	accessInflightQueued    = "evidence_queued"
	accessInflightDropped   = "evidence_dropped"
)

// accessInflightV1 freezes one legacy access-lane source range before its
// network POST. The evidence payload is stored separately because batch keeps
// it off the minute wire with json:"-". The journal lives beside the durable
// cursor, never in the evidence outbox consumed by the delivery worker.
type accessInflightV1 struct {
	Version  int            `json:"version"`
	Stage    string         `json:"stage"`
	Current  cursor         `json:"current"`
	Next     cursor         `json:"next"`
	Payload  batch          `json:"payload"`
	Evidence *evidenceBatch `json:"evidence,omitempty"`
}

func normalizeRoute(path string) string {
	path = strings.TrimSpace(strings.SplitN(path, "?", 2)[0])
	switch path {
	case "/v1/chat/completions", "/v1/responses", "/v1/messages", "/v1/models", "/api/status":
		return path
	}
	if strings.HasPrefix(path, "/v1/") {
		return "/v1/*"
	}
	if strings.HasPrefix(path, "/api/") {
		return "/api/*"
	}
	return "/other"
}

func normalizeMethod(method string) string {
	method = strings.ToUpper(strings.TrimSpace(method))
	switch method {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
		return method
	default:
		return "OTHER"
	}
}

func parseAccessLine(data []byte) (sample, rawLine, bool) {
	var raw rawLine
	if len(data) > 64<<10 || json.Unmarshal(data, &raw) != nil {
		return sample{}, rawLine{}, false
	}
	ts := float64(raw.Timestamp)
	if ts <= 0 {
		ts = float64(raw.Msec)
	}
	statusValue, upstreamStatusValue := float64(raw.Status), raw.UpstreamStatus.Value
	requestSeconds, upstreamSeconds, sentBytes := float64(raw.RequestTime), raw.UpstreamTime.Value, float64(raw.BytesSent)
	for _, value := range []float64{ts, statusValue, upstreamStatusValue, requestSeconds, upstreamSeconds, sentBytes} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return sample{}, rawLine{}, false
		}
	}
	status, upstreamStatus := int(statusValue), int(upstreamStatusValue)
	if ts <= 0 || statusValue != float64(status) || status < 100 || status > 599 ||
		upstreamStatusValue != float64(upstreamStatus) || (upstreamStatus != 0 && (upstreamStatus < 100 || upstreamStatus > 599)) ||
		requestSeconds < 0 || requestSeconds > 86400 || upstreamSeconds < 0 || upstreamSeconds > 86400 ||
		sentBytes < 0 || sentBytes > 16<<30 {
		return sample{}, rawLine{}, false
	}
	method := raw.Method
	if method == "" {
		method = raw.RequestMethod
	}
	path := raw.Path
	if path == "" {
		path = raw.URI
	}
	requestMS := int64(math.Round(requestSeconds * 1000))
	upstreamMS := int64(math.Round(upstreamSeconds * 1000))
	out := sample{BucketTs: int64(ts) / 60 * 60, Route: normalizeRoute(path), Method: normalizeMethod(method), Status: status,
		UpstreamStatus: upstreamStatus, Count: 1, RequestTimeSumMS: requestMS, RequestTimeMaxMS: requestMS,
		BytesSent: int64(sentBytes), LatencyCount: 1}
	switch {
	case requestMS <= 1000:
		out.Latency0To1s = 1
	case requestMS <= 5000:
		out.Latency1To5s = 1
	case requestMS <= 15000:
		out.Latency5To15s = 1
	case requestMS <= 30000:
		out.Latency15To30s = 1
	case requestMS <= 60000:
		out.Latency30To60s = 1
	default:
		out.LatencyOver60s = 1
	}
	if raw.UpstreamTime.Present {
		out.UpstreamTimeSumMS, out.UpstreamTimeCount = upstreamMS, 1
	}
	if strings.TrimSpace(raw.RequestID) != "" && strings.TrimSpace(raw.RequestID) != "-" {
		out.RequestIDPresent = 1
	}
	return out, raw, true
}

func rawEventMS(raw rawLine) int64 {
	ts := float64(raw.Timestamp)
	if ts <= 0 {
		ts = float64(raw.Msec)
	}
	return int64(math.Round(ts * 1000))
}

func parseLine(data []byte) (sample, bool) {
	row, _, ok := parseAccessLine(data)
	return row, ok
}

func sampleWithinWindow(row sample, now int64, retentionDays int) bool {
	if retentionDays < 1 || retentionDays > 90 {
		retentionDays = 7
	}
	oldest := now - int64(retentionDays+1)*86400
	return row.BucketTs >= oldest/60*60 && row.BucketTs <= now+300
}

func sampleKey(row sample) string {
	return fmt.Sprintf("%d\x00%s\x00%s\x00%d\x00%d", row.BucketTs, row.Route, row.Method, row.Status, row.UpstreamStatus)
}

func merge(dst *sample, src sample) {
	dst.Count++
	dst.RequestTimeSumMS += src.RequestTimeSumMS
	if src.RequestTimeMaxMS > dst.RequestTimeMaxMS {
		dst.RequestTimeMaxMS = src.RequestTimeMaxMS
	}
	dst.UpstreamTimeSumMS += src.UpstreamTimeSumMS
	dst.UpstreamTimeCount += src.UpstreamTimeCount
	dst.BytesSent += src.BytesSent
	dst.RequestIDPresent += src.RequestIDPresent
	dst.LatencyCount += src.LatencyCount
	dst.Latency0To1s += src.Latency0To1s
	dst.Latency1To5s += src.Latency1To5s
	dst.Latency5To15s += src.Latency5To15s
	dst.Latency15To30s += src.Latency15To30s
	dst.Latency30To60s += src.Latency30To60s
	dst.LatencyOver60s += src.LatencyOver60s
}

func fileInode(info os.FileInfo) uint64 {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return stat.Ino
	}
	return uint64(info.ModTime().UnixNano())
}

func fileDevice(info os.FileInfo) uint64 {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return statDeviceValue(stat.Dev)
	}
	return 0
}

func loadCursor(path string) (cursor, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return cursor{}, nil
	}
	if err != nil {
		return cursor{}, fmt.Errorf("read cursor: %w", err)
	}
	var value cursor
	if json.Unmarshal(data, &value) != nil || value.Version != cursorVersion || value.Inode == 0 || value.Offset < 0 || value.Discontinuities < 0 || value.LastDiscontinuityAt < 0 ||
		value.DiscardedLines < 0 || value.LastDiscardedAt < 0 || value.LastLogSchema < 0 || value.LastLogSchema > 2 ||
		value.EvidenceEligible < 0 || value.EvidenceParseRejected < 0 || value.LastEvidenceParseRejectedAt < 0 ||
		value.EvidencePersistFailures < 0 || value.EvidenceDroppedEvents < 0 || value.LastEvidencePersistFailureAt < 0 ||
		(value.EvidencePersistFailures == 0) != (value.LastEvidencePersistFailureAt == 0) ||
		(value.LastAckedBatchID == "") != (value.LastAckedDevice == 0 && value.LastAckedInode == 0 && value.LastAckedOffset == 0) ||
		value.LastAckedBatchID != "" && (value.LastAckedDevice == 0 || value.LastAckedInode == 0 || value.LastAckedOffset < 0) {
		return cursor{}, fmt.Errorf("cursor is invalid; restore the persistent cursor volume before restarting")
	}
	return value, nil
}

func saveCursor(path string, value cursor) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	value.Version = cursorVersion
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	// rename 的目录项也要落盘，否则宿主机异常掉电可能丢失最新游标。
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = dir.Sync()
	if closeErr := dir.Close(); err == nil {
		err = closeErr
	}
	return err
}

// readBoundedLine 最多在内存保留 64 KiB。超长完整行会被安全跳过并推进游标；
// 文件尾尚未写完的半行不会推进游标，下一轮从同一位置重读。
func readBoundedLine(reader *bufio.Reader) (line []byte, digest [sha256.Size]byte, consumed int64, complete bool, err error) {
	tooLong := false
	hasher := sha256.New()
	for {
		fragment, readErr := reader.ReadSlice('\n')
		_, _ = hasher.Write(fragment)
		consumed += int64(len(fragment))
		if !tooLong && len(line)+len(fragment) <= 64<<10 {
			line = append(line, fragment...)
		} else {
			tooLong = true
			line = nil
		}
		switch {
		case readErr == nil:
			copy(digest[:], hasher.Sum(nil))
			return line, digest, consumed, true, nil
		case errors.Is(readErr, bufio.ErrBufferFull):
			continue
		case errors.Is(readErr, io.EOF):
			return nil, digest, 0, false, nil
		default:
			return nil, digest, 0, false, readErr
		}
	}
}

const maxLogCandidates = 256

type logCandidate struct {
	path    string
	info    os.FileInfo
	device  uint64
	inode   uint64
	current bool
}

func compressedLogName(name string) bool {
	name = strings.ToLower(name)
	for _, suffix := range []string{".gz", ".xz", ".bz2", ".zst", ".zip"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

// listLogCandidates 只扫描当前日志同目录、同文件名前缀的普通未压缩文件。
// 轮转文件按最后写入时间从旧到新排列，当前文件固定放在最后，因而即使采集器
// 跨过多次轮转才恢复，也会先追完仍保留的旧文件再读取当前文件。
func listLogCandidates(logPath string) ([]logCandidate, error) {
	currentInfo, err := os.Stat(logPath)
	if err != nil {
		return nil, err
	}
	if !currentInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("nginx log is not a regular file")
	}
	dir, base := filepath.Dir(logPath), filepath.Base(logPath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	currentInode := fileInode(currentInfo)
	rotated := make([]logCandidate, 0, 8)
	currentDevice := fileDevice(currentInfo)
	seen := map[string]bool{fmt.Sprintf("%d:%d", currentDevice, currentInode): true}
	for _, entry := range entries {
		name := entry.Name()
		if name == base || !strings.HasPrefix(name, base) || compressedLogName(name) {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		inode := fileInode(info)
		device := fileDevice(info)
		identity := fmt.Sprintf("%d:%d", device, inode)
		if seen[identity] {
			continue
		}
		seen[identity] = true
		rotated = append(rotated, logCandidate{path: filepath.Join(dir, name), info: info, device: device, inode: inode})
		if len(rotated) > maxLogCandidates {
			return nil, fmt.Errorf("too many rotated nginx log candidates")
		}
	}
	sort.Slice(rotated, func(i, j int) bool {
		left, right := rotated[i].info.ModTime(), rotated[j].info.ModTime()
		if !left.Equal(right) {
			return left.Before(right)
		}
		return rotated[i].path < rotated[j].path
	})
	return append(rotated, logCandidate{path: logPath, info: currentInfo, device: currentDevice, inode: currentInode, current: true}), nil
}

func markCursorDiscontinuity(value cursor, now int64) cursor {
	if value.Discontinuities < math.MaxInt64 {
		value.Discontinuities++
	}
	value.LastDiscontinuityAt = now
	return value
}

// selectLogCandidate 解析游标应继续读取的文件。只有游标 inode 已不存在、文件被
// copytruncate 到游标之前或轮转文件只剩不完整尾行时，才记录一次客观的“游标不连续”。
func selectLogCandidate(logPath string, value cursor, now int64) (logCandidate, cursor, error) {
	candidates, err := listLogCandidates(logPath)
	if err != nil {
		return logCandidate{}, value, err
	}
	if value.Inode == 0 {
		current := candidates[len(candidates)-1]
		value.Device, value.Inode, value.Offset = current.device, current.inode, 0
		return current, value, nil
	}
	for i, candidate := range candidates {
		if candidate.inode != value.Inode || value.Device != 0 && candidate.device != value.Device {
			continue
		}
		value.Device = candidate.device
		if candidate.info.Size() < value.Offset {
			value = markCursorDiscontinuity(value, now)
			value.Offset = 0
			return candidate, value, nil
		}
		if candidate.info.Size() > value.Offset || i == len(candidates)-1 {
			return candidate, value, nil
		}
		next := candidates[i+1]
		value.Device, value.Inode, value.Offset = next.device, next.inode, 0
		return next, value, nil
	}

	// 原 inode 已被删除或压缩，无法证明连续性。选择仍保留的最旧候选文件，
	// 尽量减少缺口；服务端 batch 幂等键会阻止相同分块被重复累计。
	value = markCursorDiscontinuity(value, now)
	oldest := candidates[0]
	value.Device, value.Inode, value.Offset = oldest.device, oldest.inode, 0
	return oldest, value, nil
}

func readBatch(c config, value cursor) (batch, cursor, bool, error) {
	original := value
	now := time.Now().Unix()
	target, value, err := selectLogCandidate(c.logPath, value, now)
	if err != nil {
		return batch{}, original, false, err
	}
	file, err := os.Open(target.path)
	if err != nil {
		return batch{}, original, false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return batch{}, original, false, err
	}
	inode := fileInode(info)
	device := fileDevice(info)
	if inode != target.inode || device != target.device {
		// 文件在目录扫描与 open 之间发生了轮转。此时原 inode 通常仍以
		// .1 等名称保留；返回短暂错误让下一轮重新扫描，才能继续追读旧文件。
		// 若直接从新 inode 开头读，会在轮转瞬间跳过旧文件尚未采集的尾部。
		return batch{}, original, false, fmt.Errorf("nginx log rotated during scan")
	}
	if _, err := file.Seek(value.Offset, io.SeekStart); err != nil {
		return batch{}, original, false, err
	}
	reader := bufio.NewReaderSize(file, 64<<10)
	start, end := value.Offset, value.Offset
	batchContent := sha256.New()
	aggregates := make(map[string]sample)
	var evidenceEvents []evidenceEvent
	if c.evidenceMode != "off" {
		evidenceEvents = make([]evidenceEvent, 0, c.maxLines)
	}
	var firstEventMS, lastEventMS int64
	completeLines := 0
	for completeLines < c.maxLines {
		line, lineDigest, consumed, complete, readErr := readBoundedLine(reader)
		if readErr != nil {
			return batch{}, value, false, readErr
		}
		if !complete {
			break
		}
		lineStart := end
		end += consumed
		_, _ = batchContent.Write(lineDigest[:])
		completeLines++
		trimmed := bytes.TrimSpace(line)
		if c.evidenceMode != "off" {
			var observed struct {
				LogSchema flexNumber `json:"log_schema"`
			}
			if json.Unmarshal(trimmed, &observed) == nil && (int(observed.LogSchema) == 1 || int(observed.LogSchema) == 2) {
				value.LastLogSchema = int(observed.LogSchema)
			}
		}
		if row, raw, ok := parseAccessLine(trimmed); ok && sampleWithinWindow(row, now, c.retentionDays) {
			key := sampleKey(row)
			if current, exists := aggregates[key]; exists {
				merge(&current, row)
				aggregates[key] = current
			} else {
				aggregates[key] = row
			}
			if c.evidenceMode != "off" && evidenceCandidate(trimmed, raw, row) {
				if value.EvidenceEligible < math.MaxInt64 {
					value.EvidenceEligible++
				}
				if event, eventOK := makeEvidenceEvent(c, trimmed, raw, row, inode, lineStart, lineDigest); eventOK {
					evidenceEvents = append(evidenceEvents, event)
					if firstEventMS == 0 || event.EventMS < firstEventMS {
						firstEventMS = event.EventMS
					}
					if event.EventMS > lastEventMS {
						lastEventMS = event.EventMS
					}
				} else {
					if value.EvidenceParseRejected < math.MaxInt64 {
						value.EvidenceParseRejected++
					}
					value.LastEvidenceParseRejectedAt = now
				}
			}
		} else {
			if value.DiscardedLines < math.MaxInt64 {
				value.DiscardedLines++
			}
			value.LastDiscardedAt = now
		}
	}
	if completeLines == 0 {
		// 已轮转的旧文件不会再补齐最后一条残行。丢弃这段不完整 JSON，并把
		// 游标切到下一候选文件；runOnce 会原子持久化这个纯游标变更。
		if !target.current && info.Size() > value.Offset {
			drained := markCursorDiscontinuity(value, now)
			drained.Offset = info.Size()
			_, next, selectErr := selectLogCandidate(c.logPath, drained, now)
			if selectErr != nil {
				return batch{}, original, false, selectErr
			}
			return batch{}, next, false, nil
		}
		return batch{}, value, false, nil
	}
	rows := make([]sample, 0, len(aggregates))
	for _, row := range aggregates {
		rows = append(rows, row)
	}
	identity := sha256.New()
	// Keep the v1 idempotency key byte-for-byte compatible with deployed
	// collectors. Device belongs to the acknowledged boundary, not BatchID.
	_, _ = fmt.Fprintf(identity, "%s:%d:%d:%d:", c.node, inode, start, end)
	_, _ = identity.Write(batchContent.Sum(nil))
	digest := identity.Sum(nil)
	next := cursor{Version: cursorVersion, Device: device, Inode: inode, Offset: end}
	next.Discontinuities = value.Discontinuities
	next.LastDiscontinuityAt = value.LastDiscontinuityAt
	next.DiscardedLines = value.DiscardedLines
	next.LastDiscardedAt = value.LastDiscardedAt
	next.LastLogSchema = value.LastLogSchema
	next.EvidenceEligible = value.EvidenceEligible
	next.EvidenceParseRejected = value.EvidenceParseRejected
	next.LastEvidenceParseRejectedAt = value.LastEvidenceParseRejectedAt
	next.EvidencePersistFailures = value.EvidencePersistFailures
	next.EvidenceDroppedEvents = value.EvidenceDroppedEvents
	next.LastEvidencePersistFailureAt = value.LastEvidencePersistFailureAt
	next.LastAckedBatchID, next.LastAckedDevice, next.LastAckedInode, next.LastAckedOffset = value.LastAckedBatchID, value.LastAckedDevice, value.LastAckedInode, value.LastAckedOffset
	batchID := hex.EncodeToString(digest)
	payload := batch{Node: c.node, BatchID: batchID, Samples: rows}
	if c.sourceV2Prepare {
		payload.SourceBoundary = &sourceBoundaryV1{Device: device, Inode: inode, StartOffset: start, EndOffset: end}
	}
	if c.evidenceMode != "off" {
		payload.Evidence = newEvidenceBatch(c, batchID, inode, start, end, firstEventMS, lastEventMS, evidenceEvents, next)
	}
	return payload, next, true, nil
}

func collectorTelemetry(c config, value cursor) (backlog int64, known bool) {
	candidates, err := listLogCandidates(c.logPath)
	if err != nil {
		return 0, false
	}
	start, found := len(candidates)-1, value.Inode == 0
	if value.Inode != 0 {
		for i, candidate := range candidates {
			if candidate.inode == value.Inode {
				start, found = i, true
				break
			}
		}
	}
	if !found {
		return 0, false
	}
	for i := start; i < len(candidates); i++ {
		remaining := candidates[i].info.Size()
		if i == start && candidates[i].inode == value.Inode {
			remaining -= value.Offset
			if remaining < 0 {
				return 0, false
			}
		}
		if backlog > math.MaxInt64-remaining {
			return 0, false
		}
		backlog += remaining
	}
	return backlog, true
}

func decorateBatch(c config, payload *batch, value cursor) {
	payload.CursorDiscontinuities = value.Discontinuities
	payload.LastCursorDiscontinuityAt = value.LastDiscontinuityAt
	payload.DiscardedLines = value.DiscardedLines
	payload.LastDiscardedAt = value.LastDiscardedAt
	payload.BacklogBytes, payload.BacklogKnown = collectorTelemetry(c, value)
}

func postBatch(ctx context.Context, c config, payload batch) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.sinkURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return fmt.Errorf("collector sink redirect refused")
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("monitor returned HTTP %d", resp.StatusCode)
	}
	if payload.SourceBoundary != nil {
		var response struct {
			OK                bool                 `json:"ok"`
			SourceBoundaryAck *sourceBoundaryAckV1 `json:"source_boundary_ack"`
		}
		if json.Unmarshal(data, &response) != nil || !response.OK || !validSourceBoundaryAckV1(response.SourceBoundaryAck, payload) {
			return errors.New("monitor did not acknowledge the exact source boundary")
		}
	}
	return nil
}

func accessInflightPathV1(c config) string {
	return c.cursorPath + ".inflight-v1.json"
}

func validAccessInflightV1(c config, pending accessInflightV1) bool {
	if pending.Version != accessInflightVersionV1 || pending.Payload.Node != c.node || len(pending.Payload.BatchID) != sha256.Size*2 || pending.Next.Inode == 0 || pending.Next.Offset < 0 {
		return false
	}
	if decoded, err := hex.DecodeString(pending.Payload.BatchID); err != nil || len(decoded) != sha256.Size {
		return false
	}
	switch pending.Stage {
	case accessInflightPrepared, accessInflightAcked, accessInflightQueued, accessInflightDropped:
	default:
		return false
	}
	if pending.Payload.SourceBoundary != nil {
		boundary := pending.Payload.SourceBoundary
		expectedStart := int64(0)
		if pending.Current.Inode != 0 && pending.Current.Inode == pending.Next.Inode {
			expectedStart = pending.Current.Offset
		}
		if boundary.Device != pending.Next.Device || boundary.Inode != pending.Next.Inode || boundary.StartOffset != expectedStart || boundary.EndOffset != pending.Next.Offset || boundary.StartOffset < 0 || boundary.StartOffset >= boundary.EndOffset {
			return false
		}
	}
	if pending.Evidence != nil {
		evidence := pending.Evidence
		expectedStart := int64(0)
		if pending.Current.Inode != 0 && pending.Current.Inode == pending.Next.Inode {
			expectedStart = pending.Current.Offset
		}
		if evidence.Node != c.node || evidence.BatchID != pending.Payload.BatchID || evidence.PayloadHash != evidencePayloadHash(*evidence) ||
			evidence.Source.Kind != "access" || evidence.Source.StartOffset != expectedStart || evidence.Source.EndOffset != pending.Next.Offset || evidence.Source.StartOffset < 0 || evidence.Source.StartOffset >= evidence.Source.EndOffset {
			return false
		}
		if evidence.Source.Protocol == 0 && evidence.Source.FileID != strconv.FormatUint(pending.Next.Inode, 10) {
			return false
		}
	}
	if pending.Stage == accessInflightDropped && pending.Next.EvidencePersistFailures == 0 {
		return false
	}
	return true
}

func loadAccessInflightV1(c config) (accessInflightV1, bool, error) {
	file, err := os.Open(accessInflightPathV1(c))
	if errors.Is(err, os.ErrNotExist) {
		return accessInflightV1{}, false, nil
	}
	if err != nil {
		return accessInflightV1{}, false, fmt.Errorf("read access inflight journal: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, accessInflightMaxBytes+1))
	if err != nil {
		return accessInflightV1{}, false, fmt.Errorf("read access inflight journal: %w", err)
	}
	if len(data) > accessInflightMaxBytes {
		return accessInflightV1{}, false, errors.New("access inflight journal is too large; restore the persistent cursor volume before restarting")
	}
	var pending accessInflightV1
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&pending); err != nil {
		return accessInflightV1{}, false, errors.New("access inflight journal is invalid; restore the persistent cursor volume before restarting")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) || !validAccessInflightV1(c, pending) {
		return accessInflightV1{}, false, errors.New("access inflight journal is invalid; restore the persistent cursor volume before restarting")
	}
	pending.Payload.Evidence = pending.Evidence
	return pending, true, nil
}

func saveAccessInflightV1(c config, pending accessInflightV1) error {
	pending.Version = accessInflightVersionV1
	pending.Payload.Evidence = nil
	if !validAccessInflightV1(c, pending) {
		return errors.New("refusing to persist invalid access inflight journal")
	}
	data, err := json.Marshal(pending)
	if err != nil {
		return err
	}
	return writeAtomic(accessInflightPathV1(c), data, 0o600)
}

func removeAccessInflightV1(c config) error {
	path := accessInflightPathV1(c)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func finishAccessInflightV1(ctx context.Context, c config, pending accessInflightV1) error {
	if pending.Stage == accessInflightPrepared {
		if err := postBatch(ctx, c, pending.Payload); err != nil {
			return err
		}
		if pending.Payload.SourceBoundary != nil {
			boundary := pending.Payload.SourceBoundary
			pending.Next.LastAckedBatchID, pending.Next.LastAckedDevice = pending.Payload.BatchID, boundary.Device
			pending.Next.LastAckedInode, pending.Next.LastAckedOffset = boundary.Inode, boundary.EndOffset
		} else {
			pending.Next.Device = 0
		}
		pending.Stage = accessInflightAcked
		if err := saveAccessInflightV1(c, pending); err != nil {
			return fmt.Errorf("persist acknowledged access inflight journal: %w", err)
		}
	}
	if pending.Stage == accessInflightAcked {
		if pending.Evidence == nil {
			pending.Stage = accessInflightQueued
		} else if err := spoolEvidence(c, *pending.Evidence); err != nil {
			if pending.Next.EvidencePersistFailures < math.MaxInt64 {
				pending.Next.EvidencePersistFailures++
			}
			pending.Next.LastEvidencePersistFailureAt = time.Now().Unix()
			if pending.Next.EvidenceDroppedEvents <= math.MaxInt64-int64(len(pending.Evidence.Events)) {
				pending.Next.EvidenceDroppedEvents += int64(len(pending.Evidence.Events))
			}
			pending.Stage = accessInflightDropped
			logEvidenceDeliveryError(fmt.Errorf("persist evidence outbox: %w", err))
		} else {
			pending.Stage = accessInflightQueued
		}
		if err := saveAccessInflightV1(c, pending); err != nil {
			return fmt.Errorf("persist finalized access inflight journal: %w", err)
		}
	}
	if pending.Stage != accessInflightQueued && pending.Stage != accessInflightDropped {
		return errors.New("access inflight journal did not reach a final stage")
	}
	// The cursor and evidence outbox are separate persistent volumes. Re-spool a
	// finalized queued batch before committing/confirming the cursor so a
	// point-in-time restore of only the cursor volume cannot silently lose it.
	// spoolEvidence is idempotent by BatchID and payload hash; if the worker had
	// already delivered and removed the file, the Monitor ledger deduplicates the
	// replay using the same identity.
	if pending.Stage == accessInflightQueued && pending.Evidence != nil {
		if err := spoolEvidence(c, *pending.Evidence); err != nil {
			return fmt.Errorf("reconfirm queued evidence before cursor commit: %w", err)
		}
	}
	if err := saveCursor(c.cursorPath, pending.Next); err != nil {
		return err
	}
	return removeAccessInflightV1(c)
}

func runOnce(ctx context.Context, c config) error {
	current, err := loadCursor(c.cursorPath)
	if err != nil {
		return err
	}
	if pending, exists, err := loadAccessInflightV1(c); err != nil {
		return err
	} else if exists {
		if current == pending.Next && (pending.Stage == accessInflightQueued || pending.Stage == accessInflightDropped) {
			return finishAccessInflightV1(ctx, c, pending)
		}
		if current != pending.Current {
			return errors.New("access cursor does not match inflight journal; restore the persistent cursor volume before restarting")
		}
		return finishAccessInflightV1(ctx, c, pending)
	}
	payload, next, ok, err := readBatch(c, current)
	if err != nil {
		return err
	}
	if !ok {
		if !c.sourceV2Prepare {
			next.Device = 0
		}
		if next != current {
			return saveCursor(c.cursorPath, next)
		}
		return nil
	}
	decorateBatch(c, &payload, next)
	pending := accessInflightV1{Version: accessInflightVersionV1, Stage: accessInflightPrepared, Current: current, Next: next, Payload: payload, Evidence: payload.Evidence}
	if err := saveAccessInflightV1(c, pending); err != nil {
		return fmt.Errorf("persist access inflight journal before POST: %w", err)
	}
	pending.Payload.Evidence = pending.Evidence
	return finishAccessInflightV1(ctx, c, pending)
}

// runLegacyHeartbeatOnce never sends or persists a legacy heartbeat while an
// access journal exists. In prepare mode a heartbeat carries a source boundary;
// changing LastAcked* beside an unfinished frozen range would make the next
// recovery fail its exact cursor/journal match.
func runLegacyHeartbeatOnce(ctx context.Context, c config, now time.Time) (bool, error) {
	if _, err := os.Stat(accessInflightPathV1(c)); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("inspect access inflight journal before heartbeat: %w", err)
	}
	current, err := loadCursor(c.cursorPath)
	if err != nil {
		return false, err
	}
	payload := heartbeatBatch(c, now, current)
	if err := postBatch(ctx, c, payload); err != nil {
		return false, err
	}
	if payload.SourceBoundary != nil {
		current.LastAckedBatchID, current.LastAckedDevice, current.LastAckedInode, current.LastAckedOffset = payload.BatchID, payload.SourceBoundary.Device, payload.SourceBoundary.Inode, payload.SourceBoundary.EndOffset
		if err := saveCursor(c.cursorPath, current); err != nil {
			return false, fmt.Errorf("persist heartbeat boundary: %w", err)
		}
	}
	return true, nil
}

func heartbeatBatch(c config, now time.Time, value cursor) batch {
	minute := now.Unix() / 60
	identity := fmt.Sprintf("heartbeat:%s:%d", c.node, minute)
	if c.sourceV2Prepare {
		identity = fmt.Sprintf("heartbeat:%s:%d:%d:%d:%d", c.node, minute, value.Device, value.Inode, value.Offset)
	}
	digest := sha256.Sum256([]byte(identity))
	payload := batch{Node: c.node, BatchID: "hb_" + hex.EncodeToString(digest[:16]), Samples: []sample{}}
	if c.sourceV2Prepare && value.Inode != 0 {
		payload.SourceBoundary = &sourceBoundaryV1{Device: value.Device, Inode: value.Inode, StartOffset: value.Offset, EndOffset: value.Offset, Checkpoint: true}
	}
	decorateBatch(c, &payload, value)
	return payload
}

func main() {
	c, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	var sourceV2Client *sourceV2HTTPClient
	if c.sourceV2Access || c.sourceV2Error {
		sourceV2Client, err = newSourceV2HTTPClient(c.sourceV2BaseURL, c.token, c.allowHTTP)
		if err != nil {
			log.Fatal(err)
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	if c.evidenceMode != "off" {
		go runEvidenceWorker(ctx, c)
	}
	if c.errorEnabled {
		if c.sourceV2Error {
			go runErrorSourceV2Worker(ctx, c, sourceV2Client)
		} else {
			go runErrorWorker(ctx, c)
		}
	}
	var nextHeartbeat time.Time
	for {
		var runErr error
		if c.sourceV2Access {
			runErr = runAccessSourceV2Once(ctx, c, c.sourceV2Cursor, sourceV2Client)
		} else {
			runErr = runOnce(ctx, c)
		}
		if runErr != nil && !errors.Is(runErr, os.ErrNotExist) && ctx.Err() == nil {
			log.Printf("nginxcollector: 本轮未推进游标，将自动重试: %v", runErr)
		}
		now := time.Now()
		if !now.Before(nextHeartbeat) && ctx.Err() == nil {
			if c.sourceV2Access {
				if err := runSourceV2Heartbeat(ctx, c, c.logPath, c.cursorPath, c.sourceV2Cursor, "access", sourceV2Client); err != nil {
					log.Printf("nginxcollector: source v2 心跳失败，将自动重试: %v", err)
				} else {
					nextHeartbeat = now.Add(time.Minute)
				}
			} else {
				sent, err := runLegacyHeartbeatOnce(ctx, c, now)
				if err != nil {
					log.Printf("nginxcollector: 心跳未发送或未持久化，将自动重试: %v", err)
				} else if sent {
					nextHeartbeat = now.Add(time.Minute)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
