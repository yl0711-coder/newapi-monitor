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
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type config struct {
	node       string
	logPath    string
	cursorPath string
	sinkURL    string
	token      string
	interval   time.Duration
	maxLines   int
}

func loadConfig() (config, error) {
	c := config{
		node: strings.TrimSpace(os.Getenv("NGINXCOLLECTOR_NODE")), logPath: env("NGINXCOLLECTOR_LOG_PATH", "/logs/nexusapi_access.jsonl"),
		cursorPath: env("NGINXCOLLECTOR_CURSOR_PATH", "/data/cursor.json"), sinkURL: strings.TrimSpace(os.Getenv("NGINXCOLLECTOR_SINK_URL")),
		token: os.Getenv("NGINXCOLLECTOR_TOKEN"), interval: time.Duration(envInt("NGINXCOLLECTOR_INTERVAL_SECONDS", 5)) * time.Second,
		maxLines: envInt("NGINXCOLLECTOR_MAX_LINES", 1000),
	}
	if c.node == "" || c.sinkURL == "" || c.token == "" {
		return config{}, fmt.Errorf("NGINXCOLLECTOR_NODE, NGINXCOLLECTOR_SINK_URL and NGINXCOLLECTOR_TOKEN are required")
	}
	if c.interval < time.Second {
		c.interval = time.Second
	}
	if c.maxLines < 1 || c.maxLines > 2000 {
		c.maxLines = 1000
	}
	return c, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return value
}

type cursor struct {
	Inode  uint64 `json:"inode"`
	Offset int64  `json:"offset"`
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
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil
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

type rawLine struct {
	Timestamp      flexNumber `json:"ts"`
	Msec           flexNumber `json:"msec"`
	Method         string     `json:"method"`
	RequestMethod  string     `json:"request_method"`
	URI            string     `json:"uri"`
	Path           string     `json:"path"`
	Status         flexNumber `json:"status"`
	RequestTime    flexNumber `json:"request_time"`
	UpstreamStatus flexNumber `json:"upstream_status"`
	UpstreamTime   flexNumber `json:"upstream_response_time"`
	BytesSent      flexNumber `json:"bytes_sent"`
	RequestID      string     `json:"request_id"`
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
}

type batch struct {
	Node    string   `json:"node"`
	BatchID string   `json:"batch_id"`
	Samples []sample `json:"samples"`
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

func parseLine(data []byte) (sample, bool) {
	var raw rawLine
	if len(data) > 64<<10 || json.Unmarshal(data, &raw) != nil {
		return sample{}, false
	}
	ts := float64(raw.Timestamp)
	if ts <= 0 {
		ts = float64(raw.Msec)
	}
	status := int(raw.Status)
	if ts <= 0 || status < 100 || status > 599 {
		return sample{}, false
	}
	method := raw.Method
	if method == "" {
		method = raw.RequestMethod
	}
	path := raw.Path
	if path == "" {
		path = raw.URI
	}
	requestMS := int64(math.Round(float64(raw.RequestTime) * 1000))
	upstreamMS := int64(math.Round(float64(raw.UpstreamTime) * 1000))
	if requestMS < 0 || upstreamMS < 0 {
		return sample{}, false
	}
	out := sample{BucketTs: int64(ts) / 60 * 60, Route: normalizeRoute(path), Method: normalizeMethod(method), Status: status,
		UpstreamStatus: int(raw.UpstreamStatus), Count: 1, RequestTimeSumMS: requestMS, RequestTimeMaxMS: requestMS,
		BytesSent: int64(math.Max(0, float64(raw.BytesSent)))}
	if upstreamMS > 0 {
		out.UpstreamTimeSumMS, out.UpstreamTimeCount = upstreamMS, 1
	}
	if strings.TrimSpace(raw.RequestID) != "" && strings.TrimSpace(raw.RequestID) != "-" {
		out.RequestIDPresent = 1
	}
	return out, true
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
}

func fileInode(info os.FileInfo) uint64 {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return stat.Ino
	}
	return uint64(info.ModTime().UnixNano())
}

func loadCursor(path string) cursor {
	data, err := os.ReadFile(path)
	if err != nil {
		return cursor{}
	}
	var value cursor
	if json.Unmarshal(data, &value) != nil || value.Offset < 0 {
		return cursor{}
	}
	return value
}

func saveCursor(path string, value cursor) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// readBoundedLine 最多在内存保留 64 KiB。超长完整行会被安全跳过并推进游标；
// 文件尾尚未写完的半行不会推进游标，下一轮从同一位置重读。
func readBoundedLine(reader *bufio.Reader) (line []byte, consumed int64, complete bool, err error) {
	tooLong := false
	for {
		fragment, readErr := reader.ReadSlice('\n')
		consumed += int64(len(fragment))
		if !tooLong && len(line)+len(fragment) <= 64<<10 {
			line = append(line, fragment...)
		} else {
			tooLong = true
			line = nil
		}
		switch {
		case readErr == nil:
			return line, consumed, true, nil
		case errors.Is(readErr, bufio.ErrBufferFull):
			continue
		case errors.Is(readErr, io.EOF):
			return nil, 0, false, nil
		default:
			return nil, 0, false, readErr
		}
	}
}

func readBatch(c config, value cursor) (batch, cursor, bool, error) {
	info, err := os.Stat(c.logPath)
	if err != nil {
		return batch{}, value, false, err
	}
	inode := fileInode(info)
	if value.Inode != inode || info.Size() < value.Offset {
		value = cursor{Inode: inode}
	}
	file, err := os.Open(c.logPath)
	if err != nil {
		return batch{}, value, false, err
	}
	defer file.Close()
	if _, err := file.Seek(value.Offset, io.SeekStart); err != nil {
		return batch{}, value, false, err
	}
	reader := bufio.NewReaderSize(file, 64<<10)
	start, end := value.Offset, value.Offset
	aggregates := make(map[string]sample)
	completeLines := 0
	for completeLines < c.maxLines {
		line, consumed, complete, readErr := readBoundedLine(reader)
		if readErr != nil {
			return batch{}, value, false, readErr
		}
		if !complete {
			break
		}
		end += consumed
		completeLines++
		if row, ok := parseLine(bytes.TrimSpace(line)); ok {
			key := sampleKey(row)
			if current, exists := aggregates[key]; exists {
				merge(&current, row)
				aggregates[key] = current
			} else {
				aggregates[key] = row
			}
		}
	}
	if completeLines == 0 {
		return batch{}, value, false, nil
	}
	rows := make([]sample, 0, len(aggregates))
	for _, row := range aggregates {
		rows = append(rows, row)
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d:%d", c.node, inode, start, end)))
	next := cursor{Inode: inode, Offset: end}
	return batch{Node: c.node, BatchID: hex.EncodeToString(digest[:]), Samples: rows}, next, true, nil
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
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("monitor returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func runOnce(ctx context.Context, c config) error {
	current := loadCursor(c.cursorPath)
	payload, next, ok, err := readBatch(c, current)
	if err != nil || !ok {
		return err
	}
	if err := postBatch(ctx, c, payload); err != nil {
		return err
	}
	return saveCursor(c.cursorPath, next)
}

func heartbeatBatch(node string, now time.Time) batch {
	minute := now.Unix() / 60
	digest := sha256.Sum256([]byte(fmt.Sprintf("heartbeat:%s:%d", node, minute)))
	return batch{Node: node, BatchID: "hb_" + hex.EncodeToString(digest[:16]), Samples: []sample{}}
}

func main() {
	c, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	var nextHeartbeat time.Time
	for {
		if err := runOnce(ctx, c); err != nil && !errors.Is(err, os.ErrNotExist) && ctx.Err() == nil {
			log.Printf("nginxcollector: 本轮未推进游标，将自动重试: %v", err)
		}
		if now := time.Now(); !now.Before(nextHeartbeat) && ctx.Err() == nil {
			if err := postBatch(ctx, c, heartbeatBatch(c.node, now)); err != nil {
				log.Printf("nginxcollector: 心跳发送失败，将自动重试: %v", err)
			} else {
				nextHeartbeat = now.Add(time.Minute)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
