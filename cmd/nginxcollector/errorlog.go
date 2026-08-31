package main

// errorlog.go is an independent lane for the standard Nginx error log. Raw
// lines never leave the node: only a finite category, severity, minute and
// count are sent to Monitor. Its cursor and delivery loop cannot block access
// metrics or request evidence.

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
	"net/http"
	"os"
	"strings"
	"time"
	_ "time/tzdata"
)

type errorSample struct {
	BucketTs int64  `json:"bucket_ts"`
	Category string `json:"category"`
	Severity string `json:"severity"`
	Count    int64  `json:"count"`
}

type errorBatch struct {
	Node                      string            `json:"node"`
	BatchID                   string            `json:"batch_id"`
	Samples                   []errorSample     `json:"samples"`
	BacklogBytes              int64             `json:"backlog_bytes"`
	BacklogKnown              bool              `json:"backlog_known"`
	CursorDiscontinuities     int64             `json:"cursor_discontinuities"`
	LastCursorDiscontinuityAt int64             `json:"last_cursor_discontinuity_at"`
	DiscardedLines            int64             `json:"discarded_lines"`
	LastDiscardedAt           int64             `json:"last_discarded_at"`
	SourceBoundary            *sourceBoundaryV1 `json:"source_boundary,omitempty"`
	SourceRangeV2             *sourceRangeV2    `json:"source_range_v2,omitempty"`
}

var allowedErrorSeverities = map[string]bool{
	"emerg": true, "alert": true, "crit": true, "error": true, "warn": true, "notice": true,
}

func classifyNginxError(message string) string {
	text := strings.ToLower(message)
	switch {
	case strings.Contains(text, "upstream timed out") || strings.Contains(text, "upstream timeout"):
		return "upstream_timeout"
	case strings.Contains(text, "connect() failed") && strings.Contains(text, "upstream"),
		strings.Contains(text, "no live upstreams"):
		return "upstream_connect_failed"
	case strings.Contains(text, "upstream prematurely closed connection"),
		strings.Contains(text, "upstream sent no valid"):
		return "upstream_closed"
	case strings.Contains(text, "upstream") && (strings.Contains(text, "ssl_do_handshake() failed") || strings.Contains(text, "certificate verify failed")):
		return "upstream_tls"
	case strings.Contains(text, "client prematurely closed connection"),
		strings.Contains(text, "client closed connection"):
		return "client_closed"
	case strings.Contains(text, "worker_connections are not enough"),
		strings.Contains(text, "too many open files"), strings.Contains(text, "cannot allocate memory"):
		return "worker_capacity"
	case strings.Contains(text, "host not found in upstream"), strings.Contains(text, "could not be resolved"),
		strings.Contains(text, "resolver") && strings.Contains(text, "failed"):
		return "resolver"
	case strings.Contains(text, "limiting requests"), strings.Contains(text, "limiting connections"):
		return "rate_limited"
	case strings.Contains(text, "client intended to send too large body"), strings.Contains(text, "client timed out") && strings.Contains(text, "request body"):
		return "request_body"
	default:
		return "other_error"
	}
}

func parseNginxErrorLine(line []byte, location *time.Location) (errorSample, bool) {
	// Default Nginx error format begins with: 2006/01/02 15:04:05 [error]
	if len(line) > 64<<10 || len(line) < 28 || location == nil {
		return errorSample{}, false
	}
	stamp, err := time.ParseInLocation("2006/01/02 15:04:05", string(line[:19]), location)
	if err != nil {
		return errorSample{}, false
	}
	rest := string(line[19:])
	open, close := strings.Index(rest, "["), strings.Index(rest, "]")
	if open < 0 || close <= open {
		return errorSample{}, false
	}
	severity := strings.ToLower(strings.TrimSpace(rest[open+1 : close]))
	if severity == "info" || severity == "debug" {
		// Ordinary connection lifecycle messages are intentionally outside the
		// error metric. They are valid input, not collector data loss.
		return errorSample{}, true
	}
	if !allowedErrorSeverities[severity] {
		return errorSample{}, false
	}
	return errorSample{BucketTs: stamp.Unix() / 60 * 60, Category: classifyNginxError(rest[close+1:]), Severity: severity, Count: 1}, true
}

func errorSampleKey(row errorSample) string {
	return fmt.Sprintf("%d\x00%s\x00%s", row.BucketTs, row.Category, row.Severity)
}

func readErrorBatch(c config, value cursor) (errorBatch, cursor, bool, error) {
	original, now := value, time.Now().Unix()
	target, value, err := selectLogCandidate(c.errorLogPath, value, now)
	if err != nil {
		return errorBatch{}, original, false, err
	}
	file, err := os.Open(target.path)
	if err != nil {
		return errorBatch{}, original, false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || fileDevice(info) != target.device || fileInode(info) != target.inode {
		return errorBatch{}, original, false, fmt.Errorf("nginx error log rotated during scan")
	}
	if _, err := file.Seek(value.Offset, io.SeekStart); err != nil {
		return errorBatch{}, original, false, err
	}
	location, err := time.LoadLocation(c.errorTimezone)
	if err != nil {
		return errorBatch{}, original, false, err
	}
	reader := bufio.NewReaderSize(file, 64<<10)
	start, end, completeLines := value.Offset, value.Offset, 0
	content := sha256.New()
	aggregates := make(map[string]errorSample)
	for completeLines < c.maxLines {
		line, digest, consumed, complete, readErr := readBoundedLine(reader)
		if readErr != nil {
			return errorBatch{}, original, false, readErr
		}
		if !complete {
			break
		}
		end += consumed
		completeLines++
		_, _ = content.Write(digest[:])
		row, ok := parseNginxErrorLine(bytes.TrimSpace(line), location)
		if !ok || row.Category != "" && !sampleWithinWindow(sample{BucketTs: row.BucketTs}, now, c.retentionDays) {
			value.DiscardedLines++
			value.LastDiscardedAt = now
			continue
		}
		if row.Category == "" {
			continue
		}
		key := errorSampleKey(row)
		if current, exists := aggregates[key]; exists {
			current.Count++
			aggregates[key] = current
		} else {
			aggregates[key] = row
		}
	}
	if completeLines == 0 {
		if !target.current && info.Size() > value.Offset {
			drained := markCursorDiscontinuity(value, now)
			drained.Offset = info.Size()
			_, next, selectErr := selectLogCandidate(c.errorLogPath, drained, now)
			if selectErr != nil {
				return errorBatch{}, original, false, selectErr
			}
			return errorBatch{}, next, false, nil
		}
		return errorBatch{}, value, false, nil
	}
	rows := make([]errorSample, 0, len(aggregates))
	for _, row := range aggregates {
		rows = append(rows, row)
	}
	identity := sha256.New()
	// Preserve the deployed v1 BatchID algorithm across collector upgrades.
	_, _ = fmt.Fprintf(identity, "error:%s:%d:%d:%d:", c.node, target.inode, start, end)
	_, _ = identity.Write(content.Sum(nil))
	next := value
	next.Version, next.Device, next.Inode, next.Offset = cursorVersion, target.device, target.inode, end
	payload := errorBatch{Node: c.node, BatchID: hex.EncodeToString(identity.Sum(nil)), Samples: rows}
	if c.sourceV2Prepare {
		payload.SourceBoundary = &sourceBoundaryV1{Device: target.device, Inode: target.inode, StartOffset: start, EndOffset: end}
	}
	payload.CursorDiscontinuities, payload.LastCursorDiscontinuityAt = next.Discontinuities, next.LastDiscontinuityAt
	payload.DiscardedLines, payload.LastDiscardedAt = next.DiscardedLines, next.LastDiscardedAt
	payload.BacklogBytes, payload.BacklogKnown = collectorTelemetry(config{logPath: c.errorLogPath}, next)
	return payload, next, true, nil
}

func postErrorBatch(ctx context.Context, c config, payload errorBatch) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.errorSinkURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return errors.New("collector error sink redirect refused")
	}}
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
		return fmt.Errorf("monitor error sink returned HTTP %d", resp.StatusCode)
	}
	if payload.SourceBoundary != nil {
		var response struct {
			OK                bool                 `json:"ok"`
			SourceBoundaryAck *sourceBoundaryAckV1 `json:"source_boundary_ack"`
		}
		if json.Unmarshal(data, &response) != nil || !response.OK || !validSourceBoundaryAckV1(response.SourceBoundaryAck, batch{Node: payload.Node, BatchID: payload.BatchID, SourceBoundary: payload.SourceBoundary}) {
			return errors.New("monitor error sink did not acknowledge the exact source boundary")
		}
	}
	return nil
}

func runErrorOnce(ctx context.Context, c config) error {
	current, err := loadCursor(c.errorCursorPath)
	if err != nil {
		return err
	}
	payload, next, ok, err := readErrorBatch(c, current)
	if err != nil {
		return err
	}
	if !ok {
		if !c.sourceV2Prepare {
			next.Device = 0
		}
		if next != current {
			return saveCursor(c.errorCursorPath, next)
		}
		return nil
	}
	if err := postErrorBatch(ctx, c, payload); err != nil {
		return err
	}
	if payload.SourceBoundary != nil {
		next.LastAckedBatchID, next.LastAckedDevice, next.LastAckedInode, next.LastAckedOffset = payload.BatchID, payload.SourceBoundary.Device, payload.SourceBoundary.Inode, payload.SourceBoundary.EndOffset
	} else {
		next.Device = 0
	}
	return saveCursor(c.errorCursorPath, next)
}

func errorHeartbeatBatch(c config, now time.Time, value cursor) errorBatch {
	identity := fmt.Sprintf("error-heartbeat:%s:%d", c.node, now.Unix()/60)
	if c.sourceV2Prepare {
		identity = fmt.Sprintf("error-heartbeat:%s:%d:%d:%d:%d", c.node, now.Unix()/60, value.Device, value.Inode, value.Offset)
	}
	digest := sha256.Sum256([]byte(identity))
	payload := errorBatch{Node: c.node, BatchID: "ehb_" + hex.EncodeToString(digest[:16]), Samples: []errorSample{}}
	if c.sourceV2Prepare && value.Inode != 0 {
		payload.SourceBoundary = &sourceBoundaryV1{Device: value.Device, Inode: value.Inode, StartOffset: value.Offset, EndOffset: value.Offset, Checkpoint: true}
	}
	payload.CursorDiscontinuities, payload.LastCursorDiscontinuityAt = value.Discontinuities, value.LastDiscontinuityAt
	payload.DiscardedLines, payload.LastDiscardedAt = value.DiscardedLines, value.LastDiscardedAt
	payload.BacklogBytes, payload.BacklogKnown = collectorTelemetry(config{logPath: c.errorLogPath}, value)
	return payload
}

func runErrorWorker(ctx context.Context, c config) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	var nextHeartbeat time.Time
	for {
		if err := runErrorOnce(ctx, c); err != nil && !errors.Is(err, os.ErrNotExist) && ctx.Err() == nil {
			log.Printf("nginxcollector error lane: 本轮未推进独立游标，将自动重试: %v", err)
		}
		now := time.Now()
		if !now.Before(nextHeartbeat) && ctx.Err() == nil {
			current, cursorErr := loadCursor(c.errorCursorPath)
			if cursorErr != nil {
				log.Printf("nginxcollector error lane: 独立游标无法安全读取，已停止心跳: %v", cursorErr)
			} else {
				payload := errorHeartbeatBatch(c, now, current)
				if err := postErrorBatch(ctx, c, payload); err != nil {
					log.Printf("nginxcollector error lane: 心跳发送失败，将自动重试: %v", err)
				} else {
					if payload.SourceBoundary != nil {
						current.LastAckedBatchID, current.LastAckedDevice, current.LastAckedInode, current.LastAckedOffset = payload.BatchID, payload.SourceBoundary.Device, payload.SourceBoundary.Inode, payload.SourceBoundary.EndOffset
						if err := saveCursor(c.errorCursorPath, current); err != nil {
							log.Printf("nginxcollector error lane: 心跳边界持久化失败，将重发: %v", err)
							continue
						}
					}
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

func runErrorSourceV2Worker(ctx context.Context, c config, client *sourceV2HTTPClient) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	var nextHeartbeat time.Time
	for {
		if err := runErrorSourceV2Once(ctx, c, c.errorV2Cursor, client); err != nil && !errors.Is(err, os.ErrNotExist) && ctx.Err() == nil {
			log.Printf("nginxcollector error lane: source v2 本轮未推进，将自动重试: %v", err)
		}
		now := time.Now()
		if !now.Before(nextHeartbeat) && ctx.Err() == nil {
			if err := runSourceV2Heartbeat(ctx, c, c.errorLogPath, c.errorCursorPath, c.errorV2Cursor, "error", client); err != nil {
				log.Printf("nginxcollector error lane: source v2 心跳失败，将自动重试: %v", err)
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
