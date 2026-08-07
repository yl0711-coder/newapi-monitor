package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func fixtureLine(ts int64, uri, status string) string {
	return fmt.Sprintf(`{"msec":"%d.250","request_method":"POST","uri":"%s","status":"%s","request_time":"0.250","upstream_status":"%s","upstream_response_time":"0.200","bytes_sent":"123","request_id":"secret-request-id"}`, ts, uri, status, status)
}

func TestParseLineKeepsOnlySanitizedAggregate(t *testing.T) {
	row, ok := parseLine([]byte(fixtureLine(time.Now().Unix(), "/api/user/123?token=secret", "502")))
	if !ok {
		t.Fatal("合法 JSON 应解析")
	}
	if row.Route != "/api/*" || row.Method != "POST" || row.Status != 502 || row.RequestIDPresent != 1 {
		t.Fatalf("聚合字段错误: %+v", row)
	}
	if strings.Contains(fmt.Sprintf("%+v", row), "secret") || strings.Contains(row.Route, "123") || strings.Contains(row.Route, "?") {
		t.Fatalf("动态路径/query/Request ID 原值不应进入样本: %+v", row)
	}
}

func TestReadBatchDoesNotAdvancePartialLine(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	now := time.Now().Unix()
	first, second := fixtureLine(now, "/v1/responses", "200"), fixtureLine(now, "/v1/chat/completions", "200")
	partial := second[:len(second)/2]
	if err := os.WriteFile(path, []byte(first+"\n"+partial), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config{node: "local", logPath: path, cursorPath: filepath.Join(dir, "cursor.json"), maxLines: 100}
	payload, next, ok, err := readBatch(cfg, cursor{})
	if err != nil || !ok || len(payload.Samples) != 1 || next.Offset != int64(len(first)+1) {
		t.Fatalf("首轮应只推进完整行: samples=%d next=%d ok=%v err=%v", len(payload.Samples), next.Offset, ok, err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.WriteString(second[len(partial):] + "\n")
	file.Close()
	if err != nil {
		t.Fatal(err)
	}
	payload, final, ok, err := readBatch(cfg, next)
	if err != nil || !ok || len(payload.Samples) != 1 || final.Offset <= next.Offset {
		t.Fatalf("补齐后应读取第二行: samples=%d next=%d ok=%v err=%v", len(payload.Samples), final.Offset, ok, err)
	}
}

func TestRunOnceOnlyAdvancesCursorAfterAcceptedBatch(t *testing.T) {
	dir := t.TempDir()
	logPath, cursorPath := filepath.Join(dir, "access.jsonl"), filepath.Join(dir, "cursor.json")
	line := fixtureLine(time.Now().Unix(), "/v1/responses", "200") + "\n"
	if err := os.WriteFile(logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	var status atomic.Int64
	status.Store(http.StatusServiceUnavailable)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Errorf("token header missing")
		}
		w.WriteHeader(int(status.Load()))
	}))
	defer server.Close()
	cfg := config{node: "local", logPath: logPath, cursorPath: cursorPath, sinkURL: server.URL, token: "secret", maxLines: 100}
	if err := runOnce(context.Background(), cfg); err == nil {
		t.Fatal("接收失败应返回错误")
	}
	if got := loadCursor(cursorPath); got.Offset != 0 {
		t.Fatalf("失败后不应推进游标: %+v", got)
	}
	status.Store(http.StatusOK)
	if err := runOnce(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if got := loadCursor(cursorPath); got.Offset != int64(len(line)) {
		t.Fatalf("成功后应推进到文件尾: %+v", got)
	}
}

func TestReadBatchResetsOnRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	old := fixtureLine(time.Now().Unix(), "/v1/responses", "200") + "\n"
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(path)
	cur := cursor{Inode: fileInode(info), Offset: int64(len(old))}
	rotated := filepath.Join(dir, "new.jsonl")
	newLine := fixtureLine(time.Now().Unix(), "/api/status", "200") + "\n"
	if err := os.WriteFile(rotated, []byte(newLine), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(rotated, path); err != nil {
		t.Fatal(err)
	}
	cfg := config{node: "local", logPath: path, maxLines: 100}
	payload, next, ok, err := readBatch(cfg, cur)
	if err != nil || !ok || len(payload.Samples) != 1 || next.Offset != int64(len(newLine)) {
		t.Fatalf("轮转后应从新文件开头读取: samples=%d next=%+v ok=%v err=%v", len(payload.Samples), next, ok, err)
	}
}
