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
	if row.LatencyCount != 1 || row.Latency0To1s != 1 || row.Latency1To5s+row.Latency5To15s+row.Latency15To30s+row.Latency30To60s+row.LatencyOver60s != 0 {
		t.Fatalf("延迟直方图必须且只能命中一个桶: %+v", row)
	}
}

func TestParseLineUsesFinalUpstreamAttemptForCommaAndColonSequences(t *testing.T) {
	now := time.Now().Unix()
	for _, tc := range []struct {
		name, statuses, times string
	}{
		{name: "retry", statuses: "502, 200", times: "0.050, 0.250"},
		{name: "internal redirect", statuses: "502 : 200", times: "0.050 : 0.300"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			line := fmt.Sprintf(`{"msec":"%d.250","request_method":"POST","uri":"/v1/responses","status":"200","request_time":"0.350","upstream_status":"%s","upstream_response_time":"%s","bytes_sent":"123","request_id":"id"}`,
				now, tc.statuses, tc.times)
			row, ok := parseLine([]byte(line))
			if !ok {
				t.Fatal("合法的多 upstream 序列不应被丢弃")
			}
			if row.Status != 200 || row.UpstreamStatus != 200 || row.UpstreamTimeCount != 1 {
				t.Fatalf("应保留客户最终状态和最后一次 upstream 状态: %+v", row)
			}
			wantMS := int64(250)
			if tc.name == "internal redirect" {
				wantMS = 300
			}
			if row.UpstreamTimeSumMS != wantMS {
				t.Fatalf("最后一次 upstream 耗时=%d want=%d", row.UpstreamTimeSumMS, wantMS)
			}
		})
	}
}

func TestParseLineDistinguishesZeroUpstreamTimeFromNoUpstream(t *testing.T) {
	now := time.Now().Unix()
	withUpstream := fmt.Sprintf(`{"msec":"%d.250","request_method":"GET","uri":"/api/status","status":"200","request_time":"0.000","upstream_status":"200","upstream_response_time":"0.000","bytes_sent":"1","request_id":"id"}`, now)
	row, ok := parseLine([]byte(withUpstream))
	if !ok || row.UpstreamTimeCount != 1 || row.UpstreamTimeSumMS != 0 {
		t.Fatalf("0.000 是有效 upstream 耗时，不能当成无 upstream: %+v ok=%v", row, ok)
	}
	withoutUpstream := strings.Replace(withUpstream, `"upstream_status":"200"`, `"upstream_status":"-"`, 1)
	withoutUpstream = strings.Replace(withoutUpstream, `"upstream_response_time":"0.000"`, `"upstream_response_time":"-"`, 1)
	row, ok = parseLine([]byte(withoutUpstream))
	if !ok || row.UpstreamStatus != 0 || row.UpstreamTimeCount != 0 {
		t.Fatalf("'-' 应明确表示无 upstream: %+v ok=%v", row, ok)
	}
}

func TestParseLineRejectsMalformedUpstreamSequence(t *testing.T) {
	line := fixtureLine(time.Now().Unix(), "/v1/responses", "200")
	line = strings.Replace(line, `"upstream_status":"200"`, `"upstream_status":"502 : invalid"`, 1)
	if _, ok := parseLine([]byte(line)); ok {
		t.Fatal("非法 upstream 序列必须拒绝，不能猜测或静默改成 0")
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
	if got, err := loadCursor(cursorPath); err != nil || got.Offset != 0 {
		t.Fatalf("失败后不应推进游标: %+v", got)
	}
	status.Store(http.StatusOK)
	if err := runOnce(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if got, err := loadCursor(cursorPath); err != nil || got.Offset != int64(len(line)) {
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
	if next.Discontinuities != 1 {
		t.Fatalf("旧 inode 已消失时应明确记录游标不连续: %+v", next)
	}
}

func TestReadBatchDrainsRenamedOldBeforeCurrent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	now := time.Now().Unix()
	first := fixtureLine(now, "/v1/responses", "200") + "\n"
	tail := fixtureLine(now, "/v1/chat/completions", "502") + "\n"
	if err := os.WriteFile(path, []byte(first+tail), 0o600); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(path)
	cur := cursor{Inode: fileInode(info), Offset: int64(len(first))}
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	current := fixtureLine(now, "/api/status", "200") + "\n"
	if err := os.WriteFile(path, []byte(current), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config{node: "local", logPath: path, maxLines: 100}
	payload, next, ok, err := readBatch(cfg, cur)
	if err != nil || !ok || len(payload.Samples) != 1 || payload.Samples[0].Route != "/v1/chat/completions" {
		t.Fatalf("必须先追读轮转旧文件尾部: payload=%+v next=%+v ok=%v err=%v", payload, next, ok, err)
	}
	payload, final, ok, err := readBatch(cfg, next)
	if err != nil || !ok || len(payload.Samples) != 1 || payload.Samples[0].Route != "/api/status" {
		t.Fatalf("追完旧文件后应读取当前文件: payload=%+v next=%+v ok=%v err=%v", payload, final, ok, err)
	}
	if final.Discontinuities != 0 {
		t.Fatalf("旧 inode 仍在且连续追读时不应误报不连续: %+v", final)
	}
}

func TestReadBatchDrainsMultipleRotationsInOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	now := time.Now()
	files := []struct {
		name, route string
		age         time.Duration
	}{
		{path + ".2", "/v1/responses", -2 * time.Hour},
		{path + ".1", "/v1/messages", -time.Hour},
		{path, "/api/status", 0},
	}
	for _, item := range files {
		line := fixtureLine(now.Unix(), item.route, "200") + "\n"
		if err := os.WriteFile(item.name, []byte(line), 0o600); err != nil {
			t.Fatal(err)
		}
		stamp := now.Add(item.age)
		if err := os.Chtimes(item.name, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	oldest, _ := os.Stat(path + ".2")
	cur := cursor{Inode: fileInode(oldest)}
	cfg := config{node: "local", logPath: path, maxLines: 100}
	for i, want := range []string{"/v1/responses", "/v1/messages", "/api/status"} {
		payload, next, ok, err := readBatch(cfg, cur)
		if err != nil || !ok || len(payload.Samples) != 1 || payload.Samples[0].Route != want {
			t.Fatalf("第 %d 个轮转文件顺序错误 want=%s payload=%+v next=%+v ok=%v err=%v", i, want, payload, next, ok, err)
		}
		cur = next
	}
}

func TestRunOnceSkipsPartialTailOfRotatedFile(t *testing.T) {
	dir := t.TempDir()
	logPath, cursorPath := filepath.Join(dir, "access.jsonl"), filepath.Join(dir, "cursor.json")
	line := fixtureLine(time.Now().Unix(), "/v1/responses", "200") + "\n"
	if err := os.WriteFile(logPath, []byte(line+`{"msec":"partial"`), 0o600); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(logPath)
	if err := saveCursor(cursorPath, cursor{Inode: fileInode(info), Offset: int64(len(line))}); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(logPath, logPath+".1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config{node: "local", logPath: logPath, cursorPath: cursorPath, maxLines: 100}
	if err := runOnce(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	currentInfo, _ := os.Stat(logPath)
	got, err := loadCursor(cursorPath)
	if err != nil {
		t.Fatal(err)
	}
	if got.Inode != fileInode(currentInfo) || got.Offset != 0 || got.Discontinuities != 1 {
		t.Fatalf("旧文件残行应被客观记录并切到当前文件: %+v", got)
	}
}

func TestLoadCursorFailsClosedOnCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cursor.json")
	if err := os.WriteFile(path, []byte(`{"inode":1,"offset":10}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCursor(path); err == nil {
		t.Fatal("缺少版本的游标不能静默当作首次启动")
	}
	if err := os.WriteFile(path, []byte(`not-json`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCursor(path); err == nil {
		t.Fatal("损坏游标必须失败关闭，避免重读并重复计数")
	}
}

func TestBatchIDIncludesContentDigest(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	now := time.Now().Unix()
	first := fixtureLine(now, "/v1/responses", "200") + "\n"
	if err := os.WriteFile(path, []byte(first), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config{node: "local", logPath: path, maxLines: 100, retentionDays: 7}
	before, _, ok, err := readBatch(cfg, cursor{})
	if err != nil || !ok {
		t.Fatalf("首个批次读取失败: ok=%v err=%v", ok, err)
	}
	// 保持同 inode、同起止偏移和同字节数，只改变日志内容。旧的
	// node+inode+offset 口径会碰撞；新口径必须产生不同幂等键。
	second := fixtureLine(now, "/v1/responses", "502") + "\n"
	if len(second) != len(first) {
		t.Fatal("测试样本必须等长")
	}
	if err := os.WriteFile(path, []byte(second), 0o600); err != nil {
		t.Fatal(err)
	}
	after, _, ok, err := readBatch(cfg, cursor{})
	if err != nil || !ok {
		t.Fatalf("第二个批次读取失败: ok=%v err=%v", ok, err)
	}
	if before.BatchID == after.BatchID {
		t.Fatalf("内容不同的批次不得因 inode/偏移相同而碰撞: %s", before.BatchID)
	}
}

func TestLoadConfigRejectsUnsafeOrAmbiguousValues(t *testing.T) {
	for _, key := range []string{
		"NGINXCOLLECTOR_NODE", "NGINXCOLLECTOR_SINK_URL", "NGINXCOLLECTOR_TOKEN",
		"NGINXCOLLECTOR_INTERVAL_SECONDS", "NGINXCOLLECTOR_MAX_LINES", "NGINXCOLLECTOR_RETENTION_DAYS",
		"NGINXCOLLECTOR_ALLOW_INSECURE_HTTP",
	} {
		t.Setenv(key, "")
	}
	t.Setenv("NGINXCOLLECTOR_NODE", "master")
	t.Setenv("NGINXCOLLECTOR_TOKEN", "secret")
	t.Setenv("NGINXCOLLECTOR_SINK_URL", "https://monitor.example/internal/nginx")
	if _, err := loadConfig(); err != nil {
		t.Fatalf("安全默认配置应通过: %v", err)
	}
	t.Setenv("NGINXCOLLECTOR_SINK_URL", "http://monitor.example/internal/nginx")
	if _, err := loadConfig(); err == nil {
		t.Fatal("公网明文 sink 必须默认拒绝")
	}
	t.Setenv("NGINXCOLLECTOR_ALLOW_INSECURE_HTTP", "true")
	if _, err := loadConfig(); err != nil {
		t.Fatalf("经显式确认的私网 HTTP 应允许: %v", err)
	}
	t.Setenv("NGINXCOLLECTOR_MAX_LINES", "not-a-number")
	if _, err := loadConfig(); err == nil {
		t.Fatal("错误整数不能静默回落，否则会改变批次边界")
	}
}

func TestPostBatchRefusesRedirect(t *testing.T) {
	var targetHits atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	err := postBatch(context.Background(), config{sinkURL: redirect.URL, token: "must-not-forward"}, batch{Node: "master", BatchID: "batch_abcdefgh"})
	if err == nil || targetHits.Load() != 0 {
		t.Fatalf("采集 token 不得跟随重定向: hits=%d err=%v", targetHits.Load(), err)
	}
}

func TestCollectorTelemetryReportsBacklog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	if err := os.WriteFile(path+".1", []byte("1234567890"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("12345678901234567890"), 0o600); err != nil {
		t.Fatal(err)
	}
	old, _ := os.Stat(path + ".1")
	backlog, known := collectorTelemetry(config{logPath: path}, cursor{Inode: fileInode(old), Offset: 3})
	if !known || backlog != 27 {
		t.Fatalf("积压字节统计错误: known=%v backlog=%d", known, backlog)
	}
	backlog, known = collectorTelemetry(config{logPath: path}, cursor{})
	if !known || backlog != 20 {
		t.Fatalf("首次启动应与游标选择一致，只统计当前日志: known=%v backlog=%d", known, backlog)
	}
}

func TestReadBatchAdvancesPastInvalidAndExpiredLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	now := time.Now().Unix()
	content := "not-json\n" +
		fixtureLine(now-10*86400, "/v1/responses", "200") + "\n" +
		fixtureLine(now, "/api/status", "200") + "\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config{node: "local", logPath: path, maxLines: 100, retentionDays: 7}
	payload, next, ok, err := readBatch(cfg, cursor{})
	if err != nil || !ok || len(payload.Samples) != 1 || payload.Samples[0].Route != "/api/status" {
		t.Fatalf("无效/超窗行不应阻塞后续合法行: payload=%+v next=%+v ok=%v err=%v", payload, next, ok, err)
	}
	if next.DiscardedLines != 2 || next.LastDiscardedAt == 0 || next.Offset != int64(len(content)) {
		t.Fatalf("跳过行必须推进并记录客观计数: %+v", next)
	}
	decorateBatch(cfg, &payload, next)
	if payload.DiscardedLines != 2 || payload.LastDiscardedAt == 0 {
		t.Fatalf("跳过行状态必须随批次上报: %+v", payload)
	}
}
