package monitor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
	"github.com/yl0711-coder/newapi-monitor/internal/ecslogagent"
)

func TestECSLogRealAgentShutdownDrainsTailToArchive(t *testing.T) {
	for _, kind := range []string{"nginx", "reject"} {
		t.Run(kind, func(t *testing.T) { testECSShutdownProcess(t, kind, false, false, "") })
	}
}

func TestECSLogRealAgentShutdownClosesArchive(t *testing.T) {
	for _, kind := range []string{"nginx", "reject"} {
		t.Run(kind, func(t *testing.T) { testECSShutdownProcess(t, kind, true, false, "") })
	}
}

func TestECSLogRealAgentFinalBoundary(t *testing.T) {
	for _, kind := range []string{"nginx", "reject"} {
		t.Run(kind, func(t *testing.T) { testECSShutdownProcess(t, kind, true, true, "") })
	}
}

func TestECSLogRealAgentNewAPIFileBoundary(t *testing.T) {
	for _, kind := range []string{"nginx", "reject"} {
		t.Run(kind, func(t *testing.T) { testECSShutdownProcess(t, kind, true, true, ecsarchive.NewAPIFileContract) })
	}
}

func testECSShutdownProcess(t *testing.T, kind string, closure, final bool, contract string) {
	t.Helper()
	variable := "MONITOR_TEST_NGINX_COLLECTOR_BIN"
	if kind == "reject" {
		variable = "MONITOR_TEST_REJECT_COLLECTOR_BIN"
	}
	binary := os.Getenv(variable)
	if binary == "" {
		t.Skip("real binaries mandatory in ecs-local-acceptance.sh")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("absolute binary required")
	}
	m := newECSLogTestMonitor(t)
	m.cfg.ECSArchiveEnabled = closure
	if contract != "" {
		m.cfg.ECSLogOwnershipEnabled = true
		if err := m.initECSLogOwnership(m.storeDB); err != nil {
			t.Fatal(err)
		}
	}
	taskID := strings.Repeat("d", 32)
	logRoot := t.TempDir()
	agent, cfg, _, _ := ecsAgentFixture(t, m, kind, taskID, nil, func(c *ecslogagent.Config) {
		c.DeferredArchiveACK = true
		c.ArchiveClosure = closure
		if final {
			c.FinalLogRoot = logRoot
			c.FinalFileContract = contract
		}
	})
	accessName, rejectName := "access.jsonl", "new-api.log"
	if contract != "" {
		accessName, rejectName = "nexusapi_access.jsonl", "oneapi-20000101.log"
	}
	if final {
		agent.ConfigureProducerStopCheck(func(context.Context) error { return nil })
		names := []string{accessName, "error.log"}
		if kind == "reject" {
			names = []string{rejectName}
		}
		for _, name := range names {
			if err := os.WriteFile(filepath.Join(logRoot, name), nil, 0600); err != nil {
				t.Fatal(err)
			}
		}
	} else {
		logRoot = cfg.StateRoot
	}
	archive := &notifyingArchiveFixture{disk: archiveDiskFixture{root: t.TempDir()}, written: make(chan string, 64)}
	if err := agent.ConfigureArchive(archive, "isolated/", "AROAABCDEFGHIJKLMNOPQ:"+taskID); err != nil {
		t.Fatal(err)
	}
	lanes := []string{"access", "error", "evidence"}
	if kind == "reject" {
		lanes = []string{"reject"}
	}
	for _, lane := range lanes {
		if err := agent.Renew(context.Background(), lane); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	sources := map[string]string{}
	if kind == "nginx" {
		access, errorLog := filepath.Join(logRoot, accessName), filepath.Join(logRoot, "error.log")
		sources[access] = fmt.Sprintf(`{"log_schema":2,"msec":"%d.250","request_method":"POST","uri":"/v1/responses","status":"200","request_time":"0.350","upstream_status":"200","upstream_response_time":"0.300","upstream_connect_time":"0.025","upstream_header_time":"0.125","bytes_sent":"1024","nginx_request_id":"fixture-nginx","oneapi_request_id":"fixture-api","request_completion":"OK"}`+"\n", now.Unix())
		sources[errorLog] = now.Format("2006/01/02 15:04:05") + " [error] 1#1: *1 upstream timed out (110: Connection timed out) while reading response header from upstream\n"
		t.Setenv("NGINXCOLLECTOR_LOG_PATH", access)
		t.Setenv("NGINXCOLLECTOR_ERROR_LOG_PATH", errorLog)
		t.Setenv("NGINXCOLLECTOR_INTERVAL_SECONDS", "3600")
		t.Setenv("NGINXCOLLECTOR_ERROR_TIMEZONE", "UTC")
		t.Setenv("NGINXCOLLECTOR_EVIDENCE_HMAC_KEY", strings.Repeat("k", 32))
		t.Setenv("NGINXCOLLECTOR_EVIDENCE_HMAC_KEY_ID", "key-1")
	} else {
		path := filepath.Join(logRoot, rejectName)
		sources[path] = "[ERR] " + now.Format("2006/01/02 - 15:04:05") + " | fixture | user 7 | No available channel for model fixture under group fixture-group (distributor)\n"
		if contract != "" {
			sources[filepath.Join(logRoot, "oneapi-20000102.log")] = sources[path]
		}
		t.Setenv("COLLECTOR_LOG_GLOB", path)
		t.Setenv("COLLECTOR_LOG_TIMEZONE", "UTC")
		t.Setenv("COLLECTOR_FLUSH_SECONDS", "3600")
	}
	for path, line := range sources {
		if err := os.WriteFile(path, []byte(line), 0600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- agent.RunChild(ctx, []string{binary}) }()
	keys := []string{}
	seen := map[string]bool{}
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	for len(seen) < len(lanes) {
		select {
		case key := <-archive.written:
			keys = append(keys, key)
			o, err := archive.Get(context.Background(), key)
			if err != nil {
				t.Fatal(err)
			}
			e, err := ecsarchive.Decode(o.Body)
			if err != nil {
				t.Fatal(err)
			}
			seen[e.Lane] = true
		case err := <-done:
			t.Fatalf("collector exited before initial batch: %v", err)
		case <-deadline.C:
			t.Fatal("initial collection unavailable")
		}
	}
	// Write the final line immediately before SIGTERM. nginx's normal polling
	// interval is one hour, so it must use the new drain path to collect it.
	wantReject := int64(2 * len(sources))
	if kind == "reject" && contract != "" {
		// A new append-only file appears after initial collection. Retain the
		// old files: shutdown must discover and drain all three independent IDs.
		sources[filepath.Join(logRoot, "oneapi-20000103.log")] = sources[filepath.Join(logRoot, rejectName)]
		wantReject++
	}
	for path, line := range sources {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := f.WriteString(line)
		closeErr := f.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatal("tail append failed", writeErr, closeErr)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown failed: %v", err)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("agent shutdown hung")
	}
	for len(archive.written) > 0 {
		keys = append(keys, <-archive.written)
	}
	if err := agent.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(cfg.StateRoot, "/tmp/eai-") {
		t.Fatal("unexpected disposable task root")
	}
	if err := os.RemoveAll(cfg.StateRoot); err != nil {
		t.Fatal(err)
	}
	r := newECSArchiveReplayer(m, "isolated/")
	if final {
		// Independent control-plane observation arrives after archived upload.
		if err := m.storeDB.Model(&ECSLogSource{}).Where("node = ?", agent.Node()).Updates(map[string]any{"stopped_at": time.Now().Unix(), "stop_code": "ServiceSchedulerInitiated", "producer_exit_code": 0}).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range keys {
		o, err := archive.Get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		if final {
			envelope, err := ecsarchive.Decode(o.Body)
			if err != nil {
				t.Fatal(err)
			}
			if envelope.Kind == ecsarchive.CollectorClosure {
				boundary, err := ecsarchive.ReadClosure(envelope.Body, agent.Node(), envelope.Lane)
				if err != nil || boundary == nil || boundary.Contract != contract {
					t.Fatalf("wrong file contract: %+v %v", boundary, err)
				}
				if kind == "reject" && len(boundary.Files) != len(sources) {
					t.Fatal("multi-file boundary omitted a retained source")
				}
			}
		}
		if result := r.replay(context.Background(), o, time.Now()); !result.Accepted {
			t.Fatalf("tail replay %+v", result)
		}
		if result := r.replay(context.Background(), o, time.Now()); !result.Accepted || !result.Duplicate {
			t.Fatalf("duplicate replay %+v", result)
		}
	}
	if closure {
		var count int64
		if err := m.storeDB.Model(&ECSLogArchiveReceipt{}).Where("kind = ? AND status = 200", ecsarchive.CollectorClosure).Count(&count).Error; err != nil || count != int64(len(lanes)) {
			t.Fatal("missing lane closure", count, err)
		}
	}
	if final {
		var count int64
		if err := m.storeDB.Model(&ECSLogArchiveReceipt{}).Where("final_boundary_verified = ? AND status = 200", true).Count(&count).Error; err != nil || count != int64(len(lanes)) {
			t.Fatal("final boundaries not verified", count, err)
		}
		var sources []ECSLogSource
		if err := m.storeDB.Find(&sources).Error; err != nil {
			t.Fatal(err)
		}
		if err := projectArchiveDelivery(m.storeDB, sources); err != nil {
			t.Fatal(err)
		}
		for _, source := range sources {
			if source.FinalBoundaryStatus != "retained_files_verified" {
				t.Fatalf("boundary projection %+v", source)
			}
		}
	}
	if kind == "reject" {
		var count int64
		for _, row := range m.storeRejections(now.Unix() - 120) {
			count += row.Count
		}
		if count != wantReject {
			t.Fatalf("reject final-tail count=%d want=%d", count, wantReject)
		}
	} else {
		var access, errors, evidence int64
		if err := m.storeDB.Model(&NginxMinuteSample{}).Select("COALESCE(SUM(count),0)").Scan(&access).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Model(&NginxErrorMinuteSample{}).Select("COALESCE(SUM(count),0)").Scan(&errors).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Count(&evidence).Error; err != nil {
			t.Fatal(err)
		}
		if access != 2 || errors != 2 || evidence != 2 {
			t.Fatalf("tail counts access=%d error=%d evidence=%d", access, errors, evidence)
		}
	}
}
