package main

// Product startup composition against new local synthetic stores only. The
// SELECT-only source connector is lazy and points at an unused loopback port;
// no source queries, .env, online settings or real identities are used.
import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlydraft"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestMonthlyGenerationColdMaintenanceExplicitGateAndDisable(t *testing.T) {
	settings := monitor.Settings{PortalAddr: "127.0.0.1:8091", PortalMonthlyStatementEnabled: true, StorePath: "/private/tmp/synthetic-main.sqlite"}
	base := map[string]string{
		"MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED": "true", "MONITOR_PORTAL_MONTHLY_GENERATION_TARGETS": "1:2026-09",
		"MONITOR_PORTAL_MONTHLY_GENERATION_MODE": "automatic-series", "MONITOR_PORTAL_MONTHLY_GENERATION_SCOPE_SOURCE": "scope",
		"MONITOR_PORTAL_MONTHLY_GENERATION_DISPLAY_SERIES_INDEX": "series.json",
		"MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_CONFIG":   "capture.json", "MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_SHA256": strings.Repeat("a", 64),
		"MONITOR_PORTAL_MONTHLY_GENERATION_COLD_DIRECTORY": "/private/tmp/synthetic-monthly-cold",
	}
	for _, kind := range []string{"valid", "omitted", "relative", "root", "dotdot", "whitespace", "missing_capture", "missing_pin", "draft", "inbox", "snapshot", "disabled"} {
		t.Run(kind, func(t *testing.T) {
			env := map[string]string{}
			for k, v := range base {
				env[k] = v
			}
			s := settings
			switch kind {
			case "omitted":
				delete(env, "MONITOR_PORTAL_MONTHLY_GENERATION_COLD_DIRECTORY")
			case "relative":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_COLD_DIRECTORY"] = "cold"
			case "root":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_COLD_DIRECTORY"] = "/"
			case "dotdot":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_COLD_DIRECTORY"] = "/private/tmp/one/../cold"
			case "whitespace":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_COLD_DIRECTORY"] = " /private/tmp/cold"
			case "missing_capture":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_CONFIG"] = ""
				env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_SHA256"] = ""
			case "missing_pin":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_SHA256"] = ""
			case "draft":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_MODE"] = "draft"
			case "inbox":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_INBOX"] = "old-inbox.json"
			case "snapshot":
				s.LocalSnapshotOnly = true
			case "disabled":
				env["MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED"] = "false"
				env["MONITOR_PORTAL_MONTHLY_GENERATION_COLD_DIRECTORY"] = "../stale-cold"
				s = monitor.Settings{}
			}
			cfg, err := monthlyGenerationConfig(func(k string) string { return env[k] }, s)
			valid := kind == "valid" || kind == "omitted" || kind == "disabled"
			if (err == nil) != valid {
				t.Fatal(kind, err)
			}
			if kind == "valid" && cfg.coldDirectory != base["MONITOR_PORTAL_MONTHLY_GENERATION_COLD_DIRECTORY"] {
				t.Fatal("lost explicit cold path")
			}
			if kind == "omitted" && cfg.coldDirectory != "" {
				t.Fatal("default enabled maintenance")
			}
			if kind == "disabled" {
				if cfg.coldDirectory != "" || cfg.Rollout.Enabled {
					t.Fatal("disabled mode inspected stale directory")
				}
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if d, err := openMonthlyGeneration(ctx, cfg, nil); d != nil || err != nil {
					t.Fatal("disabled startup accessed resources", err)
				}
			}
		})
	}
	for _, mode := range []string{"draft", "automatic", "automatic-series"} {
		cfg := monthlyServiceConfig{DeploymentConfig: monthlydraft.DeploymentConfig{Rollout: monthlydraft.GenerationRollout{Enabled: true}}, mode: mode, coldDirectory: "/private/tmp/must-not-open"}
		if d, err := openMonthlyGeneration(t.Context(), cfg, nil); d != nil || !errors.Is(err, monthlydraft.ErrGenerationInput) {
			t.Fatal("direct opener bypassed composition gate", mode, err)
		}
	}
}

type monthlyCapacityStartupFixture struct {
	m                             *monitor.Monitor
	cfg                           monthlyServiceConfig
	cold, spool, captureDirectory string
}

func newMonthlyCapacityStartupFixture(t *testing.T) monthlyCapacityStartupFixture {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	s := monitor.Settings{StorePath: filepath.Join(root, "main.sqlite"), PortalAddr: "127.0.0.1:8091", PortalMonthlyStatementEnabled: true,
		SessionSecret: "synthetic-cold-startup-no-credentials", AlertsDisabled: true}
	// Isolate the unrelated monitoring connector. The product config parser
	// below still receives the normal (non-snapshot) settings and its separate
	// snapshot-rejection test stays intact; no production guard is relaxed.
	isolatedMonitor := s
	isolatedMonitor.LocalSnapshotOnly = true
	m, err := monitor.New(isolatedMonitor)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	receiver := filepath.Join(root, "receiver.sqlite")
	if _, err := monthlydraft.InitializeGenerationReceiverFile(t.Context(), receiver, true); err != nil {
		t.Fatal(err)
	}
	funding := filepath.Join(root, "funding.sqlite")
	fund, err := sql.Open("sqlite", funding)
	if err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(monthlysource.InitFundingPageStore(t.Context(), fund), fund.Close()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"raw", "capture", "cold"} {
		if err := os.Mkdir(filepath.Join(root, name), 0700); err != nil {
			t.Fatal(err)
		}
	}
	capture := filepath.Join(root, "capture", "capture.sqlite")
	db, err := sql.Open("sqlite", capture)
	if err != nil {
		t.Fatal(err)
	}
	for _, init := range []func(context.Context, *sql.DB) error{monthlysource.InitFundingMonthCaptureStore, monthlysource.InitTokenMonthCaptureStore, monthlysource.InitWalletMonthCaptureStore} {
		if err := init(t.Context(), db); err != nil {
			_ = db.Close()
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(capture, 0600); err != nil {
		t.Fatal(err)
	}
	write := func(name string, raw []byte) monthlysource.MonthlyObservationFile {
		t.Helper()
		p := filepath.Join(root, name)
		if err := os.WriteFile(p, raw, 0600); err != nil {
			t.Fatal(err)
		}
		h := sha256.Sum256(raw)
		return monthlysource.MonthlyObservationFile{Path: p, SHA256: hex.EncodeToString(h[:])}
	}
	// These are intentionally empty PINNED metadata, not valid financial
	// evidence or permission to generate a bill; no collection runs at open.
	history := write("history.json", []byte(`{}`))
	catalogue := write("catalogue.json", []byte(`{}`))
	connection := write("connection.json", []byte(`{"network":"tcp","address":"127.0.0.1:9","user":"synthetic_select_only","password":"not-a-live-secret","database":"synthetic"}`))
	p := monthlydraft.CapturedPreparationConfig{Version: 1, SourceIdentity: strings.Repeat("a", 64), Connection: connection, CapturePath: capture,
		SpoolDirectory: filepath.Join(root, "raw"), History: history, ScopeCatalogue: catalogue}
	bytes, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	config := write("capture.json", bytes)
	write("series.json", []byte(`{"version":1,"ranges":[{"group_id":1,"source_identity":"`+p.SourceIdentity+`","from":"2026-09","through":"2026-09"}]}`))
	env := map[string]string{
		"MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED": "true", "MONITOR_PORTAL_MONTHLY_GENERATION_TARGETS": "1:2026-09",
		"MONITOR_PORTAL_MONTHLY_GENERATION_MODE": "automatic-series", "MONITOR_PORTAL_MONTHLY_GENERATION_SCOPE_SOURCE": "scope",
		"MONITOR_PORTAL_MONTHLY_GENERATION_DISPLAY_SERIES_INDEX": "series.json",
		"MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_CONFIG":   "capture.json", "MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_SHA256": config.SHA256,
		"MONITOR_PORTAL_MONTHLY_GENERATION_RECEIVER": receiver, "MONITOR_PORTAL_MONTHLY_GENERATION_FUNDING": funding,
		"MONITOR_PORTAL_MONTHLY_GENERATION_EVIDENCE": root, "MONITOR_PORTAL_MONTHLY_GENERATION_COLD_DIRECTORY": filepath.Join(root, "cold"),
	}
	cfg, err := monthlyGenerationConfig(func(k string) string { return env[k] }, s)
	if err != nil {
		t.Fatal(err)
	}
	return monthlyCapacityStartupFixture{m, cfg, filepath.Join(root, "cold"), filepath.Join(root, "raw"), filepath.Join(root, "capture")}
}

func TestMonthlyGenerationColdMaintenanceActualStartupStopAndReopen(t *testing.T) {
	f := newMonthlyCapacityStartupFixture(t)
	for _, enabled := range []bool{false, true, false, true} {
		cfg := f.cfg
		if !enabled {
			cfg.coldDirectory = ""
		}
		d, err := openMonthlyGeneration(t.Context(), cfg, f.m)
		if err != nil || d == nil {
			t.Fatal("startup failed", enabled, err)
		}
		if _, err := d.SubmitMonthly(t.Context(), 1, 1, "2026-09"); err == nil {
			t.Fatal("startup admitted work before Start")
		}
		if err := d.Start(t.Context()); err != nil {
			closeMonthlyGeneration(d)
			t.Fatal(err)
		}
		closeMonthlyGeneration(d)
		if err := d.Start(t.Context()); err == nil {
			t.Fatal("stopped owner restarted")
		}
		if err := d.EnablePublishedCapacityMaintenance(t.Context(), f.cold); err == nil {
			t.Fatal("closed owner enabled maintenance")
		}
	}
	db, err := sql.Open("sqlite", f.cfg.ReceiverPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var guard string
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='trigger' AND name='monthly_generation_no_delete'`).Scan(&guard); err != nil || !strings.Contains(guard, "verified saved publication") {
		t.Fatal("durable release barrier missing", guard, err)
	}
}

func TestMonthlyGenerationColdMaintenanceFailedOpenDoesNotLeakOwnerOrRepairDirectory(t *testing.T) {
	for _, kind := range []string{"missing", "raw_alias", "capture_alias", "public", "symlink", "unknown_file", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			f := newMonthlyCapacityStartupFixture(t)
			cfg := f.cfg
			ctx := t.Context()
			switch kind {
			case "missing":
				cfg.coldDirectory = filepath.Join(f.cold, "absent")
			case "raw_alias":
				cfg.coldDirectory = f.spool
			case "capture_alias":
				cfg.coldDirectory = f.captureDirectory
			case "public":
				if err := os.Chmod(f.cold, 0755); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				alias := f.cold + "-alias"
				if err := os.Symlink(f.cold, alias); err != nil {
					t.Fatal(err)
				}
				cfg.coldDirectory = alias
			case "unknown_file":
				if err := os.WriteFile(filepath.Join(f.cold, "keep-me"), []byte("synthetic sentinel"), 0600); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if d, err := openMonthlyGeneration(ctx, cfg, f.m); d != nil || err == nil {
				t.Fatal("invalid maintenance started or fell back", d, err)
			}
			if kind == "missing" {
				if _, err := os.Stat(cfg.coldDirectory); !os.IsNotExist(err) {
					t.Fatal("startup created missing directory")
				}
			}
			// Recovery explicitly disables ONLY this optional maintenance. Same
			// receiver/capture paths must be immediately reusable, not repaired.
			cfg.coldDirectory = ""
			d, err := openMonthlyGeneration(t.Context(), cfg, f.m)
			if err != nil {
				t.Fatal("failed open leaked resources", err)
			}
			closeMonthlyGeneration(d)
			if kind == "unknown_file" {
				if raw, err := os.ReadFile(filepath.Join(f.cold, "keep-me")); err != nil || string(raw) != "synthetic sentinel" {
					t.Fatal("startup removed sentinel", err)
				}
			}
		})
	}
}
