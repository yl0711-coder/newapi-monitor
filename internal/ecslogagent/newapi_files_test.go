package ecslogagent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func newAPIFilesFixture(t *testing.T) (*Agent, map[string]ecsarchive.FinalFile) {
	t.Helper()
	c, meta := testConfig(t)
	c.DeferredArchiveACK, c.ArchiveClosure = true, true
	c.FinalLogRoot, c.FinalFileContract = t.TempDir(), ecsarchive.NewAPIFileContract
	a := testAgent(t, c, meta, roundTripFunc(func(*http.Request) (*http.Response, error) { return response(503, "offline"), nil }))
	for _, name := range []string{"oneapi-20000101.log", "oneapi-20000102.log"} {
		if err := os.WriteFile(filepath.Join(c.FinalLogRoot, name), []byte("fixture\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	files, err := a.finalInventory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cursors := map[string]any{}
	for _, f := range files {
		h := sha256.Sum256([]byte(fmt.Sprintf("reject-file-v1:%d:%d", f.Device, f.Inode)))
		cursors[hex.EncodeToString(h[:])] = map[string]any{"offset": f.Size, "observed_size": f.Size}
	}
	b, err := json.Marshal(map[string]any{"version": 1, "files": cursors})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.dir, "reject-state.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	return a, files
}

func TestNewAPIWholeInventoryAndCursor(t *testing.T) {
	for _, scenario := range []string{"valid", "missing", "replacement", "truncated", "new-file", "unexpected-file", "hardlink", "symlink", "partial-tail", "deleted-history", "pending", "hash-only", "oversize", "too-many-files"} {
		t.Run(scenario, func(t *testing.T) {
			a, before := newAPIFilesFixture(t)
			path := filepath.Join(a.cfg.FinalLogRoot, "oneapi-20000102.log")
			var err error
			switch scenario {
			case "missing", "replacement", "symlink":
				moved := filepath.Join(t.TempDir(), "retained.log")
				err = os.Rename(path, moved)
				if err == nil && scenario == "replacement" {
					err = os.WriteFile(path, []byte("fixture\n"), 0600)
				}
				if err == nil && scenario == "symlink" {
					err = os.Symlink(moved, path)
				}
			case "truncated":
				err = os.Truncate(path, 0)
			case "new-file":
				err = os.WriteFile(filepath.Join(a.cfg.FinalLogRoot, "oneapi-20000103.log"), nil, 0600)
			case "unexpected-file":
				err = os.WriteFile(path+".1", nil, 0600)
			case "too-many-files":
				for i := 0; i < ecsarchive.FinalMaxFiles; i++ {
					if e := os.WriteFile(filepath.Join(a.cfg.FinalLogRoot, fmt.Sprintf("oneapi-extra-%d.log", i)), nil, 0600); e != nil {
						t.Fatal(e)
					}
				}
			case "hardlink":
				err = os.Link(path, filepath.Join(a.cfg.FinalLogRoot, "oneapi-alias.log"))
			case "oversize":
				err = os.Truncate(path, finalReadBudget+1)
			case "partial-tail", "deleted-history", "pending", "hash-only":
				var state map[string]any
				if e := readFinalCursor(filepath.Join(a.dir, "reject-state.json"), &state); e != nil {
					t.Fatal(e)
				}
				cursors := state["files"].(map[string]any)
				switch scenario {
				case "partial-tail":
					for _, cur := range cursors {
						cur.(map[string]any)["offset"] = 7
					}
				case "deleted-history":
					cursors["lost-inode"] = map[string]any{"offset": 0, "observed_size": 0}
				case "pending":
					state["pending"] = map[string]any{"batch_id": "still-pending"}
				case "hash-only":
					state["pending_hash"] = "unacknowledged"
				}
				b, e := json.Marshal(state)
				if e != nil {
					t.Fatal(e)
				}
				err = os.WriteFile(filepath.Join(a.dir, "reject-state.json"), b, 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			boundaries, err := a.finishBoundaries(context.Background(), before)
			if (err == nil) != (scenario == "valid") {
				t.Fatalf("%s: %v", scenario, err)
			}
			if err == nil {
				b := boundaries["reject"]
				if len(b.Files) != 2 || b.Contract != ecsarchive.NewAPIFileContract {
					t.Fatalf("wrong manifest %+v", b)
				}
				if _, err := ecsarchive.ReadClosure(ecsarchive.BoundaryBody(a.node, b), a.node, "reject"); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestNewAPIFileConfigAndCapability(t *testing.T) {
	a, _ := newAPIFilesFixture(t)
	for _, mutate := range []func(*Config){
		func(c *Config) { c.FinalFileContract = "anything" },
		func(c *Config) { c.FinalLogRoot = "" },
		func(c *Config) { c.Scope = "production" },
	} {
		bad := a.cfg
		mutate(&bad)
		if bad.Validate() == nil {
			t.Fatal("unsafe config accepted")
		}
	}
	env := strings.Join(a.ChildEnvironment(nil), "\n")
	if !strings.Contains(env, "COLLECTOR_LOG_GLOB="+filepath.Join(a.cfg.FinalLogRoot, "oneapi-*.log")) || !strings.Contains(env, "NGINXCOLLECTOR_LOG_PATH="+filepath.Join(a.cfg.FinalLogRoot, "nexusapi_access.jsonl")) {
		t.Fatal("wrong real-source mapping")
	}
	for _, capability := range []any{nil, false, "true", 1, true} {
		a.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
			ack := map[string]any{"ok": true, "version": 1, "node": a.node, "lane": "reject", "audience": a.cfg.Audience, "lease_until": time.Now().Unix() + 600, "archive_closure_v2": true, "final_boundary_v1": true}
			if capability != nil {
				ack["final_newapi_files_v1"] = capability
			}
			b, _ := json.Marshal(ack)
			return response(200, string(b)), nil
		})
		err := a.Renew(context.Background(), "reject")
		if (err == nil) != (capability == true) {
			t.Fatalf("capability %v: %v", capability, err)
		}
	}
}

func TestNewAPIFileContractCannotReuseLegacyIdentity(t *testing.T) {
	a, before := newAPIFilesFixture(t)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := testAgent(t, a.cfg, a.meta, a.client.Transport)
	if _, err := reopened.finishBoundaries(context.Background(), before); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	changed := a.cfg
	changed.FinalFileContract = ""
	other, err := New(changed, a.meta, fixtureCredentials(), a.client)
	if err == nil {
		_ = other.Close()
		t.Fatal("changed file contract reused old identity and cursors")
	}
}
