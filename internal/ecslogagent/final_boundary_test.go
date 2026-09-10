package ecslogagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func TestFinalMetadataRequiresSameExitedIncarnation(t *testing.T) {
	meta := Metadata{TaskARN: "task", RuntimeID: "runtime"}
	for _, tc := range []struct {
		task, runtime, status, exit string
		ok                          bool
	}{
		{"task", "runtime", "STOPPED", ",\"ExitCode\":0", true},
		{"other", "runtime", "STOPPED", ",\"ExitCode\":0", false},
		{"task", "other", "STOPPED", ",\"ExitCode\":0", false},
		{"task", "runtime", "RUNNING", ",\"ExitCode\":0", false},
		{"task", "runtime", "STOPPED", "", false},
		{"task", "runtime", "STOPPED", ",\"ExitCode\":137", false},
	} {
		body := []byte(fmt.Sprintf(`{"TaskARN":%q,"Containers":[{"Name":"producer","DockerId":%q,"KnownStatus":%q%s}]}`, tc.task, tc.runtime, tc.status, tc.exit))
		if got := checkStoppedMetadata(body, "producer", meta); (got == nil) != tc.ok {
			t.Fatalf("stop %+v: %v", tc, got)
		}
	}
}

func TestFinalBoundaryLostACKKeepsFrozenManifest(t *testing.T) {
	for name, fixture := range map[string]func(*testing.T) (*Agent, map[string]ecsarchive.FinalFile){"legacy": finalFixture, "newapi-multi": newAPIFilesFixture} {
		t.Run(name, func(t *testing.T) {
			a, before := fixture(t)
			testFrozenFinalManifest(t, a, before)
		})
	}
}

func testFrozenFinalManifest(t *testing.T, a *Agent, before map[string]ecsarchive.FinalFile) {
	t.Helper()
	store := &memoryArchive{objects: map[string][]byte{}}
	if err := a.ConfigureArchive(store, "isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("a", 32)); err != nil {
		t.Fatal(err)
	}
	a.leases["reject"] = time.Now().Unix() + 600
	boundaries, err := a.finishBoundaries(context.Background(), before)
	if err != nil {
		t.Fatal(err)
	}
	store.lostACK = true
	if a.closeArchiveWithBoundaries(context.Background(), boundaries) == nil {
		t.Fatal("lost ACK hidden")
	}
	var key string
	var raw []byte
	for k, b := range store.objects {
		key = k
		raw = append([]byte(nil), b...)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a = testAgent(t, a.cfg, a.meta, a.client.Transport)
	if err := a.ConfigureArchive(store, "isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("a", 32)); err != nil {
		t.Fatal(err)
	}
	a.leases["reject"] = time.Now().Unix() + 600
	store.lostACK = false
	boundaries["reject"].ObservedAt++
	if err := a.closeArchiveWithBoundaries(context.Background(), boundaries); err != nil {
		t.Fatal(err)
	}
	if len(store.objects) != 1 || !bytes.Equal(raw, store.objects[key]) {
		t.Fatal("retry rewrote final boundary")
	}
}

func TestFinalNginxCursorRejectsEveryKnownGap(t *testing.T) {
	a, _ := finalFixture(t)
	a.cfg.Kind = "nginx"
	f, err := snapshotFinalFile(context.Background(), filepath.Join(a.cfg.FinalLogRoot, "access.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"discontinuities", "discarded_lines", "evidence_dropped_events", "evidence_persist_failures", "evidence_parse_rejected", "offset", "inode", "device"} {
		cur := map[string]any{"version": 1, "inode": f.Inode, "device": f.Device, "offset": f.Size}
		switch field {
		case "offset":
			cur[field] = f.Size - 1
		case "inode":
			cur[field] = f.Inode + 1
		case "device":
			cur[field] = f.Device + 1
		default:
			cur[field] = 1
		}
		b, _ := json.Marshal(cur)
		if err := os.WriteFile(filepath.Join(a.dir, "access-cursor.json"), b, 0600); err != nil {
			t.Fatal(err)
		}
		if a.checkFinalCursor(f) == nil {
			t.Fatal("gap hidden", field)
		}
	}
}

func finalFixture(t *testing.T) (*Agent, map[string]ecsarchive.FinalFile) {
	t.Helper()
	c, meta := testConfig(t)
	c.DeferredArchiveACK = true
	c.ArchiveClosure = true
	c.FinalLogRoot = t.TempDir()
	a := testAgent(t, c, meta, roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") }))
	for _, name := range []string{"access.jsonl", "error.log", "new-api.log"} {
		if err := os.WriteFile(filepath.Join(c.FinalLogRoot, name), []byte("fixture\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	files, err := a.finalInventory(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	f := files["new-api.log"]
	h := sha256.Sum256([]byte(fmt.Sprintf("reject-file-v1:%d:%d", f.Device, f.Inode)))
	raw, _ := json.Marshal(map[string]any{"version": 1, "files": map[string]any{hex.EncodeToString(h[:]): map[string]any{"offset": f.Size, "observed_size": f.Size}}})
	if err := os.WriteFile(filepath.Join(a.dir, "reject-state.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	return a, files
}

func TestFinalInventoryAndCursorFailClosed(t *testing.T) {
	for _, scenario := range []string{"valid", "changed", "rotated", "missing", "symlink", "cursor", "extra_cursor"} {
		t.Run(scenario, func(t *testing.T) {
			a, before := finalFixture(t)
			path := filepath.Join(a.cfg.FinalLogRoot, "new-api.log")
			var err error
			switch scenario {
			case "changed":
				err = os.WriteFile(path, []byte("changed\n"), 0600)
			case "rotated":
				err = os.WriteFile(path+".1", []byte("fixture\n"), 0600)
			case "missing":
				err = os.Rename(path, path+".1")
			case "symlink":
				err = os.Rename(path, path+".1")
				if err == nil {
					err = os.Symlink(path+".1", path)
				}
			case "cursor":
				err = os.WriteFile(filepath.Join(a.dir, "reject-state.json"), []byte(`{"version":1,"files":{}}`), 0600)
			case "extra_cursor":
				var state map[string]any
				if e := readFinalCursor(filepath.Join(a.dir, "reject-state.json"), &state); e != nil {
					t.Fatal(e)
				}
				state["files"].(map[string]any)["deleted-file"] = map[string]any{"offset": 0, "observed_size": 0}
				b, _ := json.Marshal(state)
				err = os.WriteFile(filepath.Join(a.dir, "reject-state.json"), b, 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			_, err = a.finishBoundaries(context.Background(), before)
			if (err == nil) != (scenario == "valid") {
				t.Fatalf("%s: %v", scenario, err)
			}
		})
	}
}

func TestFinalInventoryAcceptsKindSpecificReadOnlyMount(t *testing.T) {
	a, before := finalFixture(t)
	// Only the rejection log is required by the rejection collector. Nginx
	// files may live in a different producer container/mounted directory.
	for _, name := range []string{"access.jsonl", "error.log"} {
		if err := os.Rename(filepath.Join(a.cfg.FinalLogRoot, name), filepath.Join(t.TempDir(), name)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.finishBoundaries(context.Background(), before); err != nil {
		t.Fatal(err)
	}
}
