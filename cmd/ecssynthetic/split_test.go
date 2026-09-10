package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSplitFinalFilesAndIndependentOracle(t *testing.T) {
	for _, kind := range []string{"nginx", "new-api"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			var output bytes.Buffer
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Deterministic initial + final batch, no sleeps.
			if err := produceSplit(ctx, kind, root, &output, time.Hour, time.Hour); err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(strings.TrimSpace(output.String()), "\n")
			if len(lines) != 2 {
				t.Fatal("initial and final oracle required", output.String())
			}
			var oracle struct {
				Schema   string         `json:"schema"`
				Kind     string         `json:"kind"`
				Instance string         `json:"instance"`
				Files    map[string]int `json:"files"`
				Final    bool           `json:"final"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "SPLIT_SYNTHETIC_ORACLE ")), &oracle); err != nil {
				t.Fatal(err)
			}
			if oracle.Schema != "split-v1" || oracle.Kind != kind || len(oracle.Instance) != 16 || !oracle.Final {
				t.Fatal("invalid final oracle", oracle)
			}
			want := 2
			if kind == "new-api" {
				want = 3
			}
			entries, err := os.ReadDir(root)
			if err != nil || len(entries) != want || len(oracle.Files) != want {
				t.Fatal("wrong retained file set", entries, oracle, err)
			}
			for name, count := range oracle.Files {
				data, err := os.ReadFile(filepath.Join(root, name))
				if err != nil || bytes.Count(data, []byte("\n")) != count {
					t.Fatal("oracle differs from actual file", name, err)
				}
				expected := 2
				if name == rejectFixtureNames[2] {
					expected = 1
				}
				if count != expected {
					t.Fatal("tail or late-created file missing", name, count)
				}
			}
		})
	}
}

func TestSplitRejectsExistingAndSymlinkFiles(t *testing.T) {
	for _, symlink := range []bool{false, true} {
		root := t.TempDir()
		path := filepath.Join(root, "nexusapi_access.jsonl")
		target := filepath.Join(t.TempDir(), "untouched")
		if err := os.WriteFile(target, []byte("original"), 0600); err != nil {
			t.Fatal(err)
		}
		var err error
		if symlink {
			err = os.Symlink(target, path)
		} else {
			err = os.WriteFile(path, []byte("original"), 0600)
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := newSplitWriter("nginx", root, &bytes.Buffer{}); err == nil {
			t.Fatal("existing source accepted")
		}
		data, _ := os.ReadFile(target)
		if string(data) != "original" {
			t.Fatal("source target modified")
		}
	}
}

func TestSplitCannotPublishFinalOnWriteFailure(t *testing.T) {
	var output bytes.Buffer
	w, err := newSplitWriter("nginx", t.TempDir(), &output)
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	if err := w.files["error.log"].Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.write(true); err == nil || strings.Contains(output.String(), `"final":true`) {
		t.Fatal("failed tail write cannot produce final oracle", err, output.String())
	}
}

func TestSplitConfigurationFailsClosed(t *testing.T) {
	if _, _, err := splitLayout("all"); err == nil {
		t.Fatal("unknown producer accepted")
	}
	if err := runSplit("nginx", []string{"--unknown"}); err == nil {
		t.Fatal("unknown flag accepted")
	}
	t.Setenv("ECSLOG_SCOPE", "production")
	t.Setenv("ECSLOG_SYNTHETIC_KIND", "nginx")
	if err := run(); err == nil {
		t.Fatal("production scope accepted")
	}
}
