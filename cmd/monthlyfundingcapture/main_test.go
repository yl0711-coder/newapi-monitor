package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixtureInitArgs(t *testing.T) []string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return []string{"init", "-workspace", dir, "-job", "group-1-sep-window-1", "-group-id", "1", "-month", "2026-09", "-source-identity", strings.Repeat("a", 64), "-user-ids", "201,202", "-from", "1788192000", "-to", "1788195600"}
}

func invoke(t *testing.T, args []string) (receipt, error) {
	t.Helper()
	var out bytes.Buffer
	err := run(context.Background(), args, &out)
	var result receipt
	if out.Len() != 0 {
		if e := json.Unmarshal(out.Bytes(), &result); e != nil {
			t.Fatal("non-json receipt", e)
		}
	}
	return result, err
}

func createFixture(t *testing.T) ([]string, receipt) {
	t.Helper()
	args := fixtureInitArgs(t)
	r, err := invoke(t, append(args, "-create"))
	if err != nil || !r.Created || !validSHA(r.ManifestSHA256) {
		t.Fatal("fixture init failed", r, err)
	}
	return args, r
}

func inspectArgs(init []string, r receipt, phase string) []string {
	return []string{"run", "-workspace", init[2], "-job", init[4], "-expected-manifest", r.ManifestSHA256, "-phase", phase}
}

func fileHash(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return hash(raw)
}

func TestCaptureCLIInitAndDefaultInspectionNeverConnectOrWrite(t *testing.T) {
	args := fixtureInitArgs(t)
	r, err := invoke(t, args)
	if err != nil || r.Created || r.ManifestSHA256 != "" {
		t.Fatal("preview created identities", r, err)
	}
	entries, err := os.ReadDir(args[2])
	if err != nil || len(entries) != 0 {
		t.Fatal("preview created files", err)
	}
	r, err = invoke(t, append(args, "-create"))
	if err != nil || !r.Created || r.OlderScanID == r.NewerScanID {
		t.Fatal(r, err)
	}
	job := filepath.Join(args[2], args[4])
	mp, dbp := filepath.Join(job, "manifest.json"), filepath.Join(job, "funding.sqlite")
	before, dbBefore := fileHash(t, mp), fileHash(t, dbp)
	for _, phase := range []string{"older", "newer"} {
		got, err := invoke(t, append(inspectArgs(args, r, phase), "-connection-file", "/nonexistent/private.json"))
		if err != nil || got.Started || got.EndReached || got.Capture != nil || got.ManifestSHA256 != before || (phase == "older") != got.LocalReady {
			t.Fatal("inspection contract failed", got, err)
		}
	}
	if before != fileHash(t, mp) || dbBefore != fileHash(t, dbp) {
		t.Fatal("default checks wrote state")
	}
	if _, err := invoke(t, append(args, "-create")); err == nil {
		t.Fatal("existing job overwritten")
	}
	for _, path := range []string{job, mp, dbp, filepath.Join(args[2], ".funding-capture.lock")} {
		st, err := os.Stat(path)
		if err != nil || st.Mode().Perm()&0077 != 0 {
			t.Fatal("private permissions lost", path, err)
		}
	}
}

func TestCaptureCLIRejectsScopeAndPrematureSecondPass(t *testing.T) {
	args, r := createFixture(t)
	for _, extra := range [][]string{{"-expected-manifest", strings.Repeat("b", 64)}, {"-phase", "unknown"}, {"-max-pages", "0"}, {"-max-pages", "11"}, {"-interval", "10ms"}, {"-budget", "31s"}, {"-job", "../escape"}, {"-job", "."}, {"-capture"}, {"positional"}} {
		if _, err := invoke(t, append(inspectArgs(args, r, "older"), extra...)); err == nil {
			t.Fatal("invalid control accepted", extra)
		}
	}
	got, err := invoke(t, append(inspectArgs(args, r, "newer"), "-capture", "-connection-file", "/never-read.json"))
	if err == nil || got.BlockedReason == "" || got.Capture != nil || got.Started {
		t.Fatal("premature second pass", got, err)
	}
	for _, extra := range [][]string{{"-group-id", "0"}, {"-month", "2026-08"}, {"-month", "2999-01"}, {"-to", "1788195601"}, {"-user-ids", "201,201"}, {"-user-ids", "201,0"}, {"-source-identity", "bad"}} {
		if _, err := invoke(t, append(fixtureInitArgs(t), extra...)); err == nil {
			t.Fatal("invalid scope accepted", extra)
		}
	}
}

func TestCaptureCLIManifestAndFileBoundaries(t *testing.T) {
	for _, kind := range []string{"unknown", "changed-group", "changed-users", "same-scan", "pending", "permissions", "symlink", "hardlink", "database-link", "lock-link", "journal-link"} {
		t.Run(kind, func(t *testing.T) {
			args, r := createFixture(t)
			job := filepath.Join(args[2], args[4])
			path := filepath.Join(job, "manifest.json")
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var m manifest
			if err := json.Unmarshal(raw, &m); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "unknown":
				raw = append([]byte(`{"unknown":1,`), raw[1:]...)
			case "changed-group":
				m.GroupID++
				raw, _ = json.Marshal(m)
			case "changed-users":
				m.Newer.UserIDs = []int64{201}
				raw, _ = json.Marshal(m)
			case "same-scan":
				m.Newer.ID = m.Older.ID
				raw, _ = json.Marshal(m)
			case "pending":
				err = os.WriteFile(filepath.Join(job, "manifest.pending"), raw, 0600)
			case "permissions":
				err = os.Chmod(path, 0644)
			case "symlink", "hardlink":
				backup := filepath.Join(job, "manifest.backup")
				if err = os.Rename(path, backup); err == nil {
					if kind == "symlink" {
						err = os.Symlink(backup, path)
					} else {
						err = os.Link(backup, path)
					}
				}
			case "database-link":
				err = os.Link(filepath.Join(job, "funding.sqlite"), filepath.Join(job, "alias.sqlite"))
			case "lock-link":
				err = os.Link(filepath.Join(args[2], ".funding-capture.lock"), filepath.Join(job, "alias.lock"))
			case "journal-link":
				err = os.Symlink(path, filepath.Join(job, "funding.sqlite-journal"))
			}
			if err != nil {
				t.Fatal(err)
			}
			if kind == "unknown" || kind == "changed-group" || kind == "changed-users" || kind == "same-scan" {
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
				if kind != "changed-group" {
					r.ManifestSHA256 = hash(raw)
				}
			}
			if _, err := invoke(t, inspectArgs(args, r, "older")); err == nil {
				t.Fatal("invalid job accepted", kind)
			}
		})
	}
}

func TestCaptureCLIConnectionWhitelistAndPrivateDiagnostics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "connection.json")
	base := connectionConfig{Network: "tcp", Address: "127.0.0.1:13306", User: "readonly", Password: "do-not-print-this", Database: "test"}
	write := func(c connectionConfig) {
		t.Helper()
		raw, _ := json.Marshal(c)
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, address := range []string{"127.0.0.1:13306", "[::1]:3306"} {
		c := base
		c.Address = address
		write(c)
		cfg, err := readConnection(path)
		if err != nil || cfg.MultiStatements || cfg.AllowAllFiles || cfg.AllowCleartextPasswords || cfg.Timeout != 2*time.Second || cfg.ReadTimeout != 4*time.Second {
			t.Fatal("unsafe driver config", err)
		}
	}
	for _, address := range []string{"example.com:3306", "localhost:3306", "10.0.0.1:3306", "127.0.0.1:0", "127.0.0.1:99999"} {
		c := base
		c.Address = address
		write(c)
		if _, err := readConnection(path); err == nil {
			t.Fatal("invalid source accepted", address)
		}
	}
	write(base)
	for _, extra := range []string{`,"multiStatements":true}`, `,"tls":"skip-verify"}`} {
		raw, _ := json.Marshal(base)
		raw = append(raw[:len(raw)-1], []byte(extra)...)
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := readConnection(path); err == nil {
			t.Fatal("unknown driver option accepted")
		}
	}
	rootConfig := base
	rootConfig.User = "root"
	write(rootConfig)
	if _, err := readConnection(path); err == nil {
		t.Fatal("root source credentials accepted")
	}
	write(base)
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readConnection(path); err == nil {
		t.Fatal("shared credentials accepted")
	}
	for _, err := range []error{errors.New(base.Password), errors.Join(errInput, errors.New(base.Password)), context.Canceled} {
		if strings.Contains(safeError(err), base.Password) {
			t.Fatal("private diagnostics leaked")
		}
	}
	var out bytes.Buffer
	if err := run(context.Background(), []string{"run", "-password", base.Password}, &out); err == nil || out.Len() != 0 {
		t.Fatal("credentials accepted in argv")
	}
}

func TestCaptureCLIWorkspaceLockAcrossProcesses(t *testing.T) {
	args, r := createFixture(t)
	root, _, err := jobPath(args[2], args[4])
	if err != nil {
		t.Fatal(err)
	}
	lock, err := workspaceLock(root, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, self, "-test.run=^TestCaptureCLILockChild$")
	cmd.Env = append(os.Environ(), "MONTHLY_CAPTURE_LOCK_CHILD="+root)
	if raw, err := cmd.CombinedOutput(); err != nil {
		t.Fatal("cross-process lock failed", string(raw), err)
	}
	if _, err := invoke(t, inspectArgs(args, r, "older")); !errors.Is(err, errBusy) {
		t.Fatal("inspection raced writer", err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := invoke(t, inspectArgs(args, r, "older")); err != nil {
		t.Fatal("released workspace could not resume", err)
	}
}

func TestCaptureCLILockChild(t *testing.T) {
	root := os.Getenv("MONTHLY_CAPTURE_LOCK_CHILD")
	if root == "" {
		t.Skip("private child only")
	}
	if lock, err := workspaceLock(root, true, false); !errors.Is(err, errBusy) {
		if lock != nil {
			_ = lock.Close()
		}
		t.Fatal("writer lock not respected", err)
	}
}
