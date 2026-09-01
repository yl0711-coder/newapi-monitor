package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateOptionsRejectsUnsafeTargets(t *testing.T) {
	base := options{container: defaultContainer, collector: defaultCollector, logDir: "/safe/logs", containerLogDir: "/var/log/safe", logNames: []string{"access.log"}, lockPath: "/tmp/reopen.lock", proofPath: "/safe/logs/.nginx-writer-release-v2.json", logGID: 987, probeStatus: 200, checkOnly: true, timeout: 20 * time.Second}
	for _, mutate := range []func(*options){
		func(o *options) { o.container = "nginx;reboot" },
		func(o *options) { o.logDir = "/" },
		func(o *options) { o.containerLogDir = "/" },
		func(o *options) { o.logNames = []string{"../access.log"} },
		func(o *options) { o.lockPath = "relative.lock" },
		func(o *options) { o.proofPath = "/another-dir/proof.json" },
		func(o *options) { o.logGID = 0 },
		func(o *options) { o.probeURL = "https://example.com/status" },
		func(o *options) { o.probeStatus = 99 },
	} {
		value := base
		value.logNames = append([]string(nil), base.logNames...)
		mutate(&value)
		if err := validateOptions(value); err == nil {
			t.Fatalf("unsafe options must be rejected: %+v", value)
		}
	}
}

func TestValidateOptionsRequiresLoopbackProbeForReopen(t *testing.T) {
	value := options{container: defaultContainer, collector: defaultCollector, logDir: "/safe/logs", containerLogDir: "/var/log/safe", logNames: []string{"access.log"}, lockPath: "/tmp/reopen.lock", proofPath: "/safe/logs/.nginx-writer-release-v2.json", logGID: 987, probeStatus: 200, timeout: 20 * time.Second}
	if err := validateOptions(value); err == nil {
		t.Fatal("a production reopen without a loopback probe must be rejected")
	}
	value.probeURL = "http://127.0.0.1:18080/health"
	if err := validateOptions(value); err != nil {
		t.Fatalf("safe production options rejected: %v", err)
	}
}

func TestEnsureContainerUnchangedRejectsNameRemap(t *testing.T) {
	original := command
	t.Cleanup(func() { command = original })
	oldID := testContainerID
	newID := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	command = func(_ context.Context, name string, args ...string) (string, error) {
		if name != "docker" || len(args) != 4 || args[0] != "inspect" {
			return "", fmt.Errorf("unexpected command: %s %v", name, args)
		}
		switch args[3] {
		case "nexusapi-nginx":
			return fmt.Sprintf("%s 202 0 2026-08-31T00:01:00Z", newID), nil
		case oldID:
			return fmt.Sprintf("%s 101 0 2026-08-31T00:00:00Z", oldID), nil
		default:
			return "", fmt.Errorf("unknown container %s", args[3])
		}
	}
	expected := containerState{ID: oldID, PID: 101, RestartCount: 0, StartedAt: "2026-08-31T00:00:00Z"}
	if err := ensureContainerUnchanged(context.Background(), "nexusapi-nginx", expected); err == nil {
		t.Fatal("a container-name remap must fail even while the old container ID remains inspectable")
	}
}

func TestContainerProcessPIDsFailsClosedOnUnexpectedRows(t *testing.T) {
	original := command
	t.Cleanup(func() { command = original })
	command = func(_ context.Context, _ string, _ ...string) (string, error) {
		return "PID COMMAND\n101 nginx: master process\nnot-a-pid helper\n", nil
	}
	if _, err := containerProcessPIDs(context.Background(), "nexusapi-nginx"); err == nil {
		t.Fatal("unexpected docker top rows must fail closed")
	}
	command = func(_ context.Context, _ string, _ ...string) (string, error) {
		return "PID COMMAND\n101 nginx: master process\n202 nginx: worker process\n", nil
	}
	pids, err := containerProcessPIDs(context.Background(), "nexusapi-nginx")
	if err != nil || len(pids) != 2 || pids[0] != 101 || pids[1] != 202 {
		t.Fatalf("valid docker top output rejected: pids=%v err=%v", pids, err)
	}
}

func TestBuildWriterReleaseProofAccumulatesReleasedInodes(t *testing.T) {
	dir := t.TempDir()
	access := filepath.Join(dir, "access.log")
	rotated := filepath.Join(dir, "access.log.1")
	if err := os.WriteFile(access, []byte("current\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rotated, []byte("old\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	files, err := inspectLogFiles(dir, []string{"access.log"})
	if err != nil {
		t.Fatal(err)
	}
	opts := options{logDir: dir, logNames: []string{"access.log"}, proofPath: filepath.Join(dir, ".nginx-writer-release-v2.json"), logGID: os.Getgid()}
	state := containerState{ID: testContainerID, StartedAt: "2026-08-31T00:00:00Z"}
	proof, err := buildWriterReleaseProof(opts, state, files, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(proof.Current) != 1 || len(proof.Released) != 1 || proof.Released[0].ReleasedAt != 100 {
		t.Fatalf("unexpected proof: %+v", proof)
	}
	encoded, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(opts.proofPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(rotated); err != nil {
		t.Fatal(err)
	}
	refreshed, err := buildWriterReleaseProof(opts, state, files, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(refreshed.Released) != 1 || refreshed.Released[0].ReleasedAt != 200 {
		t.Fatalf("a fresh verified reopen must refresh prior release evidence: %+v", refreshed)
	}
}

const testContainerID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestInspectLogFilesRejectsSymlinkAndHardlink(t *testing.T) {
	dir := t.TempDir()
	real := filepath.Join(dir, "access.log")
	if err := os.WriteFile(real, []byte("ok"), 0o640); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "error.log")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectLogFiles(dir, []string{"error.log"}); err == nil {
		t.Fatal("symlink log must be rejected")
	}
	hard := filepath.Join(dir, "hard.log")
	if err := os.Link(real, hard); err != nil {
		t.Fatal(err)
	}
	if _, err := inspectLogFiles(dir, []string{"access.log"}); err == nil {
		t.Fatal("multiply-linked log must be rejected")
	}
}

func TestAcquireLockIsExclusive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reopen.lock")
	first, err := acquireLock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := acquireLock(path); err == nil {
		t.Fatal("second process must not acquire the same lock")
	}
}

func TestVerifyLogPermissionsDoesNotRepairUnsafeMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log")
	if err := os.WriteFile(path, []byte("line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	files, err := inspectLogFiles(dir, []string{"access.log"})
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyLogPermissions(files, os.Getuid(), os.Getgid()); err == nil {
		t.Fatal("unsafe create mode must fail closed instead of being repaired")
	}
}

func TestVerifyLogPermissionsSeparatesPreAndPostReopenOwners(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log")
	if err := os.WriteFile(path, []byte("line\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	files, err := inspectLogFiles(dir, []string{"access.log"})
	if err != nil {
		t.Fatal(err)
	}
	uid, gid := os.Getuid(), os.Getgid()
	if err := verifyLogPermissions(files, uid, gid); err != nil {
		t.Fatalf("steady-state worker ownership rejected: %v", err)
	}
	wrongUID := uid + 1
	if wrongUID == 0 {
		wrongUID++
	}
	if err := verifyLogPermissions(files, wrongUID, gid); err == nil {
		t.Fatal("a file owned by a different user must be rejected")
	}
	if err := verifyCurrentFileSet(files, uid, gid); err != nil {
		t.Fatalf("unchanged file set rejected: %v", err)
	}
	if err := verifyCurrentFileSet(files, wrongUID, gid); err == nil {
		t.Fatal("current-file validation must enforce the phase-specific owner")
	}
}

func TestProbeAcceptsExplicitOriginLockStatusAndRequiresGrowth(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log")
	if err := os.WriteFile(path, nil, 0o640); err != nil {
		t.Fatal(err)
	}
	files, err := inspectLogFiles(dir, []string{"access.log"})
	if err != nil {
		t.Fatal(err)
	}
	originalProbe := performProbe
	t.Cleanup(func() { performProbe = originalProbe })
	performProbe = func(_ context.Context, _ string) (*http.Response, error) {
		file, openErr := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if openErr != nil {
			return nil, openErr
		}
		_, _ = file.WriteString("probe\n")
		_ = file.Close()
		return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader("forbidden"))}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := probeAndVerifyGrowth(ctx, "http://127.0.0.1/api/status", http.StatusForbidden, files[0]); err != nil {
		t.Fatalf("explicit origin-lock status rejected: %v", err)
	}
	refreshed, err := inspectLogFiles(dir, []string{"access.log"})
	if err != nil {
		t.Fatal(err)
	}
	if err := probeAndVerifyGrowth(ctx, "http://127.0.0.1/api/status", http.StatusOK, refreshed[0]); err == nil {
		t.Fatal("unexpected probe status must fail closed")
	}
}

func TestSafeName(t *testing.T) {
	for _, good := range []string{"nexusapi-nginx", "nginx_1", "node.example"} {
		if !safeName(good) {
			t.Fatalf("valid name rejected: %s", good)
		}
	}
	for _, bad := range []string{"", "nginx worker", "nginx;true", "../nginx"} {
		if safeName(bad) {
			t.Fatalf("unsafe name accepted: %s", bad)
		}
	}
}
