// Command nginxreopen safely reopens the dedicated Monitor logs of an already
// running Nginx container. It is intended for logrotate's postrotate hook.
//
// It deliberately does not restart/reload the container, edit Nginx config, or
// move/delete logs. A successful exit means the Nginx master and every active
// worker hold the current log inode, no container process holds a rotated log,
// and the container identity/restart count did not change.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	defaultContainer = "nexusapi-nginx"
	defaultCollector = "nexusapi-nginxcollector"
	defaultLogDir    = "/opt/nexusapi/logs/nginx-monitor"
)

type options struct {
	container       string
	collector       string
	logDir          string
	containerLogDir string
	logNames        []string
	lockPath        string
	probeURL        string
	probeStatus     int
	proofPath       string
	logGID          int
	checkOnly       bool
	timeout         time.Duration
}

type containerState struct {
	ID           string
	PID          int
	RestartCount int
	StartedAt    string
}

type workerProcess struct {
	PID    int
	UID    int
	Groups map[int]bool
}

type fileIdentity struct {
	Path  string `json:"path"`
	Dev   uint64 `json:"device"`
	Inode uint64 `json:"inode"`
	Size  int64  `json:"size"`
	UID   uint32 `json:"uid"`
	GID   uint32 `json:"gid"`
	Mode  uint32 `json:"mode"`
}

type result struct {
	OK              bool           `json:"ok"`
	CheckOnly       bool           `json:"check_only"`
	Container       string         `json:"container"`
	ContainerPID    int            `json:"container_pid"`
	WorkerPIDs      []int          `json:"worker_pids"`
	WorkerHostUID   int            `json:"worker_host_uid"`
	CollectorLogGID int            `json:"collector_log_gid"`
	Files           []fileIdentity `json:"files"`
	ProbeUsed       bool           `json:"probe_used"`
	ElapsedMS       int64          `json:"elapsed_ms"`
	Warnings        []string       `json:"warnings,omitempty"`
	ProofPath       string         `json:"proof_path,omitempty"`
	ReleasedInodes  int            `json:"released_inodes,omitempty"`
}

type writerReleaseIdentity struct {
	Name       string `json:"name"`
	Device     uint64 `json:"device"`
	Inode      uint64 `json:"inode"`
	ReleasedAt int64  `json:"released_at,omitempty"`
}

type writerReleaseProof struct {
	Version     int                     `json:"version"`
	GeneratedAt int64                   `json:"generated_at"`
	ContainerID string                  `json:"container_id"`
	StartedAt   string                  `json:"container_started_at"`
	Current     []writerReleaseIdentity `json:"current"`
	Released    []writerReleaseIdentity `json:"released"`
}

// statDeviceID keeps the syscall.Stat_t device conversion portable: Linux
// exposes Dev as uint64 while Darwin uses a narrower signed integer.
func statDeviceID[T ~int32 | ~uint32 | ~uint64](device T) uint64 {
	return uint64(device)
}

func statModeBits[T ~uint16 | ~uint32](mode T) uint32 {
	return uint32(mode)
}

func main() {
	var rawLogs string
	var timeout time.Duration
	opts := options{}
	flag.StringVar(&opts.container, "container", defaultContainer, "Nginx container name")
	flag.StringVar(&opts.collector, "collector-container", defaultCollector, "collector container name")
	flag.StringVar(&opts.logDir, "log-dir", defaultLogDir, "dedicated host log directory")
	flag.StringVar(&opts.containerLogDir, "container-log-dir", "/var/log/nexusapi-monitor", "dedicated log directory as mounted inside Nginx")
	flag.StringVar(&rawLogs, "logs", "nexusapi_access.jsonl,error.log", "comma-separated log basenames")
	flag.StringVar(&opts.lockPath, "lock", "/run/lock/nexusapi-nginx-reopen.lock", "exclusive lock path")
	flag.StringVar(&opts.probeURL, "probe-url", "", "required loopback HTTP status URL unless -check is used")
	flag.IntVar(&opts.probeStatus, "probe-expected-status", http.StatusOK, "exact HTTP status expected from the loopback probe")
	flag.StringVar(&opts.proofPath, "proof", "", "writer-release proof path (default: inside log-dir)")
	flag.IntVar(&opts.logGID, "log-gid", -1, "expected numeric host GID of the dedicated log group")
	flag.BoolVar(&opts.checkOnly, "check", false, "validate only; do not chown, chmod or signal")
	flag.DurationVar(&timeout, "timeout", 20*time.Second, "total operation timeout")
	flag.Parse()
	opts.timeout = timeout
	for _, name := range strings.Split(rawLogs, ",") {
		if name = strings.TrimSpace(name); name != "" {
			opts.logNames = append(opts.logNames, name)
		}
	}
	if opts.proofPath == "" {
		opts.proofPath = filepath.Join(opts.logDir, ".nginx-writer-release-v2.json")
	}
	if err := validateOptions(opts); err != nil {
		fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()
	started := time.Now()
	out, err := run(ctx, opts)
	if err != nil {
		fatal(err)
	}
	out.ElapsedMS = time.Since(started).Milliseconds()
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(out); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	encoded, _ := json.Marshal(map[string]any{"ok": false, "error": err.Error()})
	_, _ = fmt.Fprintln(os.Stderr, string(encoded))
	os.Exit(1)
}

func validateOptions(o options) error {
	if !safeName(o.container) || !safeName(o.collector) {
		return errors.New("container names must use only letters, digits, dot, underscore or dash")
	}
	if !filepath.IsAbs(o.logDir) || !filepath.IsAbs(o.containerLogDir) || !filepath.IsAbs(o.lockPath) || !filepath.IsAbs(o.proofPath) || filepath.Clean(o.logDir) == "/" || filepath.Clean(o.containerLogDir) == "/" {
		return errors.New("log-dir, container-log-dir, lock and proof must be absolute, and log directories cannot be root")
	}
	if filepath.Dir(filepath.Clean(o.proofPath)) != filepath.Clean(o.logDir) {
		return errors.New("proof must be a direct child of log-dir")
	}
	if len(o.logNames) == 0 || len(o.logNames) > 8 {
		return errors.New("one to eight log basenames are required")
	}
	if o.logGID <= 0 {
		return errors.New("log-gid must be an explicit positive numeric host GID")
	}
	if o.probeURL != "" {
		u, err := url.ParseRequestURI(o.probeURL)
		if err != nil || u.Scheme != "http" || u.User != nil || u.Fragment != "" || u.Hostname() == "" {
			return errors.New("probe-url must be an absolute loopback HTTP URL without credentials or fragment")
		}
		host := strings.TrimSpace(strings.TrimSuffix(u.Hostname(), "."))
		ip := net.ParseIP(host)
		if !strings.EqualFold(host, "localhost") && (ip == nil || !ip.IsLoopback()) {
			return errors.New("probe-url must target localhost or a loopback IP")
		}
	}
	if o.probeStatus < 100 || o.probeStatus > 599 {
		return errors.New("probe-expected-status must be between 100 and 599")
	}
	if !o.checkOnly && o.probeURL == "" {
		return errors.New("probe-url is required for a production reopen")
	}
	for _, name := range o.logNames {
		if filepath.Base(name) != name || name == "." || name == ".." || strings.ContainsAny(name, `/\\`) {
			return fmt.Errorf("unsafe log basename %q", name)
		}
	}
	if o.timeout < 2*time.Second || o.timeout > time.Minute {
		return errors.New("timeout must be between 2s and 1m")
	}
	return nil
}

func safeName(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-' {
			continue
		}
		return false
	}
	return true
}

func run(ctx context.Context, o options) (result, error) {
	if os.Geteuid() != 0 {
		return result{}, errors.New("nginx reopen verification must run as root")
	}
	lock, err := acquireLock(o.lockPath)
	if err != nil {
		return result{}, err
	}
	defer func() {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
	}()

	_, dirStat, err := secureDirectory(o.logDir)
	if err != nil {
		return result{}, err
	}
	if int(dirStat.Gid) != o.logGID {
		return result{}, fmt.Errorf("log directory GID %d does not match explicit log-gid %d", dirStat.Gid, o.logGID)
	}
	before, err := inspectContainer(ctx, o.container)
	if err != nil {
		return result{}, err
	}
	master, err := nginxMasterProcess(before.PID)
	if err != nil {
		return result{}, err
	}
	workers, workerUID, err := nginxWorkers(ctx, before.ID)
	if err != nil {
		return result{}, err
	}
	if err := ensureContainerUnchanged(ctx, o.container, before); err != nil {
		return result{}, err
	}
	if err := verifyCollectorAccess(ctx, o.collector, o.logGID, o.logNames); err != nil {
		return result{}, err
	}
	for _, worker := range workers {
		if err := verifyDirectoryTraverse(o.logDir, worker); err != nil {
			return result{}, err
		}
	}
	if err := verifyContainerLogTraverse(ctx, before.ID, workerUID, o.containerLogDir); err != nil {
		return result{}, err
	}
	files, err := inspectLogFiles(o.logDir, o.logNames)
	if err != nil {
		return result{}, err
	}
	if o.checkOnly {
		// A completed Nginx USR1 reopen deliberately transfers the current
		// files to the unprivileged worker user.  Check mode validates that
		// steady-state ownership; it must not require the pre-reopen root
		// ownership used by logrotate's create directive.
		if err := verifyLogPermissions(files, workerUID, o.logGID); err != nil {
			return result{}, err
		}
		if err := verifyWorkerFDs(append([]workerProcess{master}, workers...), files, o.logNames); err != nil {
			return result{}, err
		}
		processes, err := containerProcessPIDs(ctx, before.ID)
		if err != nil {
			return result{}, err
		}
		if err := verifyNoRotatedLogFDs(processes, o.logNames); err != nil {
			return result{}, err
		}
		return result{OK: true, CheckOnly: true, Container: o.container, ContainerPID: before.PID, WorkerPIDs: workerPIDs(workers), WorkerHostUID: workerUID, CollectorLogGID: o.logGID, Files: files}, nil
	}

	// Ownership and mode are deployment invariants. Never repair them in this
	// safety-critical hook: a bad logrotate create rule must fail closed.
	if err := verifyLogPermissions(files, 0, o.logGID); err != nil {
		return result{}, err
	}
	// Revalidate the whole set after all per-file operations. A second rotation
	// between the first and last file must abort before USR1.
	if err := verifyCurrentFileSet(files, 0, o.logGID); err != nil {
		return result{}, err
	}
	if err := ensureContainerUnchanged(ctx, o.container, before); err != nil {
		return result{}, err
	}
	if _, err := command(ctx, "docker", "kill", "-s", "USR1", before.ID); err != nil {
		return result{}, fmt.Errorf("signal nginx master: %w", err)
	}
	if _, err := waitForWorkerFDs(ctx, o.container, before, files, o.logNames, workerUID, o.logGID); err != nil {
		return result{}, err
	}
	after, err := inspectContainer(ctx, before.ID)
	if err != nil {
		return result{}, err
	}
	if before != after {
		return result{}, fmt.Errorf("container identity changed during reopen: before=%+v after=%+v", before, after)
	}
	if err := probeAndVerifyGrowth(ctx, o.probeURL, o.probeStatus, files[0]); err != nil {
		return result{}, err
	}
	// The probe itself can race with a worker replacement or a late writer.
	// Re-establish the full invariant immediately before publishing proof.
	if err := ensureContainerUnchanged(ctx, o.container, before); err != nil {
		return result{}, err
	}
	workers, err = verifyNginxFDState(ctx, o.container, before.PID, files, o.logNames)
	if err != nil {
		return result{}, err
	}
	if err := ensureContainerUnchanged(ctx, o.container, before); err != nil {
		return result{}, err
	}
	released, err := writeWriterReleaseProof(o, before, files, time.Now().Unix())
	if err != nil {
		return result{}, err
	}
	return result{OK: true, Container: o.container, ContainerPID: after.PID, WorkerPIDs: workerPIDs(workers), WorkerHostUID: workerUID, CollectorLogGID: o.logGID, Files: files, ProbeUsed: true, ProofPath: o.proofPath, ReleasedInodes: released}, nil
}

func acquireLock(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || stat.Nlink != 1 || int(stat.Uid) != os.Geteuid() || info.Mode().Perm()&0o077 != 0 {
		_ = f.Close()
		return nil, errors.New("lock must be caller-owned, single-linked and mode 0600")
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, errors.New("another nginx reopen operation is active")
	}
	return f, nil
}

func secureDirectory(path string) (os.FileInfo, *syscall.Stat_t, error) {
	var finalInfo os.FileInfo
	var finalStat *syscall.Stat_t
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return nil, nil, fmt.Errorf("lstat directory %s: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return nil, nil, fmt.Errorf("directory component must be a real directory: %s", current)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 0 {
			return nil, nil, fmt.Errorf("directory component must be root-owned: %s", current)
		}
		if info.Mode().Perm()&0o022 != 0 {
			return nil, nil, fmt.Errorf("directory component must not be group/world writable: %s", current)
		}
		if current == filepath.Clean(path) {
			finalInfo, finalStat = info, stat
		}
		if current == "/" {
			break
		}
	}
	return finalInfo, finalStat, nil
}

func inspectLogFiles(dir string, names []string) ([]fileIdentity, error) {
	out := make([]fileIdentity, 0, len(names))
	for _, name := range names {
		path := filepath.Join(dir, name)
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("lstat %s: %w", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("%s must be a regular non-symlink file", path)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 {
			return nil, fmt.Errorf("%s must have one hard link and a stable inode", path)
		}
		out = append(out, fileIdentity{Path: path, Dev: statDeviceID(stat.Dev), Inode: stat.Ino, Size: info.Size(), UID: stat.Uid, GID: stat.Gid, Mode: statModeBits(stat.Mode & 0o777)})
	}
	return out, nil
}

func inspectRotatedLogIdentities(dir string, names []string) ([]writerReleaseIdentity, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []writerReleaseIdentity
	for _, entry := range entries {
		name := entry.Name()
		matched := false
		for _, base := range names {
			if strings.HasPrefix(name, base+".") {
				matched = true
				break
			}
		}
		if !matched || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		info, err := os.Lstat(filepath.Join(dir, name))
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			continue
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 || stat.Dev == 0 || stat.Ino == 0 {
			return nil, fmt.Errorf("rotated log %s has no safe physical identity", name)
		}
		key := fmt.Sprintf("%d:%d", stat.Dev, stat.Ino)
		if seen[key] {
			return nil, errors.New("rotated logs contain a duplicate physical identity")
		}
		seen[key] = true
		out = append(out, writerReleaseIdentity{Name: name, Device: statDeviceID(stat.Dev), Inode: stat.Ino})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Device != out[j].Device {
			return out[i].Device < out[j].Device
		}
		return out[i].Inode < out[j].Inode
	})
	return out, nil
}

func loadPreviousWriterReleaseProof(path string) (writerReleaseProof, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return writerReleaseProof{}, nil
	}
	if err != nil {
		return writerReleaseProof{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 256<<10 || stat.Nlink != 1 || int(stat.Uid) != os.Geteuid() || info.Mode().Perm()&0o022 != 0 {
		return writerReleaseProof{}, errors.New("existing writer-release proof is unsafe")
	}
	handle, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return writerReleaseProof{}, err
	}
	defer handle.Close()
	opened, err := handle.Stat()
	if err != nil {
		return writerReleaseProof{}, err
	}
	openedStat, ok := opened.Sys().(*syscall.Stat_t)
	if !ok || !opened.Mode().IsRegular() || openedStat.Nlink != 1 || int(openedStat.Uid) != os.Geteuid() || opened.Mode().Perm()&0o022 != 0 || opened.Size() != info.Size() || openedStat.Dev != stat.Dev || openedStat.Ino != stat.Ino {
		return writerReleaseProof{}, errors.New("existing writer-release proof changed while opening")
	}
	var proof writerReleaseProof
	decoder := json.NewDecoder(io.LimitReader(handle, 256<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&proof); err != nil {
		return writerReleaseProof{}, fmt.Errorf("decode existing writer-release proof: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return writerReleaseProof{}, errors.New("existing writer-release proof contains trailing data")
	}
	if proof.Version != 1 || proof.GeneratedAt <= 0 || len(proof.Released) > 1024 {
		return writerReleaseProof{}, errors.New("existing writer-release proof envelope is invalid")
	}
	return proof, nil
}

func buildWriterReleaseProof(o options, state containerState, current []fileIdentity, releasedAt int64) (writerReleaseProof, error) {
	if len(state.ID) != 64 || state.StartedAt == "" || releasedAt <= 0 {
		return writerReleaseProof{}, errors.New("container identity is incomplete")
	}
	previous, err := loadPreviousWriterReleaseProof(o.proofPath)
	if err != nil {
		return writerReleaseProof{}, err
	}
	if previous.GeneratedAt > releasedAt+300 {
		return writerReleaseProof{}, errors.New("existing writer-release proof is from the future")
	}
	rotated, err := inspectRotatedLogIdentities(o.logDir, o.logNames)
	if err != nil {
		return writerReleaseProof{}, err
	}
	proof := writerReleaseProof{Version: 1, GeneratedAt: releasedAt, ContainerID: state.ID, StartedAt: state.StartedAt}
	active := make(map[string]bool, len(current))
	for _, file := range current {
		name := filepath.Base(file.Path)
		key := fmt.Sprintf("%d:%d", file.Dev, file.Inode)
		if active[key] {
			return writerReleaseProof{}, errors.New("current logs share a physical identity")
		}
		proof.Current = append(proof.Current, writerReleaseIdentity{Name: name, Device: file.Dev, Inode: file.Inode})
		active[key] = true
	}
	ledger := make(map[string]writerReleaseIdentity, len(previous.Released)+len(rotated))
	for _, identity := range previous.Released {
		if identity.Device == 0 || identity.Inode == 0 || identity.ReleasedAt <= 0 || filepath.Base(identity.Name) != identity.Name {
			return writerReleaseProof{}, errors.New("existing writer-release ledger contains an invalid identity")
		}
		key := fmt.Sprintf("%d:%d", identity.Device, identity.Inode)
		if _, duplicate := ledger[key]; duplicate {
			return writerReleaseProof{}, errors.New("existing writer-release ledger repeats a physical identity")
		}
		identity.ReleasedAt = releasedAt
		ledger[key] = identity
	}
	for _, identity := range rotated {
		key := fmt.Sprintf("%d:%d", identity.Device, identity.Inode)
		if active[key] {
			return writerReleaseProof{}, errors.New("current and rotated logs share a physical identity")
		}
		identity.ReleasedAt = releasedAt
		ledger[key] = identity
	}
	for key, identity := range ledger {
		if !active[key] {
			proof.Released = append(proof.Released, identity)
		}
	}
	sort.Slice(proof.Current, func(i, j int) bool { return proof.Current[i].Name < proof.Current[j].Name })
	sort.Slice(proof.Released, func(i, j int) bool {
		if proof.Released[i].ReleasedAt != proof.Released[j].ReleasedAt {
			return proof.Released[i].ReleasedAt < proof.Released[j].ReleasedAt
		}
		if proof.Released[i].Device != proof.Released[j].Device {
			return proof.Released[i].Device < proof.Released[j].Device
		}
		return proof.Released[i].Inode < proof.Released[j].Inode
	})
	if len(proof.Released) > 1024 {
		proof.Released = append([]writerReleaseIdentity(nil), proof.Released[len(proof.Released)-1024:]...)
	}
	return proof, nil
}

func writeWriterReleaseProof(o options, state containerState, current []fileIdentity, releasedAt int64) (int, error) {
	proof, err := buildWriterReleaseProof(o, state, current, releasedAt)
	if err != nil {
		return 0, err
	}
	data, err := json.Marshal(proof)
	if err != nil {
		return 0, err
	}
	temp, err := os.CreateTemp(o.logDir, ".nginx-writer-release-v2.tmp-")
	if err != nil {
		return 0, err
	}
	tempPath := temp.Name()
	clean := func() {
		_ = temp.Close()
		_ = os.Remove(tempPath)
	}
	if err := temp.Chown(os.Geteuid(), o.logGID); err != nil {
		clean()
		return 0, err
	}
	if err := temp.Chmod(0o640); err != nil {
		clean()
		return 0, err
	}
	if _, err := temp.Write(data); err != nil {
		clean()
		return 0, err
	}
	if err := temp.Sync(); err != nil {
		clean()
		return 0, err
	}
	if err := temp.Close(); err != nil {
		_ = os.Remove(tempPath)
		return 0, err
	}
	if err := os.Rename(tempPath, o.proofPath); err != nil {
		_ = os.Remove(tempPath)
		return 0, err
	}
	dir, err := os.Open(o.logDir)
	if err != nil {
		return 0, err
	}
	err = dir.Sync()
	_ = dir.Close()
	if err != nil {
		return 0, err
	}
	return len(proof.Released), nil
}

func verifyLogPermissions(files []fileIdentity, uid, gid int) error {
	for _, file := range files {
		info, err := os.Lstat(file.Path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("cannot safely inspect current log %s", file.Path)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || int(stat.Uid) != uid || int(stat.Gid) != gid || stat.Mode&0o777 != 0o640 {
			return fmt.Errorf("current log %s must be owned by %d:%d with mode 0640", file.Path, uid, gid)
		}
	}
	return nil
}

func verifyCurrentFileSet(expected []fileIdentity, uid, gid int) error {
	for _, file := range expected {
		info, err := os.Lstat(file.Path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("current log disappeared or changed type: %s", file.Path)
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || statDeviceID(stat.Dev) != file.Dev || stat.Ino != file.Inode || stat.Nlink != 1 {
			return fmt.Errorf("current log rotated during preparation: %s", file.Path)
		}
		if int(stat.Uid) != uid || int(stat.Gid) != gid || stat.Mode&0o777 != 0o640 {
			return fmt.Errorf("current log permissions changed during preparation: %s", file.Path)
		}
	}
	return nil
}

func inspectContainer(ctx context.Context, name string) (containerState, error) {
	out, err := command(ctx, "docker", "inspect", "-f", `{{.Id}} {{.State.Pid}} {{.RestartCount}} {{.State.StartedAt}}`, name)
	if err != nil {
		return containerState{}, fmt.Errorf("inspect %s: %w", name, err)
	}
	parts := strings.Fields(strings.TrimSpace(out))
	if len(parts) != 4 || len(parts[0]) != 64 {
		return containerState{}, fmt.Errorf("unexpected docker inspect output for %s", name)
	}
	pid, err1 := strconv.Atoi(parts[1])
	restarts, err2 := strconv.Atoi(parts[2])
	if err1 != nil || err2 != nil || pid <= 0 {
		return containerState{}, fmt.Errorf("invalid docker state for %s", name)
	}
	return containerState{ID: parts[0], PID: pid, RestartCount: restarts, StartedAt: parts[3]}, nil
}

func ensureContainerUnchanged(ctx context.Context, name string, expected containerState) error {
	byName, err := inspectContainer(ctx, name)
	if err != nil {
		return err
	}
	byID, err := inspectContainer(ctx, expected.ID)
	if err != nil {
		return err
	}
	if byName != expected || byID != expected {
		return fmt.Errorf("container identity changed: expected=%+v name=%+v id=%+v", expected, byName, byID)
	}
	return nil
}

func nginxWorkers(ctx context.Context, container string) ([]workerProcess, int, error) {
	out, err := command(ctx, "docker", "top", container, "-eo", "pid,args")
	if err != nil {
		return nil, 0, fmt.Errorf("list nginx workers: %w", err)
	}
	var workers []workerProcess
	uid := -1
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, "nginx: worker process") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil || pid <= 0 {
			return nil, 0, fmt.Errorf("invalid nginx worker row %q", line)
		}
		worker, err := processIdentity(pid)
		if err != nil {
			return nil, 0, err
		}
		if worker.UID <= 0 {
			return nil, 0, errors.New("nginx workers must run as a non-root host UID")
		}
		if uid != -1 && uid != worker.UID {
			return nil, 0, errors.New("nginx workers use different host effective UIDs")
		}
		uid = worker.UID
		workers = append(workers, worker)
	}
	if len(workers) == 0 || uid <= 0 {
		return nil, 0, errors.New("no active nginx worker processes found")
	}
	return workers, uid, nil
}

func nginxMasterProcess(pid int) (workerProcess, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return workerProcess{}, fmt.Errorf("read container PID1 command line: %w", err)
	}
	commandLine := strings.TrimSpace(strings.ReplaceAll(string(data), "\x00", " "))
	if !strings.HasPrefix(commandLine, "nginx: master process") {
		return workerProcess{}, fmt.Errorf("container PID1 %d is not an nginx master process", pid)
	}
	master, err := processIdentity(pid)
	if err != nil {
		return workerProcess{}, err
	}
	if master.UID != 0 {
		return workerProcess{}, fmt.Errorf("nginx master PID %d must run as root", pid)
	}
	return master, nil
}

func containerProcessPIDs(ctx context.Context, container string) ([]int, error) {
	out, err := command(ctx, "docker", "top", container, "-eo", "pid,args")
	if err != nil {
		return nil, fmt.Errorf("list container processes: %w", err)
	}
	seen := map[int]bool{}
	var pids []int
	nonEmptyLine := 0
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			if nonEmptyLine != 0 || !strings.EqualFold(fields[0], "PID") {
				return nil, fmt.Errorf("unexpected docker top row %q", line)
			}
			nonEmptyLine++
			continue
		}
		nonEmptyLine++
		if pid <= 0 || seen[pid] {
			return nil, fmt.Errorf("invalid or duplicate container process PID in %q", line)
		}
		seen[pid] = true
		pids = append(pids, pid)
	}
	if len(pids) == 0 {
		return nil, errors.New("container has no inspectable processes")
	}
	sort.Ints(pids)
	return pids, nil
}

func processIdentity(pid int) (workerProcess, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return workerProcess{}, fmt.Errorf("read worker %d status: %w", pid, err)
	}
	out := workerProcess{PID: pid, UID: -1, Groups: map[int]bool{}}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		switch {
		case strings.HasPrefix(line, "Uid:") && len(fields) >= 3:
			out.UID, _ = strconv.Atoi(fields[2])
		case strings.HasPrefix(line, "Gid:") && len(fields) >= 3:
			if gid, err := strconv.Atoi(fields[2]); err == nil {
				out.Groups[gid] = true
			}
		case strings.HasPrefix(line, "Groups:"):
			for _, raw := range fields[1:] {
				if gid, err := strconv.Atoi(raw); err == nil {
					out.Groups[gid] = true
				}
			}
		}
	}
	if out.UID < 0 || len(out.Groups) == 0 {
		return workerProcess{}, fmt.Errorf("cannot determine worker %d runtime identity", pid)
	}
	return out, nil
}

func workerPIDs(workers []workerProcess) []int {
	out := make([]int, len(workers))
	for i, worker := range workers {
		out[i] = worker.PID
	}
	return out
}

func verifyCollectorAccess(ctx context.Context, collector string, gid int, logNames []string) error {
	out, err := command(ctx, "docker", "inspect", "-f", `{{json .HostConfig.GroupAdd}}`, collector)
	if err != nil {
		return fmt.Errorf("inspect collector supplemental groups: %w", err)
	}
	var groups []string
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &groups); err != nil {
		return fmt.Errorf("decode collector supplemental groups: %w", err)
	}
	want := strconv.Itoa(gid)
	for _, group := range groups {
		if group == want {
			for _, name := range logNames {
				if _, err := command(ctx, "docker", "exec", collector, "test", "-r", "/logs/"+name); err != nil {
					return fmt.Errorf("collector cannot read %s: %w", name, err)
				}
			}
			return nil
		}
	}
	return fmt.Errorf("collector does not have required log GID %d", gid)
}

func verifyDirectoryTraverse(path string, worker workerProcess) error {
	info, err := os.Lstat(filepath.Clean(path))
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("unsafe log directory %s", path)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("cannot inspect log directory %s", path)
	}
	perm := info.Mode().Perm()
	// Only the bind-mounted directory itself is shared with Nginx. Host parent
	// directories such as /var/lib/docker are outside the container mount
	// namespace and must not be evaluated as worker traversal requirements.
	canTraverse := false
	switch {
	case stat.Uid == uint32(worker.UID):
		canTraverse = perm&0o100 != 0
	case worker.Groups[int(stat.Gid)]:
		canTraverse = perm&0o010 != 0
	default:
		canTraverse = perm&0o001 != 0
	}
	if !canTraverse {
		return fmt.Errorf("nginx worker PID %d UID %d cannot traverse mounted log directory %s", worker.PID, worker.UID, path)
	}
	return nil
}

func verifyContainerLogTraverse(ctx context.Context, container string, workerUID int, path string) error {
	if _, err := command(ctx, "docker", "exec", "--user", strconv.Itoa(workerUID), container, "test", "-x", path); err != nil {
		return fmt.Errorf("nginx worker UID %d cannot traverse container log directory %s: %w", workerUID, path, err)
	}
	return nil
}

func waitForWorkerFDs(ctx context.Context, container string, expected containerState, files []fileIdentity, logNames []string, workerUID, logGID int) ([]workerProcess, error) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	stable := 0
	lastSet := ""
	for {
		if err := ensureContainerUnchanged(ctx, container, expected); err != nil {
			return nil, err
		}
		workers, err := verifyNginxFDState(ctx, expected.ID, expected.PID, files, logNames)
		if err == nil {
			err = verifyCurrentFileSet(files, workerUID, logGID)
		}
		set := fmt.Sprint(workerPIDs(workers))
		if err == nil && set == lastSet {
			stable++
		} else if err == nil {
			stable, lastSet = 1, set
		} else {
			stable, lastSet = 0, ""
		}
		if stable >= 3 {
			return workers, nil
		}
		select {
		case <-ctx.Done():
			return nil, errors.New("nginx workers did not stably reopen every current log inode before timeout")
		case <-ticker.C:
		}
	}
}

func verifyNginxFDState(ctx context.Context, container string, masterPID int, files []fileIdentity, logNames []string) ([]workerProcess, error) {
	master, err := nginxMasterProcess(masterPID)
	if err != nil {
		return nil, err
	}
	workers, _, err := nginxWorkers(ctx, container)
	if err != nil {
		return nil, err
	}
	if err := verifyWorkerFDs(append([]workerProcess{master}, workers...), files, logNames); err != nil {
		return nil, err
	}
	processes, err := containerProcessPIDs(ctx, container)
	if err != nil {
		return nil, err
	}
	if err := verifyNoRotatedLogFDs(processes, logNames); err != nil {
		return nil, err
	}
	return workers, nil
}

func verifyNoRotatedLogFDs(pids []int, logNames []string) error {
	for _, pid := range pids {
		entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
		if err != nil {
			return fmt.Errorf("read container process %d descriptors: %w", pid, err)
		}
		for _, entry := range entries {
			target, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, entry.Name()))
			if err != nil {
				continue
			}
			for _, name := range logNames {
				base := filepath.Base(strings.TrimSuffix(target, " (deleted)"))
				if strings.HasPrefix(base, name+".") || strings.HasSuffix(target, name+" (deleted)") {
					return fmt.Errorf("container process %d still holds rotated log %s", pid, target)
				}
			}
		}
	}
	return nil
}

func verifyWorkerFDs(workers []workerProcess, files []fileIdentity, logNames []string) error {
	for _, worker := range workers {
		pid := worker.PID
		entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
		if err != nil {
			return fmt.Errorf("read worker %d descriptors: %w", pid, err)
		}
		found := make(map[string]bool, len(files))
		for _, entry := range entries {
			fdPath := fmt.Sprintf("/proc/%d/fd/%s", pid, entry.Name())
			target, _ := os.Readlink(fdPath)
			for _, name := range logNames {
				base := filepath.Base(strings.TrimSuffix(target, " (deleted)"))
				if strings.HasPrefix(base, name+".") || strings.HasSuffix(target, name+" (deleted)") {
					return fmt.Errorf("worker %d still holds rotated log %s", pid, target)
				}
			}
			info, err := os.Stat(fdPath)
			if err != nil {
				continue
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok {
				continue
			}
			for _, file := range files {
				if statDeviceID(stat.Dev) == file.Dev && stat.Ino == file.Inode {
					if err := verifyWritableAppendFD(pid, entry.Name()); err != nil {
						return err
					}
					found[file.Path] = true
				}
			}
		}
		for _, file := range files {
			if !found[file.Path] {
				return fmt.Errorf("worker %d has not opened current inode for %s", pid, file.Path)
			}
		}
	}
	return nil
}

func verifyWritableAppendFD(pid int, fd string) error {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/fdinfo/%s", pid, fd))
	if err != nil {
		return fmt.Errorf("read worker %d fd %s flags: %w", pid, fd, err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "flags:") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 2 {
			break
		}
		flags, err := strconv.ParseUint(parts[1], 8, 64)
		if err != nil {
			break
		}
		writable := flags&uint64(syscall.O_WRONLY) != 0 || flags&uint64(syscall.O_RDWR) != 0
		if !writable || flags&uint64(syscall.O_APPEND) == 0 {
			return fmt.Errorf("worker %d fd %s is not writable append", pid, fd)
		}
		return nil
	}
	return fmt.Errorf("worker %d fd %s has no valid flags", pid, fd)
}

func probeAndVerifyGrowth(ctx context.Context, rawURL string, expectedStatus int, access fileIdentity) error {
	baseline, err := inspectLogFiles(filepath.Dir(access.Path), []string{filepath.Base(access.Path)})
	if err != nil || len(baseline) != 1 || baseline[0].Dev != access.Dev || baseline[0].Inode != access.Inode {
		return errors.New("access log identity changed before local probe")
	}
	access = baseline[0]
	resp, err := performProbe(ctx, rawURL)
	if err != nil {
		return fmt.Errorf("local probe failed: %w", err)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	if resp.StatusCode != expectedStatus {
		return fmt.Errorf("local probe returned HTTP %d, expected %d", resp.StatusCode, expectedStatus)
	}
	deadline := time.NewTicker(100 * time.Millisecond)
	defer deadline.Stop()
	for {
		current, err := inspectLogFiles(filepath.Dir(access.Path), []string{filepath.Base(access.Path)})
		if err == nil && len(current) == 1 && current[0].Dev == access.Dev && current[0].Inode == access.Inode && current[0].Size > access.Size {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.New("local probe succeeded but no current log grew")
		case <-deadline.C:
		}
	}
}

var performProbe = func(ctx context.Context, rawURL string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build local probe: %w", err)
	}
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return errors.New("probe redirect refused") }}
	return client.Do(req)
}

var command = func(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}
