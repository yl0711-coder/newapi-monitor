// Command nginxcursoraudit reconstructs the last acknowledged byte boundary
// of a legacy nginxcollector file from the immutable ingest batch IDs retained
// by Monitor. It is deliberately read-only: it never edits logs or cursors.
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
)

const defaultMaxLines = 1000

var safeNode = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

type options struct {
	node        string
	kind        string
	logPath     string
	batchIDs    string
	lastBatchID string
	maxLines    int
	baseOffset  int64
}

type lineRecord struct {
	digest [sha256.Size]byte
	end    int64
}

type auditResult struct {
	OK                  bool   `json:"ok"`
	Node                string `json:"node"`
	Kind                string `json:"kind"`
	Path                string `json:"path"`
	Device              uint64 `json:"device"`
	Inode               uint64 `json:"inode"`
	FileSize            int64  `json:"file_size"`
	BaseOffset          int64  `json:"base_offset"`
	ResumeOffset        int64  `json:"resume_offset"`
	UnacknowledgedBytes int64  `json:"unacknowledged_bytes"`
	MatchedBatches      int    `json:"matched_batches"`
	LastBatchID         string `json:"last_batch_id"`
}

func main() {
	opts := options{}
	flag.StringVar(&opts.node, "node", "", "collector node name")
	flag.StringVar(&opts.kind, "kind", "", "access or error")
	flag.StringVar(&opts.logPath, "log", "", "absolute immutable rotated log path")
	flag.StringVar(&opts.batchIDs, "batch-ids", "-", "file containing one retained batch ID per line, or - for stdin")
	flag.StringVar(&opts.lastBatchID, "last-batch-id", "", "latest retained data batch ID that must be reached")
	flag.IntVar(&opts.maxLines, "max-lines", defaultMaxLines, "collector max lines per batch")
	flag.Int64Var(&opts.baseOffset, "base-offset", 0, "known byte offset immediately before the first supplied batch")
	flag.Parse()
	result, err := run(opts)
	if err != nil {
		encoded, _ := json.Marshal(map[string]any{"ok": false, "error": err.Error()})
		_, _ = fmt.Fprintln(os.Stderr, string(encoded))
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(opts options) (auditResult, error) {
	if !safeNode.MatchString(opts.node) {
		return auditResult{}, errors.New("node must use only letters, digits, dot, underscore or dash")
	}
	if opts.kind != "access" && opts.kind != "error" {
		return auditResult{}, errors.New("kind must be access or error")
	}
	if !filepath.IsAbs(opts.logPath) || filepath.Clean(opts.logPath) == "/" {
		return auditResult{}, errors.New("log must be an absolute non-root path")
	}
	if opts.maxLines < 1 || opts.maxLines > 2000 {
		return auditResult{}, errors.New("max-lines must be between 1 and 2000")
	}
	if !validBatchID(opts.lastBatchID) {
		return auditResult{}, errors.New("last-batch-id must be exactly 64 lowercase hexadecimal characters")
	}
	ids, err := loadBatchIDs(opts.batchIDs)
	if err != nil {
		return auditResult{}, err
	}
	if !ids[opts.lastBatchID] {
		return auditResult{}, errors.New("last-batch-id is absent from the supplied retained batch IDs")
	}
	file, info, stat, err := openImmutableLog(opts.logPath)
	if err != nil {
		return auditResult{}, err
	}
	defer file.Close()
	if opts.baseOffset < 0 || opts.baseOffset > info.Size() {
		return auditResult{}, errors.New("base-offset is outside the log")
	}
	if _, err := file.Seek(opts.baseOffset, io.SeekStart); err != nil {
		return auditResult{}, err
	}
	resume, matched, err := reconstruct(file, opts.node, opts.kind, uint64(stat.Ino), opts.baseOffset, opts.maxLines, ids, opts.lastBatchID)
	if err != nil {
		return auditResult{}, err
	}
	if err := verifyUnchangedLog(opts.logPath, info, stat); err != nil {
		return auditResult{}, err
	}
	return auditResult{OK: true, Node: opts.node, Kind: opts.kind, Path: opts.logPath,
		Device: uint64(stat.Dev), Inode: uint64(stat.Ino), FileSize: info.Size(), BaseOffset: opts.baseOffset,
		ResumeOffset: resume, UnacknowledgedBytes: info.Size() - resume, MatchedBatches: matched, LastBatchID: opts.lastBatchID}, nil
}

func verifyUnchangedLog(path string, expected os.FileInfo, expectedStat *syscall.Stat_t) error {
	current, err := os.Lstat(path)
	if err != nil {
		return err
	}
	currentStat, ok := current.Sys().(*syscall.Stat_t)
	if !ok || current.Mode()&os.ModeSymlink != 0 || !current.Mode().IsRegular() || currentStat.Nlink != 1 ||
		currentStat.Dev != expectedStat.Dev || currentStat.Ino != expectedStat.Ino || current.Size() != expected.Size() || !current.ModTime().Equal(expected.ModTime()) {
		return errors.New("log changed during audit; stop writers and retry")
	}
	return nil
}

func openImmutableLog(path string) (*os.File, os.FileInfo, *syscall.Stat_t, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, nil, nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, nil, nil, errors.New("log must be a regular non-symlink file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return nil, nil, nil, errors.New("log identity is unavailable or multiply linked")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, err
	}
	opened, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, nil, err
	}
	openedStat, ok := opened.Sys().(*syscall.Stat_t)
	if !ok || openedStat.Dev != stat.Dev || openedStat.Ino != stat.Ino || opened.Size() != info.Size() {
		file.Close()
		return nil, nil, nil, errors.New("log changed while it was being opened")
	}
	return file, info, stat, nil
}

func loadBatchIDs(path string) (map[string]bool, error) {
	var reader io.Reader = os.Stdin
	var file *os.File
	if path != "-" {
		if !filepath.IsAbs(path) {
			return nil, errors.New("batch-ids must be an absolute path or -")
		}
		var err error
		file, err = os.Open(path)
		if err != nil {
			return nil, err
		}
		defer file.Close()
		reader = file
	}
	ids := make(map[string]bool)
	scanner := bufio.NewScanner(io.LimitReader(reader, 64<<20))
	for scanner.Scan() {
		id := strings.TrimSpace(scanner.Text())
		if id == "" {
			continue
		}
		if !validBatchID(id) {
			return nil, fmt.Errorf("invalid batch ID %q", id)
		}
		ids[id] = true
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, errors.New("no retained batch IDs supplied")
	}
	return ids, nil
}

func validBatchID(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func reconstruct(reader io.Reader, node, kind string, inode uint64, baseOffset int64, maxLines int, ids map[string]bool, target string) (int64, int, error) {
	buffered := make([]lineRecord, 0, maxLines)
	stream := bufio.NewReaderSize(reader, 64<<10)
	readOffset, resume, matched := baseOffset, baseOffset, 0
	eof := false
	for {
		for len(buffered) < maxLines && !eof {
			line, err := stream.ReadBytes('\n')
			switch {
			case err == nil:
				readOffset += int64(len(line))
				buffered = append(buffered, lineRecord{digest: sha256.Sum256(line), end: readOffset})
			case errors.Is(err, io.EOF):
				// A partial final line was never acknowledged by nginxcollector.
				eof = true
			default:
				return 0, matched, err
			}
		}
		if len(buffered) == 0 {
			return 0, matched, errors.New("reached EOF before last-batch-id")
		}
		content := sha256.New()
		matchAt, matchedID := 0, ""
		for i, line := range buffered {
			_, _ = content.Write(line.digest[:])
			id := legacyBatchID(node, kind, inode, resume, line.end, content.Sum(nil))
			if ids[id] {
				matchAt, matchedID = i+1, id
				break
			}
		}
		if matchAt == 0 {
			return 0, matched, fmt.Errorf("cannot prove the next batch boundary at offset %d", resume)
		}
		resume = buffered[matchAt-1].end
		buffered = buffered[matchAt:]
		delete(ids, matchedID)
		matched++
		if matchedID == target {
			return resume, matched, nil
		}
	}
}

func legacyBatchID(node, kind string, inode uint64, start, end int64, contentDigest []byte) string {
	identity := sha256.New()
	if kind == "error" {
		_, _ = fmt.Fprintf(identity, "error:%s:%d:%d:%d:", node, inode, start, end)
	} else {
		_, _ = fmt.Fprintf(identity, "%s:%d:%d:%d:", node, inode, start, end)
	}
	_, _ = identity.Write(contentDigest)
	return hex.EncodeToString(identity.Sum(nil))
}
