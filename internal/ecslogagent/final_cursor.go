package ecslogagent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func readFinalCursor(path string, out any) error {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Size() > 8<<20 {
		return errors.New("invalid final cursor file")
	}
	b, err := io.ReadAll(io.LimitReader(f, (8<<20)+1))
	if err != nil {
		return err
	}
	if len(b) > 8<<20 {
		return errors.New("final cursor budget exceeded")
	}
	return json.Unmarshal(b, out)
}

func (a *Agent) checkFinalCursor(f ecsarchive.FinalFile) error {
	if a.cfg.Kind == "reject" {
		return a.checkFinalRejectCursors(map[string]ecsarchive.FinalFile{f.Name: f})
	}
	var cur struct {
		Version         int    `json:"version"`
		Device          uint64 `json:"device"`
		Inode           uint64 `json:"inode"`
		Offset          int64  `json:"offset"`
		Discontinuities int64  `json:"discontinuities"`
		Discarded       int64  `json:"discarded_lines"`
		Dropped         int64  `json:"evidence_dropped_events"`
		Failed          int64  `json:"evidence_persist_failures"`
		Rejected        int64  `json:"evidence_parse_rejected"`
	}
	name := "access-cursor.json"
	if f.Name == "error.log" {
		name = "error-cursor.json"
	}
	if err := readFinalCursor(filepath.Join(a.dir, name), &cur); err != nil {
		return err
	}
	// Legacy V1 clears Device after ACK. Preserve that format; the immutable
	// source directory binding and before/after file snapshots retain device
	// identity here. A nonzero cursor device must still match exactly.
	if cur.Version != 1 || cur.Inode != f.Inode || (cur.Device != 0 && cur.Device != f.Device) || cur.Offset != f.Size || cur.Discontinuities != 0 || cur.Discarded != 0 || cur.Dropped != 0 || cur.Failed != 0 || cur.Rejected != 0 {
		return errors.New("nginx final cursor or gap mismatch")
	}
	return nil
}

// Compare the whole inode-keyed inventory once. A cursor for a disappeared
// file must not be ignored, even if its last observed offset was at EOF.
func (a *Agent) checkFinalRejectCursors(files map[string]ecsarchive.FinalFile) error {
	var state struct {
		Version     int             `json:"version"`
		Pending     json.RawMessage `json:"pending"`
		PendingHash string          `json:"pending_hash"`
		Files       map[string]struct {
			Offset int64 `json:"offset"`
			Size   int64 `json:"observed_size"`
		} `json:"files"`
	}
	if err := readFinalCursor(filepath.Join(a.dir, "reject-state.json"), &state); err != nil {
		return err
	}
	if state.Version != 1 || len(state.Pending) != 0 || state.PendingHash != "" || len(state.Files) != len(files) || len(files) == 0 {
		return errors.New("reject final cursor/inventory mismatch")
	}
	seen := map[string]bool{}
	for _, f := range files {
		h := sha256.Sum256([]byte(fmt.Sprintf("reject-file-v1:%d:%d", f.Device, f.Inode)))
		id := hex.EncodeToString(h[:])
		cur, exists := state.Files[id]
		if seen[id] || !exists || cur.Offset != f.Size || cur.Size != f.Size {
			return errors.New("reject final cursor/file mismatch")
		}
		seen[id] = true
	}
	return nil
}
