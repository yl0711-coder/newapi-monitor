package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const writerReleaseProofVersionV2 = 1

type writerReleaseIdentityV2 struct {
	Name       string `json:"name"`
	Device     uint64 `json:"device"`
	Inode      uint64 `json:"inode"`
	ReleasedAt int64  `json:"released_at,omitempty"`
}

type writerReleaseProofV2 struct {
	Version     int                       `json:"version"`
	GeneratedAt int64                     `json:"generated_at"`
	ContainerID string                    `json:"container_id"`
	StartedAt   string                    `json:"container_started_at"`
	Current     []writerReleaseIdentityV2 `json:"current"`
	Released    []writerReleaseIdentityV2 `json:"released"`
}

func defaultWriterReleaseProofPathV2(logPath string) string {
	return filepath.Join(filepath.Dir(logPath), ".nginx-writer-release-v2.json")
}

// loadWriterReleaseProofOwnedByV2 is called with UID 0 by the production
// applyWriterReleaseProofV2 wrapper. The explicit owner argument exists only
// so Linux tests can exercise the remaining file and payload checks on an
// unprivileged CI runner; runtime callers must use applyWriterReleaseProofV2.
func loadWriterReleaseProofOwnedByV2(path string, trustedUID uint32) (writerReleaseProofV2, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return writerReleaseProofV2{}, err
	}
	handle := os.NewFile(uintptr(fd), path)
	if handle == nil {
		_ = syscall.Close(fd)
		return writerReleaseProofV2{}, errors.New("open writer-release proof")
	}
	defer handle.Close()
	info, err := handle.Stat()
	if err != nil {
		return writerReleaseProofV2{}, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 256<<10 {
		return writerReleaseProofV2{}, errors.New("writer-release proof must be a bounded regular non-symlink file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || stat.Uid != trustedUID || info.Mode().Perm()&0o022 != 0 {
		return writerReleaseProofV2{}, errors.New("writer-release proof must be root-owned, single-linked and not group/world writable")
	}
	data, err := io.ReadAll(io.LimitReader(handle, 256<<10))
	if err != nil {
		return writerReleaseProofV2{}, err
	}
	var proof writerReleaseProofV2
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&proof); err != nil {
		return writerReleaseProofV2{}, fmt.Errorf("decode writer-release proof: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return writerReleaseProofV2{}, errors.New("writer-release proof contains trailing data")
	}
	return proof, nil
}

func validateWriterReleaseProofV2(proof writerReleaseProofV2, current sourceCandidateV2, now int64) error {
	if proof.Version != writerReleaseProofVersionV2 || proof.GeneratedAt <= 0 || proof.GeneratedAt > now+300 ||
		len(proof.ContainerID) != 64 || strings.TrimSpace(proof.StartedAt) == "" || len(proof.Current) == 0 || len(proof.Current) > 8 || len(proof.Released) > 1024 {
		return errors.New("writer-release proof envelope is invalid")
	}
	if _, err := hex.DecodeString(proof.ContainerID); err != nil {
		return errors.New("writer-release proof container identity is invalid")
	}
	started, err := time.Parse(time.RFC3339Nano, proof.StartedAt)
	if err != nil || started.Unix() <= 0 || started.Unix() > proof.GeneratedAt {
		return errors.New("writer-release proof container start time is invalid")
	}
	foundCurrent := false
	seen := make(map[string]struct{}, len(proof.Current)+len(proof.Released))
	for _, identity := range proof.Current {
		if filepath.Base(identity.Name) != identity.Name || identity.Device == 0 || identity.Inode == 0 || identity.ReleasedAt != 0 {
			return errors.New("writer-release current identity is invalid")
		}
		key := fmt.Sprintf("%d:%d", identity.Device, identity.Inode)
		if _, exists := seen[key]; exists {
			return errors.New("writer-release proof repeats a physical identity")
		}
		seen[key] = struct{}{}
		if identity.Name == current.Base && identity.Device == current.Device && identity.Inode == current.Inode {
			foundCurrent = true
		}
	}
	for _, identity := range proof.Released {
		if filepath.Base(identity.Name) != identity.Name || identity.Device == 0 || identity.Inode == 0 || identity.ReleasedAt <= 0 || identity.ReleasedAt > proof.GeneratedAt {
			return errors.New("writer-release historical identity is invalid")
		}
		key := fmt.Sprintf("%d:%d", identity.Device, identity.Inode)
		if _, exists := seen[key]; exists {
			return errors.New("writer-release proof repeats a physical identity")
		}
		seen[key] = struct{}{}
	}
	if !foundCurrent {
		return errors.New("writer-release proof does not bind the current log inode")
	}
	return nil
}

func applyWriterReleaseProofV2(state sourceCursorV2, logPath, proofPath string, now int64) (sourceCursorV2, bool, error) {
	return applyWriterReleaseProofOwnedByV2(state, logPath, proofPath, now, 0)
}

// applyWriterReleaseProofOwnedByV2 is the test seam paired with
// loadWriterReleaseProofOwnedByV2.  The production wrapper above always pins
// trustedUID to root.
func applyWriterReleaseProofOwnedByV2(state sourceCursorV2, logPath, proofPath string, now int64, trustedUID uint32) (sourceCursorV2, bool, error) {
	proof, err := loadWriterReleaseProofOwnedByV2(proofPath, trustedUID)
	if errors.Is(err, os.ErrNotExist) {
		return state, false, nil
	}
	if err != nil {
		return state, false, err
	}
	candidates, err := listSourceCandidatesV2(logPath)
	if err != nil {
		return state, false, err
	}
	var current sourceCandidateV2
	for _, candidate := range candidates {
		if candidate.Current {
			current = candidate
			break
		}
	}
	if err := validateWriterReleaseProofV2(proof, current, now); err != nil {
		return state, false, err
	}
	return applyWriterReleaseProofValueV2(state, proof)
}

func applyWriterReleaseProofValueV2(state sourceCursorV2, proof writerReleaseProofV2) (sourceCursorV2, bool, error) {
	if err := validateSourceCursorV2(state); err != nil {
		return state, false, err
	}
	released := make(map[string]writerReleaseIdentityV2, len(proof.Released))
	for _, identity := range proof.Released {
		released[fmt.Sprintf("%d:%d", identity.Device, identity.Inode)] = identity
	}
	next := state
	next.Files = append([]fileWatermark(nil), state.Files...)
	changed := false
	for _, file := range append([]fileWatermark(nil), next.Files...) {
		if file.State != "missing" || file.Current || file.AckedOffset != file.LastObservedSize {
			continue
		}
		identity, ok := released[fmt.Sprintf("%d:%d", file.Device, file.Inode)]
		if !ok || identity.ReleasedAt < file.FirstSeenAt || identity.ReleasedAt < file.LastSeenAt || identity.ReleasedAt < file.LastGrowthAt {
			continue
		}
		var confirmErr error
		next, confirmErr = confirmSourceWriterReleasedV2(next, file.FileID)
		if confirmErr != nil {
			return state, false, confirmErr
		}
		changed = true
	}
	return next, changed, nil
}
