package main

// tracker_v2.go implements the durable, multi-file source state machine used
// only by explicitly selected v2 lanes. The default production loop remains
// on v1; a lane cannot cut over before the capability handshake, manifest CAS
// and acknowledged v1 boundary all succeed.

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	sourceCursorVersionV2 = 2
	maxTrackedSourceFiles = 256
	maxSourceRangeBytesV2 = int64(128 << 20)
)

var (
	errSourceCursorLoss     = errors.New("source continuity cannot be proven")
	errSourceCursorTruncate = errors.New("tracked source file was truncated")
	errSourceSnapshotChange = errors.New("source files changed while taking a snapshot")
)

type sourceCursorV2 struct {
	Version        int    `json:"version"`
	SourceEpoch    string `json:"source_epoch"`
	NextGeneration uint64 `json:"next_generation"`
	// ManifestRegistered is durable proof that Monitor atomically registered
	// every initial source watermark. No byte may be scheduled before it is
	// true; this keeps a crash between v1 migration and cutover fail-closed.
	ManifestRegistered bool              `json:"manifest_registered"`
	ManifestSHA256     string            `json:"manifest_sha256,omitempty"`
	ManifestRevision   uint64            `json:"manifest_revision,omitempty"`
	LegacyCursorDevice uint64            `json:"legacy_cursor_device,omitempty"`
	LegacyCursorInode  uint64            `json:"legacy_cursor_inode,omitempty"`
	LegacyCursorOffset int64             `json:"legacy_cursor_offset,omitempty"`
	LegacyAckedBatchID string            `json:"legacy_acked_batch_id,omitempty"`
	RoundRobin         int               `json:"round_robin"`
	Files              []fileWatermark   `json:"files"`
	Telemetry          sourceTelemetryV2 `json:"telemetry"`
}

type sourceTelemetryV2 struct {
	Discontinuities              int64 `json:"discontinuities,omitempty"`
	LastDiscontinuityAt          int64 `json:"last_discontinuity_at,omitempty"`
	DiscardedLines               int64 `json:"discarded_lines,omitempty"`
	LastDiscardedAt              int64 `json:"last_discarded_at,omitempty"`
	LastLogSchema                int   `json:"last_log_schema,omitempty"`
	EvidenceEligible             int64 `json:"evidence_eligible,omitempty"`
	EvidenceParseRejected        int64 `json:"evidence_parse_rejected,omitempty"`
	LastEvidenceParseRejectedAt  int64 `json:"last_evidence_parse_rejected_at,omitempty"`
	EvidencePersistFailures      int64 `json:"evidence_persist_failures,omitempty"`
	EvidenceDroppedEvents        int64 `json:"evidence_dropped_events,omitempty"`
	LastEvidencePersistFailureAt int64 `json:"last_evidence_persist_failure_at,omitempty"`
	LostFiles                    int64 `json:"lost_files,omitempty"`
	LastLossAt                   int64 `json:"last_loss_at,omitempty"`
	LateGrowths                  int64 `json:"late_growths,omitempty"`
	LastLateGrowthAt             int64 `json:"last_late_growth_at,omitempty"`
}

type fileWatermark struct {
	FileID            string `json:"file_id"`
	Generation        uint64 `json:"generation"`
	Device            uint64 `json:"device"`
	Inode             uint64 `json:"inode"`
	AckedOffset       int64  `json:"acked_offset"`
	CutoverBaseOffset int64  `json:"cutover_base_offset,omitempty"`
	FirstSeenAt       int64  `json:"first_seen_at"`
	LastSeenAt        int64  `json:"last_seen_at"`
	LastGrowthAt      int64  `json:"last_growth_at"`
	LastObservedSize  int64  `json:"last_observed_size"`
	AnchorStart       int64  `json:"anchor_start,omitempty"`
	AnchorSHA256      string `json:"anchor_sha256,omitempty"`
	MissingSince      int64  `json:"missing_since,omitempty"`
	MissingScans      int64  `json:"missing_scans,omitempty"`
	PathBase          string `json:"path_base"`
	State             string `json:"state"` // active/quiescent/missing/lost/retired
	Current           bool   `json:"current"`
	Registered        bool   `json:"registered"`
	RecoveryPending   bool   `json:"recovery_pending,omitempty"`
}

type sourceCandidateV2 struct {
	Path    string
	Base    string
	Device  uint64
	Inode   uint64
	Size    int64
	ModTime time.Time
	Current bool
}

type sourceManifestFileV2 struct {
	FileID     string `json:"file_id"`
	Generation uint64 `json:"generation"`
	Device     uint64 `json:"device"`
	Inode      uint64 `json:"inode"`
	BaseOffset int64  `json:"base_offset"`
	Current    bool   `json:"current"`
}

type sourceManifestV2 struct {
	Protocol           int                    `json:"protocol"`
	Kind               string                 `json:"kind"`
	SourceEpoch        string                 `json:"source_epoch"`
	CutoverFromV1      bool                   `json:"cutover_from_v1"`
	LegacyCursorInode  uint64                 `json:"legacy_cursor_inode,omitempty"`
	LegacyCursorDevice uint64                 `json:"legacy_cursor_device,omitempty"`
	LegacyCursorOffset int64                  `json:"legacy_cursor_offset,omitempty"`
	LegacyAckedBatchID string                 `json:"legacy_acked_batch_id,omitempty"`
	Files              []sourceManifestFileV2 `json:"files"`
}

type sourceFileRegistrationV2 struct {
	Protocol             int                    `json:"protocol"`
	Kind                 string                 `json:"kind"`
	SourceEpoch          string                 `json:"source_epoch"`
	ExpectedRevision     uint64                 `json:"expected_revision"`
	PreviousManifestHash string                 `json:"previous_manifest_hash"`
	Files                []sourceManifestFileV2 `json:"files"`
}

type sourceAckProofV2 struct {
	StartOffset   int64  `json:"start_offset"`
	EndOffset     int64  `json:"end_offset"`
	ContentSHA256 string `json:"content_sha256"`
}

type sourceAckRecoveryV2 struct {
	SourceEpoch string             `json:"source_epoch"`
	FileID      string             `json:"file_id"`
	NextOffset  int64              `json:"next_offset"`
	Proofs      []sourceAckProofV2 `json:"proofs"`
}

func randomSourceEpoch() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate source epoch: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

func formatGeneration(value uint64) string {
	const digits = "0123456789abcdef"
	buf := make([]byte, 16)
	for i := len(buf) - 1; i >= 0; i-- {
		buf[i] = digits[value&15]
		value >>= 4
	}
	return string(buf)
}

func sourceDevice(info os.FileInfo) uint64 {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint64(stat.Dev)
	}
	return 0
}

func listSourceCandidatesV2(logPath string) ([]sourceCandidateV2, error) {
	return listSourceCandidatesWithHookV2(logPath, nil)
}

func listSourceCandidatesWithHookV2(logPath string, beforeRevalidate func()) ([]sourceCandidateV2, error) {
	currentInfo, err := os.Lstat(logPath)
	if err != nil {
		return nil, err
	}
	if currentInfo.Mode()&os.ModeSymlink != 0 || !currentInfo.Mode().IsRegular() {
		return nil, errors.New("current source must be a regular non-symlink file")
	}
	dir, base := filepath.Dir(logPath), filepath.Base(logPath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	physical := map[string]string{}
	makeCandidate := func(path string, info os.FileInfo, current bool) (sourceCandidateV2, error) {
		device, inode := sourceDevice(info), fileInode(info)
		if device == 0 || inode == 0 || info.Size() < 0 {
			return sourceCandidateV2{}, errors.New("source candidate has no stable filesystem identity")
		}
		key := fmt.Sprintf("%d:%d", device, inode)
		if previous := physical[key]; previous != "" {
			return sourceCandidateV2{}, fmt.Errorf("duplicate physical source identity at %s and %s", previous, path)
		}
		physical[key] = path
		return sourceCandidateV2{Path: path, Base: filepath.Base(path), Device: device, Inode: inode, Size: info.Size(), ModTime: info.ModTime(), Current: current}, nil
	}
	rotated := make([]sourceCandidateV2, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if name == base || !strings.HasPrefix(name, base) || compressedLogName(name) || entry.Type()&os.ModeSymlink != 0 {
			continue
		}
		path := filepath.Join(dir, name)
		info, err := os.Lstat(path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			continue
		}
		candidate, err := makeCandidate(path, info, false)
		if err != nil {
			return nil, err
		}
		rotated = append(rotated, candidate)
		if len(rotated) >= maxLogCandidates {
			return nil, errors.New("too many rotated nginx source candidates")
		}
	}
	sort.Slice(rotated, func(i, j int) bool {
		if !rotated[i].ModTime.Equal(rotated[j].ModTime) {
			return rotated[i].ModTime.Before(rotated[j].ModTime)
		}
		return rotated[i].Path < rotated[j].Path
	})
	current, err := makeCandidate(logPath, currentInfo, true)
	if err != nil {
		return nil, err
	}
	rotated = append(rotated, current)
	candidates := rotated
	// Rotation can happen between the first current Lstat and directory scan.
	// Revalidate the whole identity set and ask the caller to retry instead of
	// returning a mixed path/inode snapshot.
	if beforeRevalidate != nil {
		beforeRevalidate()
	}
	for _, candidate := range candidates {
		info, err := os.Lstat(candidate.Path)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || sourceDevice(info) != candidate.Device || fileInode(info) != candidate.Inode {
			return nil, fmt.Errorf("%w: %s", errSourceSnapshotChange, candidate.Path)
		}
	}
	return candidates, nil
}

// rebindPendingSourcePathsV2 repairs path renames after an ACK was accepted by
// Monitor but before the collector could persist it. It deliberately changes
// only PathBase: the immutable manifest/registration identity and Current bit
// must remain exactly as they were in the already accepted network request.
// Newly discovered inodes are ignored until that pending request is durable.
func rebindPendingSourcePathsV2(state sourceCursorV2, candidates []sourceCandidateV2) (sourceCursorV2, error) {
	if err := validateSourceCursorV2(state); err != nil {
		return state, err
	}
	paths := make(map[string]string, len(candidates))
	for _, candidate := range candidates {
		key := fmt.Sprintf("%d:%d", candidate.Device, candidate.Inode)
		if candidate.Device == 0 || candidate.Inode == 0 || filepath.Base(candidate.Base) != candidate.Base || paths[key] != "" {
			return state, errors.New("invalid source candidates while rebinding pending request")
		}
		paths[key] = candidate.Base
	}
	next := state
	next.Files = append([]fileWatermark(nil), state.Files...)
	for i := range next.Files {
		file := &next.Files[i]
		path := paths[fmt.Sprintf("%d:%d", file.Device, file.Inode)]
		if path == "" {
			if !file.Registered {
				return state, fmt.Errorf("%w: pending source file %s disappeared", errSourceCursorLoss, file.FileID)
			}
			continue
		}
		file.PathBase = path
	}
	if err := validateSourceCursorV2(next); err != nil {
		return state, err
	}
	return next, nil
}

func telemetryFromV1(value cursor) sourceTelemetryV2 {
	return sourceTelemetryV2{
		Discontinuities: value.Discontinuities, LastDiscontinuityAt: value.LastDiscontinuityAt,
		DiscardedLines: value.DiscardedLines, LastDiscardedAt: value.LastDiscardedAt,
		LastLogSchema: value.LastLogSchema, EvidenceEligible: value.EvidenceEligible,
		EvidenceParseRejected: value.EvidenceParseRejected, LastEvidenceParseRejectedAt: value.LastEvidenceParseRejectedAt,
		EvidencePersistFailures: value.EvidencePersistFailures, EvidenceDroppedEvents: value.EvidenceDroppedEvents,
		LastEvidencePersistFailureAt: value.LastEvidencePersistFailureAt,
	}
}

func migrateSourceCursorV1(logPath string, legacy cursor, now int64) (sourceCursorV2, error) {
	if legacy.Inode != 0 && (legacy.Device == 0 || legacy.LastAckedBatchID == "" || legacy.LastAckedDevice != legacy.Device || legacy.LastAckedInode != legacy.Inode || legacy.LastAckedOffset != legacy.Offset) {
		return sourceCursorV2{}, fmt.Errorf("%w: v1 cursor is not bound to an acknowledged server source boundary", errSourceCursorLoss)
	}
	candidates, err := listSourceCandidatesV2(logPath)
	if err != nil {
		return sourceCursorV2{}, err
	}
	epoch, err := randomSourceEpoch()
	if err != nil {
		return sourceCursorV2{}, err
	}
	state := sourceCursorV2{Version: sourceCursorVersionV2, SourceEpoch: epoch, NextGeneration: 1, LegacyCursorDevice: legacy.Device, LegacyCursorInode: legacy.Inode, LegacyCursorOffset: legacy.Offset, LegacyAckedBatchID: legacy.LastAckedBatchID, Telemetry: telemetryFromV1(legacy)}
	start := len(candidates) - 1
	if legacy.Inode == 0 && len(candidates) > 1 {
		return sourceCursorV2{}, fmt.Errorf("%w: cursor is absent while rotated candidates exist", errSourceCursorLoss)
	}
	if legacy.Inode != 0 {
		start = -1
		for i, candidate := range candidates {
			if candidate.Device == legacy.Device && candidate.Inode == legacy.Inode {
				start = i
				break
			}
		}
		if start < 0 {
			return sourceCursorV2{}, fmt.Errorf("%w: v1 inode %d is no longer present", errSourceCursorLoss, legacy.Inode)
		}
		if candidates[start].Size < legacy.Offset {
			return sourceCursorV2{}, fmt.Errorf("%w: v1 offset %d exceeds size %d", errSourceCursorTruncate, legacy.Offset, candidates[start].Size)
		}
		// v1 records only one inode. Any predecessor means its read status and
		// ordering cannot be proven (especially when v1 already switched to an
		// empty current while .1 still grows). Never use mtime to guess.
		if start > 0 {
			return sourceCursorV2{}, fmt.Errorf("%w: %d predecessor candidate(s) are not represented by the v1 cursor", errSourceCursorLoss, start)
		}
		// If v1 points at a rotated file, only the unambiguous pair
		// [that file,current] is safe. Multiple successors have no authoritative
		// generation order in v1.
		if start < len(candidates)-1 && len(candidates) != 2 {
			return sourceCursorV2{}, fmt.Errorf("%w: rotated v1 cursor has ambiguous successors", errSourceCursorLoss)
		}
	}
	for i := start; i < len(candidates); i++ {
		candidate := candidates[i]
		base := int64(0)
		if i == start && legacy.Inode != 0 {
			base = legacy.Offset
		}
		watermark := state.newWatermark(candidate, base, now)
		if base > 0 {
			watermark.AnchorStart = max(int64(0), base-4096)
			watermark.AnchorSHA256, err = hashSourceRangeV2(candidate.Path, watermark.AnchorStart, base)
			if err != nil {
				return sourceCursorV2{}, fmt.Errorf("build v1 cutover anchor: %w", err)
			}
		}
		state.Files = append(state.Files, watermark)
	}
	if err := validateSourceCursorV2(state); err != nil {
		return sourceCursorV2{}, err
	}
	return state, nil
}

func (s *sourceCursorV2) newWatermark(candidate sourceCandidateV2, base, now int64) fileWatermark {
	generation := s.NextGeneration
	s.NextGeneration++
	state := "quiescent"
	if candidate.Size > base {
		state = "active"
	}
	return fileWatermark{
		FileID: fmt.Sprintf("%s-%016x", s.SourceEpoch, generation), Generation: generation,
		Device: candidate.Device, Inode: candidate.Inode, AckedOffset: base, CutoverBaseOffset: base,
		FirstSeenAt: now, LastSeenAt: now, LastGrowthAt: now, LastObservedSize: candidate.Size,
		PathBase: candidate.Base, State: state, Current: candidate.Current,
	}
}

func validateSourceCursorV2(state sourceCursorV2) error {
	if state.Version != sourceCursorVersionV2 || len(state.SourceEpoch) != 32 || state.NextGeneration == 0 || state.RoundRobin < 0 || state.RoundRobin > 4 || len(state.Files) == 0 || len(state.Files) > maxTrackedSourceFiles {
		return errors.New("v2 source cursor envelope is invalid")
	}
	if _, err := hex.DecodeString(state.SourceEpoch); err != nil {
		return errors.New("v2 source epoch is invalid")
	}
	if state.LegacyCursorOffset < 0 || state.ManifestRegistered != (state.ManifestSHA256 != "") || state.ManifestRegistered != (state.ManifestRevision > 0) ||
		(state.LegacyCursorInode == 0) != (state.LegacyAckedBatchID == "") || (state.LegacyCursorInode == 0) != (state.LegacyCursorDevice == 0) || state.LegacyCursorInode == 0 && state.LegacyCursorOffset != 0 {
		return errors.New("v2 source manifest state is invalid")
	}
	if state.ManifestSHA256 != "" {
		if len(state.ManifestSHA256) != 64 {
			return errors.New("v2 source manifest hash is invalid")
		}
		if _, err := hex.DecodeString(state.ManifestSHA256); err != nil {
			return errors.New("v2 source manifest hash is invalid")
		}
	}
	seenIDs, seenGeneration, seenPhysical := map[string]bool{}, map[uint64]bool{}, map[string]bool{}
	current := 0
	var maxGeneration uint64
	for _, file := range state.Files {
		physical := fmt.Sprintf("%d:%d", file.Device, file.Inode)
		if file.FileID != state.SourceEpoch+"-"+formatGeneration(file.Generation) || file.Generation == 0 || seenIDs[file.FileID] || seenGeneration[file.Generation] || seenPhysical[physical] ||
			file.Device == 0 || file.Inode == 0 || file.AckedOffset < 0 || file.CutoverBaseOffset < 0 || file.CutoverBaseOffset > file.AckedOffset ||
			file.FirstSeenAt <= 0 || file.LastSeenAt < file.FirstSeenAt || file.LastGrowthAt < file.FirstSeenAt || file.LastObservedSize < 0 || file.AckedOffset > file.LastObservedSize ||
			file.AnchorStart < 0 || file.AnchorStart > file.AckedOffset || file.MissingSince < 0 || file.MissingScans < 0 ||
			filepath.Base(file.PathBase) != file.PathBase {
			return errors.New("v2 source cursor file watermark is invalid")
		}
		switch file.State {
		case "active":
			if file.LastObservedSize <= file.AckedOffset || file.MissingSince != 0 || file.MissingScans != 0 {
				return errors.New("active v2 file has impossible offsets")
			}
		case "quiescent":
			if file.LastObservedSize != file.AckedOffset || file.MissingSince != 0 || file.MissingScans != 0 {
				return errors.New("quiescent v2 file has unread bytes")
			}
		case "missing":
			if file.LastObservedSize != file.AckedOffset || file.MissingSince == 0 || file.MissingScans == 0 || file.Current {
				return errors.New("missing v2 file is not safely drained")
			}
		case "lost":
			if file.Current {
				return errors.New("lost v2 file cannot be current")
			}
		case "retired":
			if file.LastObservedSize != file.AckedOffset || file.Current {
				return errors.New("retired v2 file is not safely drained")
			}
		default:
			return errors.New("v2 source cursor file state is invalid")
		}
		if file.Current {
			current++
		}
		if !state.ManifestRegistered && file.Registered {
			return errors.New("source file is registered before its manifest")
		}
		if file.AckedOffset > 0 && file.AnchorSHA256 == "" {
			return errors.New("acknowledged v2 source has no content anchor")
		}
		if file.AnchorSHA256 != "" {
			if len(file.AnchorSHA256) != 64 {
				return errors.New("v2 source anchor is invalid")
			}
			if _, err := hex.DecodeString(file.AnchorSHA256); err != nil {
				return errors.New("v2 source anchor is invalid")
			}
		} else if file.AnchorStart != 0 {
			return errors.New("v2 source anchor start has no hash")
		}
		if file.Generation > maxGeneration {
			maxGeneration = file.Generation
		}
		seenIDs[file.FileID], seenGeneration[file.Generation], seenPhysical[physical] = true, true, true
	}
	if current != 1 || state.NextGeneration <= maxGeneration || state.Telemetry.LostFiles < 0 || state.Telemetry.LastLossAt < 0 || (state.Telemetry.LostFiles == 0) != (state.Telemetry.LastLossAt == 0) ||
		state.Telemetry.LateGrowths < 0 || state.Telemetry.LastLateGrowthAt < 0 || (state.Telemetry.LateGrowths == 0) != (state.Telemetry.LastLateGrowthAt == 0) {
		return errors.New("v2 source cursor state is inconsistent")
	}
	return nil
}

func reconcileSourceCursorV2(state sourceCursorV2, candidates []sourceCandidateV2, now int64) (sourceCursorV2, error) {
	if err := validateSourceCursorV2(state); err != nil {
		return state, err
	}
	for _, file := range state.Files {
		if file.State == "lost" {
			return state, fmt.Errorf("%w: lane is blocked by lost file %s", errSourceCursorLoss, file.FileID)
		}
		if now < file.FirstSeenAt || now < file.LastSeenAt || now < file.LastGrowthAt {
			return state, errors.New("clock moved backwards; source state was not changed")
		}
	}
	if len(candidates) == 0 {
		return state, errors.New("no source candidates")
	}
	physical := map[string]bool{}
	for _, candidate := range candidates {
		key := fmt.Sprintf("%d:%d", candidate.Device, candidate.Inode)
		if candidate.Device == 0 || candidate.Inode == 0 || candidate.Size < 0 || physical[key] {
			return state, errors.New("duplicate or invalid physical source candidate")
		}
		physical[key] = true
	}
	// Slices share backing arrays under ordinary value copies. Reconcile must be
	// transactional from its caller's perspective, so mutate a deep copy only.
	state.Files = append([]fileWatermark(nil), state.Files...)
	seen := make([]bool, len(state.Files))
	for i := range state.Files {
		state.Files[i].Current = false
	}
	for _, candidate := range candidates {
		match := -1
		for i := range state.Files {
			file := state.Files[i]
			if !seen[i] && file.Device == candidate.Device && file.Inode == candidate.Inode && file.State != "retired" && file.State != "lost" {
				match = i
				break
			}
		}
		if match < 0 {
			if len(state.Files) >= maxTrackedSourceFiles {
				return state, errors.New("tracked source file limit reached; explicit recovery required")
			}
			state.Files = append(state.Files, state.newWatermark(candidate, 0, now))
			seen = append(seen, true)
			continue
		}
		file := &state.Files[match]
		seen[match], file.Current, file.LastSeenAt, file.PathBase = true, candidate.Current, now, candidate.Base
		if err := verifySourceAnchorV2(candidate, *file); err != nil {
			file.State = "lost"
			state.recordLoss(now)
			return state, err
		}
		if candidate.Size < file.AckedOffset || candidate.Size < file.LastObservedSize {
			file.State = "lost"
			state.recordLoss(now)
			return state, fmt.Errorf("%w: %s size=%d acked=%d observed=%d", errSourceCursorTruncate, file.FileID, candidate.Size, file.AckedOffset, file.LastObservedSize)
		}
		if candidate.Size > file.LastObservedSize {
			if file.State == "quiescent" && !file.Current {
				if state.Telemetry.LateGrowths < math.MaxInt64 {
					state.Telemetry.LateGrowths++
				}
				state.Telemetry.LastLateGrowthAt = now
			}
			file.LastGrowthAt = now
		}
		file.LastObservedSize = candidate.Size
		file.MissingSince, file.MissingScans = 0, 0
		if candidate.Size > file.AckedOffset {
			file.State = "active"
		} else {
			file.State = "quiescent"
		}
	}
	for i := range state.Files {
		if seen[i] || state.Files[i].State == "retired" || state.Files[i].State == "lost" {
			continue
		}
		file := &state.Files[i]
		if file.AckedOffset < file.LastObservedSize {
			file.State = "lost"
			state.recordLoss(now)
			return state, fmt.Errorf("%w: unread file %s disappeared at %d/%d", errSourceCursorLoss, file.FileID, file.AckedOffset, file.LastObservedSize)
		}
		file.State = "missing"
		if file.MissingSince == 0 {
			file.MissingSince = now
		}
		if file.MissingScans < math.MaxInt64 {
			file.MissingScans++
		}
	}
	if err := validateSourceCursorV2(state); err != nil {
		return state, err
	}
	return state, nil
}

func verifySourceAnchorV2(candidate sourceCandidateV2, file fileWatermark) error {
	if file.AnchorSHA256 == "" {
		return nil
	}
	if candidate.Size < file.AckedOffset {
		return fmt.Errorf("%w: anchored file %s is smaller than its acknowledgement", errSourceCursorTruncate, file.FileID)
	}
	f, err := os.Open(candidate.Path)
	if err != nil {
		return fmt.Errorf("verify source anchor: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || sourceDevice(info) != candidate.Device || fileInode(info) != candidate.Inode {
		return fmt.Errorf("%w: source identity changed while verifying anchor", errSourceCursorLoss)
	}
	if _, err := f.Seek(file.AnchorStart, io.SeekStart); err != nil {
		return err
	}
	hasher := sha256.New()
	if _, err := io.CopyN(hasher, f, file.AckedOffset-file.AnchorStart); err != nil {
		return fmt.Errorf("%w: cannot read source anchor: %w", errSourceCursorLoss, err)
	}
	if hex.EncodeToString(hasher.Sum(nil)) != file.AnchorSHA256 {
		return fmt.Errorf("%w: source content anchor changed for %s", errSourceCursorLoss, file.FileID)
	}
	return nil
}

func hashSourceRangeV2(path string, start, end int64) (string, error) {
	if start < 0 || end < start || end-start > 4096 {
		return "", errors.New("source anchor range is invalid")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return "", err
	}
	hasher := sha256.New()
	if _, err := io.CopyN(hasher, f, end-start); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func hashSourceBatchRangeV2(path string, start, end int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	return hashPinnedSourceRangeV2(f, start, end, maxSourceRangeBytesV2)
}

func hashPinnedSourceRangeV2(file *os.File, start, end, limit int64) (string, error) {
	if file == nil || start < 0 || end <= start || end-start > limit {
		return "", errors.New("source batch proof range is invalid")
	}
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return "", err
	}
	hasher := sha256.New()
	if _, err := io.CopyN(hasher, file, end-start); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func openPinnedSourceV2(logPath string, file fileWatermark) (*os.File, error) {
	path := filepath.Join(filepath.Dir(logPath), file.PathBase)
	handle, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	info, err := handle.Stat()
	if err != nil || !info.Mode().IsRegular() || sourceDevice(info) != file.Device || fileInode(info) != file.Inode {
		_ = handle.Close()
		return nil, fmt.Errorf("%w: pinned source identity changed", errSourceSnapshotChange)
	}
	return handle, nil
}

func (s *sourceCursorV2) recordLoss(now int64) {
	if s.Telemetry.LostFiles < math.MaxInt64 {
		s.Telemetry.LostFiles++
	}
	s.Telemetry.LastLossAt = now
}

func selectReadableWatermarkV2(state *sourceCursorV2) (int, bool) {
	if !state.ManifestRegistered {
		return -1, false
	}
	for _, file := range state.Files {
		if file.State == "lost" || file.State == "missing" {
			return -1, false
		}
	}
	var current, oldest = -1, -1
	for i := range state.Files {
		file := state.Files[i]
		if file.State != "active" || file.LastObservedSize <= file.AckedOffset || !file.Registered {
			continue
		}
		if file.Current {
			current = i
		} else if oldest < 0 || file.Generation < state.Files[oldest].Generation {
			oldest = i
		}
	}
	if current < 0 {
		return oldest, oldest >= 0
	}
	if oldest < 0 {
		return current, true
	}
	// Keep the live file near real time without starving rotated backlog.
	choice := current
	if state.RoundRobin%5 == 4 {
		choice = oldest
	}
	state.RoundRobin = (state.RoundRobin + 1) % 5
	return choice, true
}

func acknowledgeSourceRangeV2(state *sourceCursorV2, fileID string, start, end, anchorStart int64, anchorSHA256 string, now int64) error {
	if err := validateSourceCursorV2(*state); err != nil {
		return err
	}
	if end <= start {
		return errors.New("source acknowledgement range must consume bytes")
	}
	if anchorStart < start || anchorStart > end || end-anchorStart > 4096 || len(anchorSHA256) != 64 {
		return errors.New("source acknowledgement content hash is invalid")
	}
	if _, err := hex.DecodeString(anchorSHA256); err != nil {
		return errors.New("source acknowledgement content hash is invalid")
	}
	for _, file := range state.Files {
		if file.State == "lost" || file.State == "missing" {
			return fmt.Errorf("%w: lane is blocked", errSourceCursorLoss)
		}
	}
	if !state.ManifestRegistered {
		return errors.New("source manifest is not registered")
	}
	next := *state
	next.Files = append([]fileWatermark(nil), state.Files...)
	for i := range next.Files {
		file := &next.Files[i]
		if file.FileID != fileID {
			continue
		}
		if !file.Registered {
			return errors.New("source acknowledgement references an unregistered file")
		}
		if start != file.AckedOffset || end > file.LastObservedSize {
			return fmt.Errorf("source acknowledgement is not contiguous: have=%d range=%d:%d size=%d", file.AckedOffset, start, end, file.LastObservedSize)
		}
		if now < file.LastSeenAt {
			return errors.New("clock moved backwards; source acknowledgement was not changed")
		}
		file.AckedOffset, file.LastSeenAt = end, now
		file.AnchorStart, file.AnchorSHA256 = anchorStart, anchorSHA256
		if end == file.LastObservedSize {
			file.State = "quiescent"
		}
		if err := validateSourceCursorV2(next); err != nil {
			return err
		}
		*state = next
		return nil
	}
	return errors.New("source acknowledgement references unknown file")
}

// setSourceRecoveryPendingV2 is persisted before any data POST. It is the
// crash-safe hint that tells a restarted collector exactly which file needs a
// server proof lookup; healthy historical files require no network polling.
func setSourceRecoveryPendingV2(state *sourceCursorV2, fileID string, pending bool) error {
	if err := validateSourceCursorV2(*state); err != nil {
		return err
	}
	next := *state
	next.Files = append([]fileWatermark(nil), state.Files...)
	for i := range next.Files {
		if next.Files[i].FileID != fileID {
			continue
		}
		if !next.Files[i].Registered || next.Files[i].State == "retired" || next.Files[i].State == "lost" {
			return errors.New("source recovery marker references unavailable file")
		}
		next.Files[i].RecoveryPending = pending
		if err := validateSourceCursorV2(next); err != nil {
			return err
		}
		*state = next
		return nil
	}
	return errors.New("source recovery marker references unknown file")
}

// recoverSourceAcknowledgementsV2 repairs a lost HTTP acknowledgement. Every
// accepted server range is re-hashed from the pinned local source before the
// durable cursor may advance. Batch-size changes therefore cannot cause a
// duplicate or a permanent overlap loop.
func recoverSourceAcknowledgementsV2(state sourceCursorV2, logPath string, recovery sourceAckRecoveryV2, now int64) (sourceCursorV2, error) {
	if err := validateSourceCursorV2(state); err != nil {
		return state, err
	}
	if recovery.SourceEpoch != state.SourceEpoch || recovery.FileID == "" || recovery.NextOffset < 0 || len(recovery.Proofs) > 4096 {
		return state, errors.New("source acknowledgement recovery envelope is invalid")
	}
	next := state
	next.Files = append([]fileWatermark(nil), state.Files...)
	index := -1
	for i := range next.Files {
		if next.Files[i].FileID == recovery.FileID {
			index = i
			break
		}
	}
	if index < 0 {
		return state, errors.New("source acknowledgement recovery references unknown file")
	}
	file := next.Files[index]
	if recovery.NextOffset < file.AckedOffset || recovery.NextOffset > file.LastObservedSize || now < file.LastSeenAt {
		return state, errors.New("source acknowledgement recovery watermark is invalid")
	}
	handle, err := openPinnedSourceV2(logPath, file)
	if err != nil {
		return state, err
	}
	defer handle.Close()
	offset := file.AckedOffset
	for _, proof := range recovery.Proofs {
		if proof.StartOffset != offset || proof.EndOffset <= proof.StartOffset || proof.EndOffset > recovery.NextOffset || !nginxHashPatternV2(proof.ContentSHA256) {
			return state, errors.New("source acknowledgement recovery proof chain is invalid")
		}
		actual, err := hashPinnedSourceRangeV2(handle, proof.StartOffset, proof.EndOffset, maxSourceRangeBytesV2)
		if err != nil || actual != proof.ContentSHA256 {
			return state, fmt.Errorf("%w: accepted source proof does not match local bytes", errSourceCursorLoss)
		}
		anchorStart := max(proof.StartOffset, proof.EndOffset-4096)
		anchor, err := hashPinnedSourceRangeV2(handle, anchorStart, proof.EndOffset, 4096)
		if err != nil {
			return state, err
		}
		if err := acknowledgeSourceRangeV2(&next, file.FileID, proof.StartOffset, proof.EndOffset, anchorStart, anchor, now); err != nil {
			return state, err
		}
		offset = proof.EndOffset
	}
	if offset != recovery.NextOffset {
		return state, errors.New("source acknowledgement recovery proof chain is incomplete")
	}
	return next, nil
}

func nginxHashPatternV2(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func buildSourceManifestV2(state sourceCursorV2, kind string) (sourceManifestV2, string, error) {
	if err := validateSourceCursorV2(state); err != nil {
		return sourceManifestV2{}, "", err
	}
	if state.ManifestRegistered || kind != "access" && kind != "error" {
		return sourceManifestV2{}, "", errors.New("source manifest cannot be built")
	}
	manifest := sourceManifestV2{
		Protocol: sourceCursorVersionV2, Kind: kind, SourceEpoch: state.SourceEpoch,
		CutoverFromV1: state.LegacyCursorInode != 0, LegacyCursorDevice: state.LegacyCursorDevice, LegacyCursorInode: state.LegacyCursorInode,
		LegacyCursorOffset: state.LegacyCursorOffset, LegacyAckedBatchID: state.LegacyAckedBatchID,
		Files: make([]sourceManifestFileV2, 0, len(state.Files)),
	}
	for _, file := range state.Files {
		if file.State == "lost" || file.State == "missing" || file.State == "retired" || file.Registered {
			return sourceManifestV2{}, "", errors.New("source manifest contains an unavailable initial file")
		}
		manifest.Files = append(manifest.Files, sourceManifestFileV2{
			FileID: file.FileID, Generation: file.Generation, Device: file.Device, Inode: file.Inode,
			BaseOffset: file.CutoverBaseOffset, Current: file.Current,
		})
	}
	sort.Slice(manifest.Files, func(i, j int) bool { return manifest.Files[i].Generation < manifest.Files[j].Generation })
	data, err := json.Marshal(manifest)
	if err != nil {
		return sourceManifestV2{}, "", err
	}
	sum := sha256.Sum256(data)
	return manifest, hex.EncodeToString(sum[:]), nil
}

func markSourceManifestRegisteredV2(state sourceCursorV2, kind, serverHash string) (sourceCursorV2, error) {
	if state.ManifestRegistered || len(serverHash) != 64 {
		return state, errors.New("source manifest acknowledgement is invalid")
	}
	if _, err := hex.DecodeString(serverHash); err != nil {
		return state, errors.New("source manifest acknowledgement is invalid")
	}
	_, expectedHash, err := buildSourceManifestV2(state, kind)
	if err != nil || expectedHash != serverHash {
		return state, errors.New("source manifest acknowledgement hash does not match local state")
	}
	next := state
	next.Files = append([]fileWatermark(nil), state.Files...)
	next.ManifestRegistered, next.ManifestSHA256, next.ManifestRevision = true, serverHash, 1
	for i := range next.Files {
		next.Files[i].Registered = true
	}
	if err := validateSourceCursorV2(next); err != nil {
		return state, err
	}
	return next, nil
}

func buildNextSourceFileRegistrationV2(state sourceCursorV2, logPath, kind string) (sourceFileRegistrationV2, string, error) {
	if err := validateSourceCursorV2(state); err != nil {
		return sourceFileRegistrationV2{}, "", err
	}
	if !state.ManifestRegistered || kind != "access" && kind != "error" {
		return sourceFileRegistrationV2{}, "", errors.New("source file registration cannot be built")
	}
	indices := make([]int, 0)
	for i := range state.Files {
		if !state.Files[i].Registered {
			indices = append(indices, i)
		}
	}
	if len(indices) == 0 {
		return sourceFileRegistrationV2{}, "", errors.New("there is no unregistered source file")
	}
	sort.Slice(indices, func(i, j int) bool { return state.Files[indices[i]].Generation < state.Files[indices[j]].Generation })
	registration := sourceFileRegistrationV2{Protocol: sourceCursorVersionV2, Kind: kind, SourceEpoch: state.SourceEpoch, ExpectedRevision: state.ManifestRevision, PreviousManifestHash: state.ManifestSHA256,
		Files: make([]sourceManifestFileV2, 0, len(indices))}
	for position, index := range indices {
		file := state.Files[index]
		if file.AckedOffset != 0 || file.CutoverBaseOffset != 0 || file.State == "missing" || file.State == "lost" || file.State == "retired" ||
			(position > 0 && file.Generation != state.Files[indices[position-1]].Generation+1) || file.Current != (position == len(indices)-1) {
			return sourceFileRegistrationV2{}, "", errors.New("unregistered source file generations are not a complete rotation suffix")
		}
		handle, err := openPinnedSourceV2(logPath, file)
		if err != nil {
			return sourceFileRegistrationV2{}, "", err
		}
		_ = handle.Close()
		registration.Files = append(registration.Files, sourceManifestFileV2{FileID: file.FileID, Generation: file.Generation, Device: file.Device, Inode: file.Inode, Current: file.Current})
	}
	hash, err := sourceFileRegistrationHashV2(registration)
	return registration, hash, err
}

func sourceFileRegistrationHashV2(registration sourceFileRegistrationV2) (string, error) {
	data, err := json.Marshal(registration)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func markSourceFileRegisteredV2(state sourceCursorV2, registration sourceFileRegistrationV2, serverRevision uint64, serverHash string) (sourceCursorV2, error) {
	expectedHash, err := sourceFileRegistrationHashV2(registration)
	if err != nil || expectedHash != serverHash || serverRevision != state.ManifestRevision+1 || registration.ExpectedRevision != state.ManifestRevision || registration.PreviousManifestHash != state.ManifestSHA256 {
		return state, errors.New("source file registration acknowledgement is invalid")
	}
	next := state
	next.Files = append([]fileWatermark(nil), state.Files...)
	found := 0
	for _, registered := range registration.Files {
		matched := false
		for i := range next.Files {
			file := &next.Files[i]
			if file.FileID != registered.FileID {
				continue
			}
			if file.Registered || file.Generation != registered.Generation || file.Device != registered.Device || file.Inode != registered.Inode || file.Current != registered.Current {
				return state, errors.New("source file registration identity changed before acknowledgement")
			}
			file.Registered, matched = true, true
			break
		}
		if !matched {
			return state, errors.New("source file registration references unknown file")
		}
		found++
	}
	if found == 0 {
		return state, errors.New("source file registration references unknown file")
	}
	next.ManifestRevision, next.ManifestSHA256 = serverRevision, serverHash
	if err := validateSourceCursorV2(next); err != nil {
		return state, err
	}
	return next, nil
}

type sourceManifestRegisterFuncV2 func(sourceManifestV2) (string, error)
type sourceFileRegisterFuncV2 func(sourceFileRegistrationV2) (uint64, string, error)
type sourceCursorSaveFuncV2 func(string, sourceCursorV2) error

type sourceCursorLockV2 struct{ file *os.File }

func acquireSourceCursorLockV2(cursorPath string) (*sourceCursorLockV2, error) {
	if strings.TrimSpace(cursorPath) == "" {
		return nil, errors.New("source cursor lock path is empty")
	}
	dir := filepath.Dir(cursorPath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	lockPath := cursorPath + ".lock"
	if info, err := os.Lstat(lockPath); err == nil && (info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular()) {
		return nil, errors.New("source cursor lock must be a regular non-symlink file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	closeWithError := func(err error) (*sourceCursorLockV2, error) { _ = file.Close(); return nil, err }
	if err := file.Chmod(0o600); err != nil {
		return closeWithError(err)
	}
	var stat syscall.Stat_t
	if err := syscall.Fstat(int(file.Fd()), &stat); err != nil {
		return closeWithError(err)
	}
	if stat.Nlink != 1 || int(stat.Uid) != os.Geteuid() {
		return closeWithError(errors.New("source cursor lock ownership is unsafe"))
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return closeWithError(errors.New("another collector owns the source cursor"))
	}
	return &sourceCursorLockV2{file: file}, nil
}

func (lock *sourceCursorLockV2) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	err := syscall.Flock(int(lock.file.Fd()), syscall.LOCK_UN)
	if closeErr := lock.file.Close(); err == nil {
		err = closeErr
	}
	lock.file = nil
	return err
}

// bootstrapSourceProtocolV2 persists the random epoch and complete initial
// manifest before the first network call. A successful server ACK is then
// persisted; if that second save fails, restart reuses the same epoch and
// retries the immutable manifest idempotently.
func bootstrapSourceProtocolV2(logPath, cursorPath, kind string, legacy cursor, now int64, save sourceCursorSaveFuncV2, register sourceManifestRegisterFuncV2) (sourceCursorV2, error) {
	if save == nil || register == nil {
		return sourceCursorV2{}, errors.New("source bootstrap dependencies are missing")
	}
	state, err := loadSourceCursorV2(cursorPath)
	if errors.Is(err, os.ErrNotExist) {
		state, err = migrateSourceCursorV1(logPath, legacy, now)
		if err != nil {
			return sourceCursorV2{}, err
		}
		if err := save(cursorPath, state); err != nil {
			return sourceCursorV2{}, fmt.Errorf("persist source epoch before registration: %w", err)
		}
	} else if err != nil {
		return sourceCursorV2{}, fmt.Errorf("load source bootstrap state: %w", err)
	}
	if state.ManifestRegistered {
		return state, nil
	}
	candidates, err := listSourceCandidatesV2(logPath)
	if err != nil {
		return sourceCursorV2{}, err
	}
	state, err = rebindPendingSourcePathsV2(state, candidates)
	if err != nil {
		return sourceCursorV2{}, err
	}
	if err := save(cursorPath, state); err != nil {
		return sourceCursorV2{}, fmt.Errorf("persist source paths before manifest retry: %w", err)
	}
	manifest, _, err := buildSourceManifestV2(state, kind)
	if err != nil {
		return sourceCursorV2{}, err
	}
	for _, file := range state.Files {
		handle, pinErr := openPinnedSourceV2(logPath, file)
		if pinErr != nil {
			return sourceCursorV2{}, pinErr
		}
		_ = handle.Close()
	}
	serverHash, err := register(manifest)
	if err != nil {
		return sourceCursorV2{}, err
	}
	registered, err := markSourceManifestRegisteredV2(state, kind, serverHash)
	if err != nil {
		return sourceCursorV2{}, err
	}
	if err := save(cursorPath, registered); err != nil {
		return sourceCursorV2{}, fmt.Errorf("persist source manifest acknowledgement: %w", err)
	}
	return registered, nil
}

// resumeSourceFileRegistrationV2 durably records every newly discovered
// rotation suffix before asking the server to register it. If the server ACK
// is received but the final cursor save fails, restart rebuilds the identical
// hash-chained registration and may safely retry it.
func resumeSourceFileRegistrationV2(logPath, cursorPath, kind string, now int64, save sourceCursorSaveFuncV2, register sourceFileRegisterFuncV2) (sourceCursorV2, error) {
	if save == nil || register == nil {
		return sourceCursorV2{}, errors.New("source file registration dependencies are missing")
	}
	state, err := loadSourceCursorV2(cursorPath)
	if err != nil {
		return sourceCursorV2{}, err
	}
	if !state.ManifestRegistered {
		return sourceCursorV2{}, errors.New("source manifest is not registered")
	}
	candidates, err := listSourceCandidatesV2(logPath)
	if err != nil {
		return sourceCursorV2{}, err
	}
	pending := false
	for _, file := range state.Files {
		pending = pending || !file.Registered
	}
	if pending {
		state, err = rebindPendingSourcePathsV2(state, candidates)
	} else {
		state, err = reconcileSourceCursorV2(state, candidates, now)
	}
	if err != nil {
		return sourceCursorV2{}, err
	}
	if err := save(cursorPath, state); err != nil {
		return sourceCursorV2{}, fmt.Errorf("persist source rotation before registration: %w", err)
	}
	registration, _, err := buildNextSourceFileRegistrationV2(state, logPath, kind)
	if err != nil {
		if strings.Contains(err.Error(), "there is no unregistered source file") {
			return state, nil
		}
		return sourceCursorV2{}, err
	}
	revision, serverHash, err := register(registration)
	if err != nil {
		return sourceCursorV2{}, err
	}
	registered, err := markSourceFileRegisteredV2(state, registration, revision, serverHash)
	if err != nil {
		return sourceCursorV2{}, err
	}
	if err := save(cursorPath, registered); err != nil {
		return sourceCursorV2{}, fmt.Errorf("persist source file registration acknowledgement: %w", err)
	}
	return registered, nil
}

// confirmSourceWriterReleasedV2 is deliberately separate from directory
// reconciliation. Only an external, verified Nginx-FD cutover may call it.
func confirmSourceWriterReleasedV2(state sourceCursorV2, fileID string) (sourceCursorV2, error) {
	if err := validateSourceCursorV2(state); err != nil {
		return state, err
	}
	state.Files = append([]fileWatermark(nil), state.Files...)
	found := false
	for i := range state.Files {
		file := &state.Files[i]
		if file.FileID != fileID {
			continue
		}
		if file.State != "missing" || file.Current || file.AckedOffset != file.LastObservedSize {
			return state, errors.New("writer release cannot retire an unverified source")
		}
		file.State, file.MissingSince, file.MissingScans = "retired", 0, 0
		found = true
		break
	}
	if !found {
		return state, errors.New("writer release references unknown source")
	}
	state.compactRetiredV2()
	return state, validateSourceCursorV2(state)
}

func (s *sourceCursorV2) compactRetiredV2() {
	const retainRetired = 64
	retired := 0
	for _, file := range s.Files {
		if file.State == "retired" {
			retired++
		}
	}
	remove := retired - retainRetired
	if remove <= 0 {
		return
	}
	kept := make([]fileWatermark, 0, len(s.Files)-remove)
	for _, file := range s.Files {
		if remove > 0 && file.State == "retired" {
			remove--
			continue
		}
		kept = append(kept, file)
	}
	s.Files = kept
}

func sourceBacklogV2(state sourceCursorV2) (int64, bool) {
	var total int64
	for _, file := range state.Files {
		if file.State == "lost" || file.State == "missing" || file.AckedOffset > file.LastObservedSize {
			return 0, false
		}
		remaining := file.LastObservedSize - file.AckedOffset
		if remaining > math.MaxInt64-total {
			return 0, false
		}
		total += remaining
	}
	return total, true
}

func saveSourceCursorV2(path string, state sourceCursorV2) error {
	if err := validateSourceCursorV2(state); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".nginx-source-v2-*.tmp")
	if err != nil {
		return err
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = dir.Sync()
	if closeErr := dir.Close(); err == nil {
		err = closeErr
	}
	return err
}

func loadSourceCursorV2(path string) (sourceCursorV2, error) {
	file, err := os.Open(path)
	if err != nil {
		return sourceCursorV2{}, err
	}
	defer file.Close()
	const maxCursorBytes = 1 << 20
	limited := io.LimitReader(file, maxCursorBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return sourceCursorV2{}, err
	}
	if len(data) > maxCursorBytes {
		return sourceCursorV2{}, errors.New("v2 source cursor is too large")
	}
	var state sourceCursorV2
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return sourceCursorV2{}, fmt.Errorf("decode v2 source cursor: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return sourceCursorV2{}, errors.New("v2 source cursor has trailing data")
	}
	if err := validateSourceCursorV2(state); err != nil {
		return sourceCursorV2{}, err
	}
	sort.SliceStable(state.Files, func(i, j int) bool { return state.Files[i].Generation < state.Files[j].Generation })
	return state, nil
}
