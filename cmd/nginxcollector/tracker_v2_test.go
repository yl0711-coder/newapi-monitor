package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func writeSourceFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

func sourceInode(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fileInode(info)
}

func sourceDev(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return sourceDevice(info)
}

func sourceHash(t *testing.T, path string, start, end int64) string {
	t.Helper()
	hash, err := hashSourceRangeV2(path, start, end)
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func registerTestSourceManifest(t *testing.T, state sourceCursorV2, kind string) sourceCursorV2 {
	t.Helper()
	_, hash, err := buildSourceManifestV2(state, kind)
	if err != nil {
		t.Fatal(err)
	}
	state, err = markSourceManifestRegisteredV2(state, kind, hash)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func acknowledgedLegacyCursor(device, inode uint64, offset int64) cursor {
	return cursor{Version: 1, Device: device, Inode: inode, Offset: offset, LastAckedBatchID: "legacy_batch_abcdefgh", LastAckedDevice: device, LastAckedInode: inode, LastAckedOffset: offset}
}

func TestMigrateSourceCursorV1RequiresAcknowledgedBoundary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "one\n")
	legacy := cursor{Version: 1, Device: sourceDev(t, path), Inode: sourceInode(t, path), Offset: 4}
	if _, err := migrateSourceCursorV1(path, legacy, 100); !errors.Is(err, errSourceCursorLoss) {
		t.Fatalf("unbound legacy cursor must fail closed: %v", err)
	}
}

func TestMigrateSourceCursorV1CurrentPreservesOffsetAndTelemetry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "one\ntwo\n")
	legacy := acknowledgedLegacyCursor(sourceDev(t, path), sourceInode(t, path), 4)
	legacy.DiscardedLines, legacy.LastDiscardedAt = 3, 10
	state, err := migrateSourceCursorV1(path, legacy, 100)
	if err != nil {
		t.Fatal(err)
	}
	if state.Version != 2 || len(state.Files) != 1 || state.Files[0].AckedOffset != 4 || state.Files[0].CutoverBaseOffset != 4 || !state.Files[0].Current {
		t.Fatalf("unexpected migration: %+v", state)
	}
	if state.Telemetry.DiscardedLines != 3 || state.Telemetry.LastDiscardedAt != 10 {
		t.Fatalf("v1 telemetry was not preserved: %+v", state.Telemetry)
	}
}

func TestMigrateSourceCursorV1RenamedIncludesOnlySuccessors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "old\n")
	oldInode := sourceInode(t, path)
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, path, "current\n")
	oldTime := time.Now().Add(-time.Hour)
	_ = os.Chtimes(path+".1", oldTime, oldTime)
	state, err := migrateSourceCursorV1(path, acknowledgedLegacyCursor(sourceDev(t, path+".1"), oldInode, 4), 100)
	if err != nil {
		t.Fatal(err)
	}
	state = registerTestSourceManifest(t, state, "access")
	if len(state.Files) != 2 || state.Files[0].PathBase != "access.jsonl.1" || state.Files[1].PathBase != "access.jsonl" {
		t.Fatalf("migration must include the v1 file and successors only: %+v", state.Files)
	}
}

func TestV1CutoverManifestRegistersRotatedEOFAndCurrentBeforeData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "old\n")
	oldInode := sourceInode(t, path)
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, path, "current\n")
	state, err := migrateSourceCursorV1(path, acknowledgedLegacyCursor(sourceDev(t, path+".1"), oldInode, 4), 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := selectReadableWatermarkV2(&state); ok {
		t.Fatal("collector must not send data before manifest acknowledgement")
	}
	manifest, hash, err := buildSourceManifestV2(state, "access")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Files) != 2 || manifest.Files[0].BaseOffset != 4 || manifest.Files[0].Current || !manifest.Files[1].Current || manifest.LegacyAckedBatchID != "legacy_batch_abcdefgh" {
		t.Fatalf("manifest did not atomically describe both files: %+v", manifest.Files)
	}
	state, err = markSourceManifestRegisteredV2(state, "access", hash)
	if err != nil {
		t.Fatal(err)
	}
	if index, ok := selectReadableWatermarkV2(&state); !ok || !state.Files[index].Current {
		t.Fatalf("current file should become readable only after manifest ACK: %+v", state.Files)
	}
}

func TestMigrateSourceCursorV1RejectsUnrepresentedPredecessor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path+".1", "still-growing\n")
	oldTime := time.Now().Add(-time.Hour)
	_ = os.Chtimes(path+".1", oldTime, oldTime)
	writeSourceFile(t, path, "")
	_, err := migrateSourceCursorV1(path, acknowledgedLegacyCursor(sourceDev(t, path), sourceInode(t, path), 0), 100)
	if !errors.Is(err, errSourceCursorLoss) {
		t.Fatalf("v1 current cannot prove predecessor was consumed: %v", err)
	}
}

func TestMigrateSourceCursorV1FailsClosedWhenIdentityMissingOrTruncated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "one\n")
	if _, err := migrateSourceCursorV1(path, acknowledgedLegacyCursor(sourceDev(t, path), 999999999, 0), 100); !errors.Is(err, errSourceCursorLoss) {
		t.Fatalf("missing v1 inode must fail closed: %v", err)
	}
	if _, err := migrateSourceCursorV1(path, acknowledgedLegacyCursor(sourceDev(t, path), sourceInode(t, path), 99), 100); !errors.Is(err, errSourceCursorTruncate) {
		t.Fatalf("truncated v1 source must fail closed: %v", err)
	}
}

func TestSourceCursorV2ReadsLateGrowthFromRotatedEOF(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "old\n")
	oldInode := sourceInode(t, path)
	state, err := migrateSourceCursorV1(path, acknowledgedLegacyCursor(sourceDev(t, path), oldInode, 4), 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, path, "current\n")
	candidates, _ := listSourceCandidatesV2(path)
	state, err = reconcileSourceCursorV2(state, candidates, 110)
	if err != nil {
		t.Fatal(err)
	}
	state = registerTestSourceManifest(t, state, "access")
	index, ok := selectReadableWatermarkV2(&state)
	if !ok || !state.Files[index].Current {
		t.Fatalf("current data should be readable while old is at EOF: %+v", state.Files)
	}
	current := state.Files[index]
	if err := acknowledgeSourceRangeV2(&state, current.FileID, 0, current.LastObservedSize, 0, sourceHash(t, path, 0, current.LastObservedSize), 111); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path+".1", os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = file.WriteString("late\n")
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	candidates, _ = listSourceCandidatesV2(path)
	state, err = reconcileSourceCursorV2(state, candidates, 120)
	if err != nil {
		t.Fatal(err)
	}
	index, ok = selectReadableWatermarkV2(&state)
	if !ok || state.Files[index].Current || state.Files[index].Inode != oldInode || state.Files[index].AckedOffset != 4 {
		t.Fatalf("late growth must resume from old acknowledged offset: %+v", state.Files)
	}
	if state.Telemetry.LateGrowths != 1 || state.Telemetry.LastLateGrowthAt != 120 {
		t.Fatalf("late growth telemetry missing: %+v", state.Telemetry)
	}
}

func TestSourceCursorV2DoesNotStarveRotatedBacklog(t *testing.T) {
	state := sourceCursorV2{Version: 2, SourceEpoch: "0123456789abcdef0123456789abcdef", NextGeneration: 3, ManifestRegistered: true, ManifestSHA256: strings.Repeat("a", 64), ManifestRevision: 1, Files: []fileWatermark{
		{FileID: "0123456789abcdef0123456789abcdef-0000000000000001", Generation: 1, Device: 1, Inode: 1, AckedOffset: 0, FirstSeenAt: 1, LastSeenAt: 1, LastGrowthAt: 1, LastObservedSize: 10, PathBase: "access.1", State: "active", Registered: true},
		{FileID: "0123456789abcdef0123456789abcdef-0000000000000002", Generation: 2, Device: 1, Inode: 2, AckedOffset: 0, FirstSeenAt: 1, LastSeenAt: 1, LastGrowthAt: 1, LastObservedSize: 10, PathBase: "access", State: "active", Current: true, Registered: true},
	}}
	var old, current int
	for i := 0; i < 10; i++ {
		index, ok := selectReadableWatermarkV2(&state)
		if !ok {
			t.Fatal("readable files disappeared")
		}
		if state.Files[index].Current {
			current++
		} else {
			old++
		}
	}
	if current != 8 || old != 2 {
		t.Fatalf("fairness current=%d old=%d", current, old)
	}
}

func TestSourceCursorV2FailsWhenUnreadFileDisappears(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "unread\n")
	oldInode := sourceInode(t, path)
	state, err := migrateSourceCursorV1(path, acknowledgedLegacyCursor(sourceDev(t, path), oldInode, 0), 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, path, "current\n")
	candidates, _ := listSourceCandidatesV2(path)
	state, err = reconcileSourceCursorV2(state, candidates, 110)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path + ".1"); err != nil {
		t.Fatal(err)
	}
	candidates, _ = listSourceCandidatesV2(path)
	state, err = reconcileSourceCursorV2(state, candidates, 120)
	if !errors.Is(err, errSourceCursorLoss) || state.Telemetry.LostFiles != 1 {
		t.Fatalf("unread disappearance must fail closed: state=%+v err=%v", state, err)
	}
}

func TestSourceCursorV2LostStateBlocksAfterRestart(t *testing.T) {
	epoch := "0123456789abcdef0123456789abcdef"
	state := sourceCursorV2{Version: 2, SourceEpoch: epoch, NextGeneration: 3, ManifestRegistered: true, ManifestSHA256: strings.Repeat("a", 64), ManifestRevision: 1, Telemetry: sourceTelemetryV2{LostFiles: 1, LastLossAt: 2}, Files: []fileWatermark{
		{FileID: epoch + "-0000000000000001", Generation: 1, Device: 1, Inode: 1, AckedOffset: 0, FirstSeenAt: 1, LastSeenAt: 1, LastGrowthAt: 1, LastObservedSize: 10, PathBase: "access.1", State: "lost", Registered: true},
		{FileID: epoch + "-0000000000000002", Generation: 2, Device: 1, Inode: 2, AckedOffset: 0, FirstSeenAt: 1, LastSeenAt: 1, LastGrowthAt: 1, LastObservedSize: 10, PathBase: "access", State: "active", Current: true, Registered: true},
	}}
	path := filepath.Join(t.TempDir(), "cursor.json")
	if err := saveSourceCursorV2(path, state); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadSourceCursorV2(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := selectReadableWatermarkV2(&loaded); ok {
		t.Fatal("lost lane must remain blocked after restart")
	}
	if err := acknowledgeSourceRangeV2(&loaded, state.Files[1].FileID, 0, 1, 0, strings.Repeat("a", 64), 3); !errors.Is(err, errSourceCursorLoss) {
		t.Fatalf("lost lane accepted acknowledgement: %v", err)
	}
}

func TestListSourceCandidatesV2RejectsHardlinkIdentity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "current\n")
	if err := os.Link(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	if _, err := listSourceCandidatesV2(path); err == nil {
		t.Fatal("two paths for one physical inode must fail closed")
	}
}

func TestListSourceCandidatesV2RotationDuringScanReturnsRetry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "old\n")
	_, err := listSourceCandidatesWithHookV2(path, func() {
		if renameErr := os.Rename(path, path+".1"); renameErr != nil {
			t.Fatal(renameErr)
		}
		writeSourceFile(t, path, "new\n")
	})
	if !errors.Is(err, errSourceSnapshotChange) {
		t.Fatalf("mixed rotation snapshot must be retried: %v", err)
	}
}

func TestSourceCursorV2RejectsNonContiguousAckAndPersists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "one\ntwo\n")
	state, err := migrateSourceCursorV1(path, cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	state = registerTestSourceManifest(t, state, "access")
	file := state.Files[0]
	if err := acknowledgeSourceRangeV2(&state, file.FileID, 1, 4, 1, sourceHash(t, path, 1, 4), 101); err == nil {
		t.Fatal("overlapping/gapped acknowledgement must be rejected")
	}
	if err := acknowledgeSourceRangeV2(&state, file.FileID, 0, 4, 0, sourceHash(t, path, 0, 4), 101); err != nil {
		t.Fatal(err)
	}
	cursorPath := filepath.Join(dir, "cursor-v2.json")
	if err := saveSourceCursorV2(cursorPath, state); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadSourceCursorV2(cursorPath)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Files[0].AckedOffset != 4 || loaded.SourceEpoch != state.SourceEpoch {
		t.Fatalf("durable cursor changed: %+v", loaded)
	}
	if backlog, known := sourceBacklogV2(loaded); !known || backlog != 4 {
		t.Fatalf("backlog=%d known=%v want=4,true", backlog, known)
	}
}

func TestSourceCursorV2MissingIsUnknownUntilWriterReleaseProof(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "old\n")
	state, err := migrateSourceCursorV1(path, acknowledgedLegacyCursor(sourceDev(t, path), sourceInode(t, path), 4), 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, path, "")
	candidates, _ := listSourceCandidatesV2(path)
	state, err = reconcileSourceCursorV2(state, candidates, 110)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path + ".1"); err != nil {
		t.Fatal(err)
	}
	candidates, _ = listSourceCandidatesV2(path)
	state, err = reconcileSourceCursorV2(state, candidates, 120)
	if err != nil {
		t.Fatal(err)
	}
	oldID := state.Files[0].FileID
	if state.Files[0].State != "missing" {
		t.Fatalf("disappeared EOF file must remain unproven: %+v", state.Files[0])
	}
	if backlog, known := sourceBacklogV2(state); known || backlog != 0 {
		t.Fatalf("missing writer proof must make backlog unknown: %d %v", backlog, known)
	}
	if _, ok := selectReadableWatermarkV2(&state); ok {
		t.Fatal("lane must not advance across an unproven missing file")
	}
	state, err = confirmSourceWriterReleasedV2(state, oldID)
	if err != nil || state.Files[0].State != "retired" {
		t.Fatalf("explicit writer release should retire the file: state=%+v err=%v", state.Files, err)
	}
}

func TestReconcileSourceCursorV2ErrorDoesNotMutateInput(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "one\ntwo\n")
	state, err := migrateSourceCursorV1(path, cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	beforeData, _ := json.Marshal(state)
	var before sourceCursorV2
	_ = json.Unmarshal(beforeData, &before)
	candidates, _ := listSourceCandidatesV2(path)
	candidates[0].Size = -1
	if _, err := reconcileSourceCursorV2(state, candidates, 110); err == nil {
		t.Fatal("invalid candidate must fail")
	}
	if !reflect.DeepEqual(state, before) {
		t.Fatalf("error path mutated caller state:\nbefore=%+v\nafter=%+v", before, state)
	}
	if _, err := reconcileSourceCursorV2(state, nil, 90); err == nil {
		t.Fatal("clock rollback must fail")
	}
	if !reflect.DeepEqual(state, before) {
		t.Fatal("clock rollback mutated caller state")
	}
}

func TestReconcileSourceCursorV2RejectsDuplicatePhysicalCandidate(t *testing.T) {
	state := sourceCursorV2{Version: 2, SourceEpoch: "0123456789abcdef0123456789abcdef", NextGeneration: 2, Files: []fileWatermark{{
		FileID: "0123456789abcdef0123456789abcdef-0000000000000001", Generation: 1, Device: 1, Inode: 1,
		FirstSeenAt: 1, LastSeenAt: 1, LastGrowthAt: 1, PathBase: "access", State: "quiescent", Current: true,
	}}}
	candidate := sourceCandidateV2{Path: "/a", Base: "access", Device: 1, Inode: 1, Current: true}
	if _, err := reconcileSourceCursorV2(state, []sourceCandidateV2{candidate, candidate}, 2); err == nil {
		t.Fatal("hardlink/duplicate physical identity must fail closed")
	}
}

func TestValidateSourceCursorV2RejectsImpossibleState(t *testing.T) {
	base := sourceCursorV2{Version: 2, SourceEpoch: "0123456789abcdef0123456789abcdef", NextGeneration: 2, Files: []fileWatermark{{
		FileID: "0123456789abcdef0123456789abcdef-0000000000000001", Generation: 1, Device: 1, Inode: 1,
		FirstSeenAt: 1, LastSeenAt: 1, LastGrowthAt: 1, PathBase: "access", State: "quiescent", Current: true,
	}}}
	for _, mutate := range []func(*sourceCursorV2){
		func(s *sourceCursorV2) { s.RoundRobin = 5 },
		func(s *sourceCursorV2) { s.NextGeneration = 1 },
		func(s *sourceCursorV2) { s.Files[0].State, s.Files[0].LastObservedSize = "quiescent", 1 },
		func(s *sourceCursorV2) { s.Files[0].State, s.Files[0].LastObservedSize = "active", 0 },
		func(s *sourceCursorV2) { s.Files[0].State = "retired" },
		func(s *sourceCursorV2) { s.Files[0].AnchorSHA256 = "bad" },
		func(s *sourceCursorV2) { s.Files[0].AckedOffset, s.Files[0].LastObservedSize = 1, 1 },
		func(s *sourceCursorV2) { s.Files[0].FileID = s.SourceEpoch + "-0000000000000002" },
	} {
		value := base
		value.Files = append([]fileWatermark(nil), base.Files...)
		mutate(&value)
		if err := validateSourceCursorV2(value); err == nil {
			t.Fatalf("impossible state accepted: %+v", value)
		}
	}
}

func TestValidateSourceCursorV2RejectsDuplicatePhysicalIdentity(t *testing.T) {
	epoch := "0123456789abcdef0123456789abcdef"
	state := sourceCursorV2{Version: 2, SourceEpoch: epoch, NextGeneration: 3, Files: []fileWatermark{
		{FileID: epoch + "-0000000000000001", Generation: 1, Device: 1, Inode: 9, FirstSeenAt: 1, LastSeenAt: 1, LastGrowthAt: 1, PathBase: "access.1", State: "quiescent"},
		{FileID: epoch + "-0000000000000002", Generation: 2, Device: 1, Inode: 9, FirstSeenAt: 1, LastSeenAt: 1, LastGrowthAt: 1, PathBase: "access", State: "quiescent", Current: true},
	}}
	if err := validateSourceCursorV2(state); err == nil {
		t.Fatal("duplicate physical identity must be rejected")
	}
}

func TestAcknowledgeSourceRangeV2ClockRollbackDoesNotMutate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "one\n")
	state, err := migrateSourceCursorV1(path, cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	state = registerTestSourceManifest(t, state, "access")
	before := state
	before.Files = append([]fileWatermark(nil), state.Files...)
	err = acknowledgeSourceRangeV2(&state, state.Files[0].FileID, 0, 4, 0, sourceHash(t, path, 0, 4), 99)
	if err == nil || !reflect.DeepEqual(state, before) {
		t.Fatalf("clock rollback must fail without mutation: err=%v before=%+v after=%+v", err, before, state)
	}
}

func TestAckLostThenBatchShrinksRecoversWithoutDuplicate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	data := strings.Repeat("0123456789", 100)
	writeSourceFile(t, path, data)
	state, err := migrateSourceCursorV1(path, cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	state = registerTestSourceManifest(t, state, "access")
	content, err := hashSourceBatchRangeV2(path, 0, int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	recovery := sourceAckRecoveryV2{SourceEpoch: state.SourceEpoch, FileID: state.Files[0].FileID, NextOffset: int64(len(data)), Proofs: []sourceAckProofV2{{StartOffset: 0, EndOffset: int64(len(data)), ContentSHA256: content}}}
	recovered, err := recoverSourceAcknowledgementsV2(state, path, recovery, 101)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Files[0].AckedOffset != int64(len(data)) {
		t.Fatalf("lost acknowledgement did not advance to server proof: %+v", recovered.Files[0])
	}
	if index, ok := selectReadableWatermarkV2(&recovered); ok {
		t.Fatalf("already accepted bytes became readable again at index %d", index)
	}
}

func TestAckRecoveryMismatchDoesNotMutate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "one\n")
	state, err := migrateSourceCursorV1(path, cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	state = registerTestSourceManifest(t, state, "error")
	before := state
	before.Files = append([]fileWatermark(nil), state.Files...)
	recovery := sourceAckRecoveryV2{SourceEpoch: state.SourceEpoch, FileID: state.Files[0].FileID, NextOffset: 4, Proofs: []sourceAckProofV2{{StartOffset: 0, EndOffset: 4, ContentSHA256: strings.Repeat("f", 64)}}}
	if _, err := recoverSourceAcknowledgementsV2(state, path, recovery, 101); err == nil || !reflect.DeepEqual(state, before) {
		t.Fatalf("invalid server proof must fail without mutation: %v", err)
	}
}

func TestBootstrapSourceV2PersistsEpochBeforeNetwork(t *testing.T) {
	dir := t.TempDir()
	logPath, cursorPath := filepath.Join(dir, "access.jsonl"), filepath.Join(dir, "cursor-v2.json")
	writeSourceFile(t, logPath, "one\n")
	postCalls := 0
	failedSave := func(string, sourceCursorV2) error { return errors.New("fsync failed") }
	post := func(sourceManifestV2) (string, error) { postCalls++; return strings.Repeat("a", 64), nil }
	if _, err := bootstrapSourceProtocolV2(logPath, cursorPath, "access", cursor{}, 100, failedSave, post); err == nil || postCalls != 0 {
		t.Fatalf("network was called before durable epoch: calls=%d err=%v", postCalls, err)
	}
	var firstEpoch string
	save := func(path string, state sourceCursorV2) error {
		if firstEpoch == "" {
			firstEpoch = state.SourceEpoch
		} else if state.SourceEpoch != firstEpoch {
			t.Fatalf("epoch changed across bootstrap saves: %s -> %s", firstEpoch, state.SourceEpoch)
		}
		return saveSourceCursorV2(path, state)
	}
	post = func(manifest sourceManifestV2) (string, error) {
		postCalls++
		data, _ := json.Marshal(manifest)
		sum := sha256.Sum256(data)
		return hex.EncodeToString(sum[:]), nil
	}
	state, err := bootstrapSourceProtocolV2(logPath, cursorPath, "access", cursor{}, 100, save, post)
	if err != nil || !state.ManifestRegistered || postCalls != 1 {
		t.Fatalf("bootstrap failed: state=%+v calls=%d err=%v", state, postCalls, err)
	}
	loaded, err := loadSourceCursorV2(cursorPath)
	if err != nil || loaded.SourceEpoch != firstEpoch || !loaded.ManifestRegistered {
		t.Fatalf("registered epoch not durable: loaded=%+v err=%v", loaded, err)
	}
}

func TestBootstrapSourceV2ReusesEpochAfterServerAckSaveFailure(t *testing.T) {
	dir := t.TempDir()
	logPath, cursorPath := filepath.Join(dir, "access.jsonl"), filepath.Join(dir, "cursor-v2.json")
	writeSourceFile(t, logPath, "one\n")
	var accepted sourceManifestV2
	var acceptedHash, epoch string
	registerCalls := 0
	register := func(manifest sourceManifestV2) (string, error) {
		registerCalls++
		data, _ := json.Marshal(manifest)
		sum := sha256.Sum256(data)
		hash := hex.EncodeToString(sum[:])
		if registerCalls == 1 {
			accepted, acceptedHash, epoch = manifest, hash, manifest.SourceEpoch
		} else if !reflect.DeepEqual(manifest, accepted) || hash != acceptedHash || manifest.SourceEpoch != epoch {
			t.Fatalf("restart changed accepted manifest: first=%+v retry=%+v", accepted, manifest)
		}
		return acceptedHash, nil
	}
	saves := 0
	failAckSave := func(path string, state sourceCursorV2) error {
		saves++
		if saves == 3 {
			return errors.New("simulated acknowledgement fsync failure")
		}
		return saveSourceCursorV2(path, state)
	}
	if _, err := bootstrapSourceProtocolV2(logPath, cursorPath, "access", cursor{}, 100, failAckSave, register); err == nil {
		t.Fatal("acknowledgement save failure must be reported")
	}
	if err := os.Rename(logPath, logPath+".1"); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, logPath, "new\n")
	persisted, err := loadSourceCursorV2(cursorPath)
	if err != nil || persisted.ManifestRegistered || persisted.SourceEpoch != epoch {
		t.Fatalf("pre-network state was not recoverable: state=%+v err=%v", persisted, err)
	}
	resumed, err := bootstrapSourceProtocolV2(logPath, cursorPath, "access", cursor{}, 101, saveSourceCursorV2, register)
	if err != nil || !resumed.ManifestRegistered || resumed.SourceEpoch != epoch || registerCalls != 2 {
		t.Fatalf("resume failed: state=%+v calls=%d err=%v", resumed, registerCalls, err)
	}
}

func TestSourceFileRegistrationRetriesIdenticalSuffixAfterAckSaveFailure(t *testing.T) {
	dir := t.TempDir()
	logPath, cursorPath := filepath.Join(dir, "access.jsonl"), filepath.Join(dir, "cursor-v2.json")
	writeSourceFile(t, logPath, "old\n")
	state, err := migrateSourceCursorV1(logPath, cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	state = registerTestSourceManifest(t, state, "access")
	if err := acknowledgeSourceRangeV2(&state, state.Files[0].FileID, 0, 4, 0, sourceHash(t, logPath, 0, 4), 101); err != nil {
		t.Fatal(err)
	}
	if err := saveSourceCursorV2(cursorPath, state); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(logPath, logPath+".1"); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, logPath, "new\n")

	var accepted sourceFileRegistrationV2
	var acceptedHash string
	calls := 0
	register := func(registration sourceFileRegistrationV2) (uint64, string, error) {
		calls++
		hash, err := sourceFileRegistrationHashV2(registration)
		if err != nil {
			return 0, "", err
		}
		if calls == 1 {
			accepted, acceptedHash = registration, hash
		} else if calls == 2 && (!reflect.DeepEqual(accepted, registration) || acceptedHash != hash) {
			t.Fatalf("restart changed accepted suffix: first=%+v retry=%+v", accepted, registration)
		}
		if calls <= 2 {
			return registration.ExpectedRevision + 1, acceptedHash, nil
		}
		return registration.ExpectedRevision + 1, hash, nil
	}
	saves := 0
	failAckSave := func(path string, state sourceCursorV2) error {
		saves++
		if saves == 2 {
			return errors.New("simulated registration acknowledgement fsync failure")
		}
		return saveSourceCursorV2(path, state)
	}
	if _, err := resumeSourceFileRegistrationV2(logPath, cursorPath, "access", 110, failAckSave, register); err == nil {
		t.Fatal("registration acknowledgement save failure must be reported")
	}
	if err := os.Rename(logPath, logPath+".2"); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, logPath, "newer\n")
	persisted, err := loadSourceCursorV2(cursorPath)
	if err != nil || len(persisted.Files) != 2 || persisted.Files[1].Registered {
		t.Fatalf("unregistered suffix was not durably recoverable: state=%+v err=%v", persisted, err)
	}
	resumed, err := resumeSourceFileRegistrationV2(logPath, cursorPath, "access", 111, saveSourceCursorV2, register)
	if err != nil || len(resumed.Files) != 2 || !resumed.Files[1].Registered || calls != 2 {
		t.Fatalf("suffix resume failed: state=%+v calls=%d err=%v", resumed, calls, err)
	}
	advanced, err := resumeSourceFileRegistrationV2(logPath, cursorPath, "access", 112, saveSourceCursorV2, register)
	if err != nil || len(advanced.Files) != 3 || !advanced.Files[2].Registered || calls != 3 {
		t.Fatalf("new rotation was not registered after pending suffix: state=%+v calls=%d err=%v", advanced, calls, err)
	}
}

func TestRotatedCurrentMustRegisterBeforeRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "old\n")
	state, err := migrateSourceCursorV1(path, cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	state = registerTestSourceManifest(t, state, "access")
	if err := acknowledgeSourceRangeV2(&state, state.Files[0].FileID, 0, 4, 0, sourceHash(t, path, 0, 4), 101); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, path, "new\n")
	candidates, err := listSourceCandidatesV2(path)
	if err != nil {
		t.Fatal(err)
	}
	state, err = reconcileSourceCursorV2(state, candidates, 110)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := selectReadableWatermarkV2(&state); ok {
		t.Fatal("new current bytes must remain blocked before file registration")
	}
	registration, hash, err := buildNextSourceFileRegistrationV2(state, path, "access")
	if err != nil {
		t.Fatal(err)
	}
	state, err = markSourceFileRegisteredV2(state, registration, 2, hash)
	if err != nil {
		t.Fatal(err)
	}
	index, ok := selectReadableWatermarkV2(&state)
	if !ok || !state.Files[index].Current || !state.Files[index].Registered {
		t.Fatalf("registered current file is not readable: %+v", state.Files)
	}
}

func TestMultipleOfflineRotationsRegisterAsOneContiguousSuffix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "g1\n")
	state, err := migrateSourceCursorV1(path, cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	state = registerTestSourceManifest(t, state, "access")
	if err := acknowledgeSourceRangeV2(&state, state.Files[0].FileID, 0, 3, 0, sourceHash(t, path, 0, 3), 101); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, path+".2"); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, path, "g2\n")
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, path, "g3\n")
	baseTime := time.Now().Add(-2 * time.Hour)
	_ = os.Chtimes(path+".2", baseTime, baseTime)
	_ = os.Chtimes(path+".1", baseTime.Add(time.Hour), baseTime.Add(time.Hour))
	candidates, err := listSourceCandidatesV2(path)
	if err != nil {
		t.Fatal(err)
	}
	state, err = reconcileSourceCursorV2(state, candidates, 110)
	if err != nil {
		t.Fatal(err)
	}
	registration, hash, err := buildNextSourceFileRegistrationV2(state, path, "access")
	if err != nil {
		t.Fatal(err)
	}
	if len(registration.Files) != 2 || registration.Files[0].Current || !registration.Files[1].Current || registration.Files[1].Generation != registration.Files[0].Generation+1 {
		t.Fatalf("offline rotations were not one contiguous suffix: %+v", registration.Files)
	}
	state, err = markSourceFileRegisteredV2(state, registration, 2, hash)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range state.Files {
		if !file.Registered {
			t.Fatalf("generation remained unregistered: %+v", state.Files)
		}
	}
}

func TestSourceCursorV2LockPreventsTwoCollectors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursor-v2.json")
	first, err := acquireSourceCursorLockV2(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := acquireSourceCursorLockV2(path); err == nil {
		_ = second.Close()
		t.Fatal("two collectors acquired the same cursor lock")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := acquireSourceCursorLockV2(path)
	if err != nil {
		t.Fatalf("released cursor lock cannot be reacquired: %v", err)
	}
	_ = third.Close()
}

func TestPinnedSourceFDIsStableWhenPathIsReplaced(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, path, "original\n")
	state, err := migrateSourceCursorV1(path, cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := openPinnedSourceV2(path, state.Files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	if err := os.Rename(path, path+".1"); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, path, "replacement\n")
	got, err := hashPinnedSourceRangeV2(handle, 0, int64(len("original\n")), maxSourceRangeBytesV2)
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256([]byte("original\n"))
	if got != hex.EncodeToString(want[:]) {
		t.Fatalf("pinned FD followed replacement path: got=%s want=%x", got, want)
	}
}

func TestSourceCursorV2CompactsOnlyExplicitlyRetiredTombstones(t *testing.T) {
	epoch := "0123456789abcdef0123456789abcdef"
	state := sourceCursorV2{Version: 2, SourceEpoch: epoch, NextGeneration: 82}
	for i := 1; i <= 80; i++ {
		state.Files = append(state.Files, fileWatermark{FileID: epoch + "-" + strings.Repeat("0", 14) + string("00"), Generation: uint64(i), Device: 1, Inode: uint64(i), FirstSeenAt: 1, LastSeenAt: 1, LastGrowthAt: 1, PathBase: "old", State: "retired"})
	}
	// File IDs above are intentionally overwritten with valid unique IDs.
	for i := range state.Files {
		state.Files[i].FileID = epoch + "-" + formatGeneration(state.Files[i].Generation)
	}
	state.Files = append(state.Files, fileWatermark{FileID: epoch + "-" + formatGeneration(81), Generation: 81, Device: 1, Inode: 81, FirstSeenAt: 1, LastSeenAt: 1, LastGrowthAt: 1, PathBase: "access", State: "quiescent", Current: true})
	state.compactRetiredV2()
	if len(state.Files) != 65 || state.Files[len(state.Files)-1].Generation != 81 {
		t.Fatalf("unexpected compacted state: len=%d tail=%+v", len(state.Files), state.Files[len(state.Files)-1])
	}
}

func TestLoadSourceCursorV2RejectsUnknownTrailingAndOversize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cursor.json")
	for name, data := range map[string]string{
		"unknown":  `{"version":2,"unknown":true}`,
		"trailing": `{}` + `{}`,
		"oversize": strings.Repeat("x", (1<<20)+1),
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadSourceCursorV2(path); err == nil {
				t.Fatal("corrupt cursor must be rejected")
			}
		})
	}
}

func TestSaveSourceCursorV2IgnoresStaleLegacyTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cursor.json")
	writeSourceFile(t, filepath.Join(dir, ".nginx-source-v2-stale.tmp"), "partial")
	logPath := filepath.Join(dir, "access.jsonl")
	writeSourceFile(t, logPath, "one\n")
	state, err := migrateSourceCursorV1(logPath, cursor{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := saveSourceCursorV2(path, state); err != nil {
		t.Fatalf("stale unrelated temp must not block atomic save: %v", err)
	}
}
