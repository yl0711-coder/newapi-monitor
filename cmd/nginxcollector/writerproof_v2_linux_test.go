//go:build linux

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestWriterReleaseProofFileRetiresMissingDrainedAccessAndError(t *testing.T) {
	dir := t.TempDir()
	type lane struct {
		name    string
		current writerReleaseIdentityV2
		old     writerReleaseIdentityV2
		state   sourceCursorV2
	}
	lanes := make([]lane, 0, 2)
	for laneIndex, name := range []string{"nexusapi_access.jsonl", "error.log"} {
		currentPath := filepath.Join(dir, name)
		oldPath := currentPath + ".1"
		if err := os.WriteFile(currentPath, nil, 0o640); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(oldPath, []byte("old\n"), 0o640); err != nil {
			t.Fatal(err)
		}
		current := writerIdentityForTest(t, currentPath, name)
		old := writerIdentityForTest(t, oldPath, name+".1")
		old.ReleasedAt = 200
		epoch := "0123456789abcdef0123456789abcdef"
		generation := uint64(laneIndex*2 + 1)
		state := sourceCursorV2{
			Version: sourceCursorVersionV2, SourceEpoch: epoch, NextGeneration: generation + 2,
			ManifestRegistered: true, ManifestSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ManifestRevision: 1,
			Files: []fileWatermark{
				{FileID: epoch + "-" + formatGeneration(generation), Generation: generation, Device: old.Device, Inode: old.Inode,
					AckedOffset: 4, FirstSeenAt: 100, LastSeenAt: 100, LastGrowthAt: 100, LastObservedSize: 4,
					AnchorSHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", PathBase: name + ".1",
					State: "missing", MissingSince: 150, MissingScans: 1, Registered: true},
				{FileID: epoch + "-" + formatGeneration(generation+1), Generation: generation + 1, Device: current.Device, Inode: current.Inode,
					FirstSeenAt: 100, LastSeenAt: 100, LastGrowthAt: 100, PathBase: name, State: "quiescent", Current: true, Registered: true},
			},
		}
		if err := os.Remove(oldPath); err != nil {
			t.Fatal(err)
		}
		lanes = append(lanes, lane{name: name, current: current, old: old, state: state})
	}
	proof := writerReleaseProofV2{
		Version: writerReleaseProofVersionV2, GeneratedAt: 200, ContainerID: testWriterContainerIDV2,
		StartedAt: "1970-01-01T00:00:10Z",
		Current:   []writerReleaseIdentityV2{lanes[0].current, lanes[1].current},
		Released:  []writerReleaseIdentityV2{lanes[0].old, lanes[1].old},
	}
	data, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	proofPath := filepath.Join(dir, ".nginx-writer-release-v2.json")
	if err := os.WriteFile(proofPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if os.Geteuid() != 0 {
		_, changed, err := applyWriterReleaseProofV2(lanes[0].state, filepath.Join(dir, lanes[0].name), proofPath, 200)
		if err == nil || changed {
			t.Fatalf("production loader accepted a non-root proof: changed=%v err=%v", changed, err)
		}
	}
	trustedUID := uint32(os.Geteuid())
	for _, lane := range lanes {
		next, changed, err := applyWriterReleaseProofOwnedByV2(lane.state, filepath.Join(dir, lane.name), proofPath, 200, trustedUID)
		if err != nil || !changed || next.Files[0].State != "retired" {
			t.Fatalf("%s did not consume shared root-owned proof: changed=%v state=%+v err=%v", lane.name, changed, next.Files, err)
		}
	}
}

func writerIdentityForTest(t *testing.T, path, name string) writerReleaseIdentityV2 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("missing linux stat identity")
	}
	return writerReleaseIdentityV2{Name: name, Device: uint64(stat.Dev), Inode: stat.Ino}
}
