package main

import "testing"

const testWriterContainerIDV2 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func testWriterReleaseStateV2() sourceCursorV2 {
	epoch := "0123456789abcdef0123456789abcdef"
	return sourceCursorV2{
		Version: sourceCursorVersionV2, SourceEpoch: epoch, NextGeneration: 3,
		ManifestRegistered: true, ManifestSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ManifestRevision: 1,
		Files: []fileWatermark{
			{FileID: epoch + "-0000000000000001", Generation: 1, Device: 1, Inode: 11, AckedOffset: 80, FirstSeenAt: 10, LastSeenAt: 30, LastGrowthAt: 20, LastObservedSize: 80, AnchorSHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", MissingSince: 30, MissingScans: 1, PathBase: "access.log.1", State: "missing", Registered: true},
			{FileID: epoch + "-0000000000000002", Generation: 2, Device: 1, Inode: 12, FirstSeenAt: 20, LastSeenAt: 30, LastGrowthAt: 20, PathBase: "access.log", State: "quiescent", Current: true, Registered: true},
		},
	}
}

func testWriterReleaseProofV2(releasedAt int64) writerReleaseProofV2 {
	return writerReleaseProofV2{
		Version: writerReleaseProofVersionV2, GeneratedAt: 40, ContainerID: testWriterContainerIDV2, StartedAt: "1970-01-01T00:00:10Z",
		Current:  []writerReleaseIdentityV2{{Name: "access.log", Device: 1, Inode: 12}},
		Released: []writerReleaseIdentityV2{{Name: "access.log.1", Device: 1, Inode: 11, ReleasedAt: releasedAt}},
	}
}

func TestValidateWriterReleaseProofV2BindsCurrentIdentity(t *testing.T) {
	proof := testWriterReleaseProofV2(35)
	current := sourceCandidateV2{Base: "access.log", Device: 1, Inode: 12, Current: true}
	if err := validateWriterReleaseProofV2(proof, current, 40); err != nil {
		t.Fatal(err)
	}
	proof.Current[0].Inode = 99
	if err := validateWriterReleaseProofV2(proof, current, 40); err == nil {
		t.Fatal("proof for a different current inode must be rejected")
	}
}

func TestValidateWriterReleaseProofV2RejectsDuplicateAndFutureIdentity(t *testing.T) {
	current := sourceCandidateV2{Base: "access.log", Device: 1, Inode: 12, Current: true}
	proof := testWriterReleaseProofV2(35)
	proof.Released[0].Inode = 12
	if err := validateWriterReleaseProofV2(proof, current, 40); err == nil {
		t.Fatal("current and released identity overlap must be rejected")
	}
	proof = testWriterReleaseProofV2(41)
	if err := validateWriterReleaseProofV2(proof, current, 40); err == nil {
		t.Fatal("release later than proof generation must be rejected")
	}
}

func TestApplyWriterReleaseProofV2RetiresOnlyMissingDrainedSource(t *testing.T) {
	state := testWriterReleaseStateV2()
	next, changed, err := applyWriterReleaseProofValueV2(state, testWriterReleaseProofV2(35))
	if err != nil || !changed || next.Files[0].State != "retired" {
		t.Fatalf("valid proof did not retire missing source: changed=%v err=%v state=%+v", changed, err, next.Files)
	}
	if state.Files[0].State != "missing" {
		t.Fatal("proof application mutated caller state")
	}
}

func TestApplyWriterReleaseProofV2RejectsStaleAndPresentSource(t *testing.T) {
	state := testWriterReleaseStateV2()
	next, changed, err := applyWriterReleaseProofValueV2(state, testWriterReleaseProofV2(25))
	if err != nil || changed || next.Files[0].State != "missing" {
		t.Fatalf("stale release proof changed state: changed=%v err=%v state=%+v", changed, err, next.Files)
	}
	state.Files[0].State, state.Files[0].MissingSince, state.Files[0].MissingScans = "quiescent", 0, 0
	next, changed, err = applyWriterReleaseProofValueV2(state, testWriterReleaseProofV2(35))
	if err != nil || changed || next.Files[0].State != "quiescent" {
		t.Fatalf("present source must not be retired: changed=%v err=%v state=%+v", changed, err, next.Files)
	}
}
