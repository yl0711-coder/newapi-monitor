package main

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReconstructFindsExactLegacyAccessBoundary(t *testing.T) {
	lines := []string{"a\n", "b\n", "c\n", "d\n", "e\n", "unacked\n"}
	data := strings.Join(lines, "")
	const inode = 91
	ids, target := fixtureBatchIDs("master", "access", inode, lines, []int{2, 1, 2})
	resume, matched, err := reconstruct(strings.NewReader(data), "master", "access", inode, 0, 1000, ids, target)
	if err != nil {
		t.Fatal(err)
	}
	want := int64(len(strings.Join(lines[:5], "")))
	if resume != want || matched != 3 {
		t.Fatalf("resume=%d matched=%d want=%d/3", resume, matched, want)
	}
}

func TestReconstructPreservesErrorBatchIdentity(t *testing.T) {
	lines := []string{"first\n", "second\n", "tail\n"}
	const inode = 77
	ids, target := fixtureBatchIDs("slave", "error", inode, lines, []int{1, 1})
	resume, matched, err := reconstruct(strings.NewReader(strings.Join(lines, "")), "slave", "error", inode, 0, 2, ids, target)
	if err != nil || resume != int64(len(lines[0])+len(lines[1])) || matched != 2 {
		t.Fatalf("resume=%d matched=%d err=%v", resume, matched, err)
	}
}

func TestReconstructFailsClosedOnMissingIntermediateBatch(t *testing.T) {
	lines := []string{"a\n", "b\n", "c\n"}
	const inode = 12
	ids, target := fixtureBatchIDs("master", "access", inode, lines, []int{1, 1, 1})
	delete(ids, firstID("master", "access", inode, lines[:1], 0))
	if _, _, err := reconstruct(strings.NewReader(strings.Join(lines, "")), "master", "access", inode, 0, 1000, ids, target); err == nil {
		t.Fatal("a missing predecessor batch must fail closed")
	}
}

func TestRunRejectsMutableIdentityInputs(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "access.log.1")
	if err := os.WriteFile(logPath, []byte("line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(dir, "linked.log")
	if err := os.Symlink(logPath, linkPath); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := openImmutableLog(linkPath); err == nil {
		t.Fatal("symlink input must be rejected")
	}
}

func TestVerifyUnchangedLogRejectsGrowth(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log.1")
	if err := os.WriteFile(path, []byte("first\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, info, stat, err := openImmutableLog(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	appendFile, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = appendFile.WriteString("second\n")
	_ = appendFile.Close()
	if err := verifyUnchangedLog(path, info, stat); err == nil {
		t.Fatal("a growing log must not produce a recovery offset")
	}
}

func fixtureBatchIDs(node, kind string, inode uint64, lines []string, groups []int) (map[string]bool, string) {
	ids := make(map[string]bool)
	start, index, target := int64(0), 0, ""
	for _, count := range groups {
		content := sha256.New()
		end := start
		for _, line := range lines[index : index+count] {
			digest := sha256.Sum256([]byte(line))
			_, _ = content.Write(digest[:])
			end += int64(len(line))
		}
		target = legacyBatchID(node, kind, inode, start, end, content.Sum(nil))
		ids[target] = true
		start, index = end, index+count
	}
	return ids, target
}

func firstID(node, kind string, inode uint64, lines []string, start int64) string {
	content := sha256.New()
	end := start
	for _, line := range lines {
		digest := sha256.Sum256([]byte(line))
		_, _ = content.Write(digest[:])
		end += int64(len(line))
	}
	return legacyBatchID(node, kind, inode, start, end, content.Sum(nil))
}
