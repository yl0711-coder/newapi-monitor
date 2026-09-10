package ecsarchive

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestNewAPIFileContractValidation(t *testing.T) {
	file := func(name string, inode uint64) FinalFile {
		return FinalFile{Name: name, Inode: inode, Size: 8, Offset: 8, SHA256: strings.Repeat("a", 64)}
	}
	for _, tc := range []struct {
		name, contract, lane string
		files                []FinalFile
		ok                   bool
	}{
		{"legacy", "", "reject", []FinalFile{file("new-api.log", 1)}, true},
		{"access", NewAPIFileContract, "access", []FinalFile{file("nexusapi_access.jsonl", 1)}, true},
		{"evidence", NewAPIFileContract, "evidence", []FinalFile{file("nexusapi_access.jsonl", 1)}, true},
		{"error", NewAPIFileContract, "error", []FinalFile{file("error.log", 1)}, true},
		{"multi", NewAPIFileContract, "reject", []FinalFile{file("oneapi-20000101.log", 1), file("oneapi-20000102.log", 2)}, true},
		{"missing", NewAPIFileContract, "reject", nil, false},
		{"wrong-contract", "unknown", "reject", []FinalFile{file("oneapi-20000102.log", 1)}, false},
		{"no-downgrade", "", "reject", []FinalFile{file("oneapi-20000102.log", 1)}, false},
		{"wrong-lane", NewAPIFileContract, "access", []FinalFile{file("error.log", 1)}, false},
		{"path", NewAPIFileContract, "reject", []FinalFile{file("../oneapi-1.log", 1)}, false},
		{"rotation", NewAPIFileContract, "access", []FinalFile{file("nexusapi_access.jsonl.1", 1)}, false},
		{"alias", NewAPIFileContract, "reject", []FinalFile{file("oneapi-1.log", 1), file("oneapi-2.log", 1)}, false},
		{"same-name", NewAPIFileContract, "reject", []FinalFile{file("oneapi-1.log", 1), file("oneapi-1.log", 2)}, false},
		{"unsorted", NewAPIFileContract, "reject", []FinalFile{file("oneapi-2.log", 2), file("oneapi-1.log", 1)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &FinalBoundary{ObservedAt: 1, Files: tc.files, Contract: tc.contract}
			_, err := ReadClosure(BoundaryBody("fixture", b), "fixture", tc.lane)
			if (err == nil) != tc.ok {
				t.Fatalf("validation: %v", err)
			}
		})
	}
	var files []FinalFile
	for i := 0; i <= FinalMaxFiles; i++ {
		files = append(files, file(fmt.Sprintf("oneapi-%03d.log", i), uint64(i+1)))
	}
	if ValidateFinalFiles(NewAPIFileContract, "reject", files[:FinalMaxFiles]) != nil {
		t.Fatal("maximum bounded count rejected")
	}
	if ValidateFinalFiles(NewAPIFileContract, "reject", files) == nil {
		t.Fatal("unbounded file count accepted")
	}
	files = files[:2]
	for i := range files {
		files[i].Size = FinalMaxBytes
		files[i].Offset = FinalMaxBytes
	}
	if ValidateFinalFiles(NewAPIFileContract, "reject", files) == nil {
		t.Fatal("combined budget ignored")
	}
}

func TestLegacyBoundaryBytesUnchanged(t *testing.T) {
	b := &FinalBoundary{ObservedAt: 1, Files: []FinalFile{{Name: "new-api.log", Inode: 1, SHA256: strings.Repeat("a", 64)}}}
	want := `{"node":"fixture","batch_id":"collector-closure-v2","boundary":{"observed_at":1,"files":[{"name":"new-api.log","device":0,"inode":1,"size":0,"offset":0,"sha256":"` + strings.Repeat("a", 64) + `"}]}}`
	if !bytes.Equal(BoundaryBody("fixture", b), []byte(want)) {
		t.Fatal("legacy signed bytes changed")
	}
}
