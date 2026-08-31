//go:build linux

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFDVerificationRejectsLateRotatedWriter(t *testing.T) {
	dir := t.TempDir()
	currentPath := filepath.Join(dir, "access.log")
	rotatedPath := filepath.Join(dir, "access.log.1")
	if err := os.WriteFile(currentPath, []byte("current\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rotatedPath, []byte("rotated\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	currentFD, err := os.OpenFile(currentPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer currentFD.Close()
	rotatedFD, err := os.OpenFile(rotatedPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	files, err := inspectLogFiles(dir, []string{"access.log"})
	if err != nil {
		t.Fatal(err)
	}
	process, err := processIdentity(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyWorkerFDs([]workerProcess{process}, files, []string{"access.log"}); err == nil {
		t.Fatal("a master-like writer retaining the rotated log must fail verification")
	}
	if err := verifyNoRotatedLogFDs([]int{os.Getpid()}, []string{"access.log"}); err == nil {
		t.Fatal("container-wide scan missed the rotated log descriptor")
	}
	if err := rotatedFD.Close(); err != nil {
		t.Fatal(err)
	}
	if err := verifyWorkerFDs([]workerProcess{process}, files, []string{"access.log"}); err != nil {
		t.Fatalf("current writable append descriptor was rejected: %v", err)
	}
	if err := verifyNoRotatedLogFDs([]int{os.Getpid()}, []string{"access.log"}); err != nil {
		t.Fatalf("closed rotated descriptor remained visible: %v", err)
	}
}

func TestNginxMasterProcessRejectsNonNginxPID1Candidate(t *testing.T) {
	if _, err := nginxMasterProcess(os.Getpid()); err == nil {
		t.Fatal("non-nginx command line must not be accepted as the master process")
	}
}
