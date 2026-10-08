package financegiftexport

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLargeHourPlanFilesAreBoundedAndNoClobber(t *testing.T) {
	plan, _ := largeExportFixture(3001)
	path := filepath.Join(t.TempDir(), "plan.json")
	digest, err := WriteLargeHourPlan(path, plan)
	if err != nil {
		t.Fatal(err)
	}
	got, repeated, err := ReadLargeHourPlan(path)
	if err != nil || repeated != digest || len(got.Rows) != 3001 {
		t.Fatal("plan changed", err)
	}
	if _, err := WriteLargeHourPlan(path, plan); err == nil {
		t.Fatal("overwrote plan")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("plan permissions", err)
	}
	for _, content := range []string{"{} {}", strings.Repeat(" ", giftScopeExportBytes+1), `{"unknown":1}`} {
		bad := filepath.Join(t.TempDir(), "bad.json")
		if err := os.WriteFile(bad, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := ReadLargeHourPlan(bad); err == nil {
			t.Fatal("invalid plan file accepted")
		}
	}
	link := filepath.Join(t.TempDir(), "link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadLargeHourPlan(link); err == nil {
		t.Fatal("linked plan accepted")
	}
}
