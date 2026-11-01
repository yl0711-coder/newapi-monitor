package monthlydraft

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestCapturedCapacityRefusesBeforeExpensiveAssembly(t *testing.T) {
	for _, kind := range []string{"bytes", "jobs"} {
		t.Run(kind, func(t *testing.T) {
			_, _, p, req, _ := capturedFilesFixture(t)
			s := capturedSpoolTest(t, p)
			if kind == "bytes" {
				s.limit = 4 << 20
			} else {
				for i := 0; i < capturedSpoolJobs; i++ {
					if err := os.Mkdir(filepath.Join(s.directory, "job-"+generationHash([]byte(stringID(int64(i))))), 0700); err != nil {
						t.Fatal(err)
					}
				}
			}
			// Closing the synthetic capture handle proves rejection precedes any
			// expensive raw/projection assembly. No real files or sources involved.
			if err := p.funding.capture.Close(); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadDir(s.directory)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.Advance(t.Context(), req); !errors.Is(err, ErrGenerationCapacity) {
				t.Fatal("capacity check read capture DB first", err)
			}
			after, err := os.ReadDir(s.directory)
			if err != nil || len(after) != len(before) {
				t.Fatal("preflight allocated or deleted files", err)
			}
		})
	}
}

func TestCapturedCapacityDoesNotSkipAuthorizationOrOriginalVerification(t *testing.T) {
	for _, kind := range []string{"unauthorized", "existing"} {
		t.Run(kind, func(t *testing.T) {
			_, auth, p, req, _ := capturedFilesFixture(t)
			s := capturedSpoolTest(t, p)
			if kind == "unauthorized" {
				s.limit = 4 << 20
				if err := auth.Model(&monitor.CustomerGroup{}).Where("id=?", req.Owner.GroupID).Update("portal_auth_ver", req.Owner.AuthVersion+1).Error; err != nil {
					t.Fatal(err)
				}
				if _, _, err := s.Advance(t.Context(), req); err == nil || errors.Is(err, ErrGenerationCapacity) {
					t.Fatal("capacity hid revoked authorization", err)
				}
				return
			}
			if _, _, err := s.Advance(t.Context(), req); err != nil {
				t.Fatal(err)
			}
			s.limit = 1 // Existing complete job can reuse, but not bypass original audit.
			if err := p.funding.capture.Close(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := s.Advance(t.Context(), req); err == nil {
				t.Fatal("cached complete marker substituted for source verification")
			}
		})
	}
}
