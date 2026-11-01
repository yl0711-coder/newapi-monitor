package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// Explicit retained-source acceptance, not a collector or customer approval.
// No credentials are loaded and all original files must remain unchanged.
func TestTokenAssemblyRetainedRealMonthReadOnly(t *testing.T) {
	root, usage := os.Getenv("MONTHLY_TOKEN_RETAINED_MONTH"), os.Getenv("MONTHLY_CAPTURE_RETAINED_MONTH")
	if root == "" || usage == "" {
		t.Skip("explicit private token and independent user observations required")
	}
	for _, dir := range []string{root, usage} {
		st, err := os.Lstat(dir)
		canonical, canonicalErr := filepath.EvalSymlinks(dir)
		if err != nil || canonicalErr != nil || !st.IsDir() || st.Mode().Perm() != 0700 || canonical != dir || !filepath.IsAbs(dir) {
			t.Fatal("private canonical source directory required")
		}
	}
	var identity struct {
		SourceIdentity string `json:"source_identity"`
		UserID         int64  `json:"user_id"`
	}
	read := func(path string) []byte {
		t.Helper()
		b, err := privateRead(path, 2<<20)
		if err != nil {
			t.Fatal("private source read failed", err)
		}
		return b
	}
	if json.Unmarshal(read(filepath.Join(root, "source-identity.json")), &identity) != nil || !validSHA(identity.SourceIdentity) || identity.UserID != 148 {
		t.Fatal("unexpected explicit acceptance source")
	}
	p := tokenMonthAssemblyPlan{Version: 1, SourceIdentity: identity.SourceIdentity, Rules: monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}, UserID: identity.UserID, Month: "2026-09"}
	before := map[string]string{}
	var records int64
	for day := 1; day <= 30; day++ {
		var pair assemblyUsagePair
		for phase, target := range map[string]*assemblyCapture{"a": &pair.Older, "b": &pair.Newer} {
			name := fmt.Sprintf("sep-d%02d-%s", day, phase)
			dir := filepath.Join(root, name)
			// Completion markers have the known pending/final two-name form;
			// readAssemblyCapture independently checks both inode identities.
			markerPath := filepath.Join(dir, "manifest.json")
			marker, err := os.ReadFile(markerPath)
			if err != nil {
				t.Fatal("source completion marker missing")
			}
			*target = assemblyCapture{Directory: dir, ManifestSHA256: hash(marker)}
			payload, err := readAssemblyCapture(*target, "observe-token-usage")
			if err != nil {
				t.Fatal(err)
			}
			var token monthlysource.TokenUsageWindowObservation
			if json.Unmarshal(payload, &token) != nil {
				t.Fatal("token observation invalid")
			}
			userDir := filepath.Join(usage, name)
			userMarker, err := os.ReadFile(filepath.Join(userDir, "manifest.json"))
			if err != nil {
				t.Fatal("independent user observation marker missing")
			}
			userRaw, err := readAssemblyCapture(assemblyCapture{Directory: userDir, ManifestSHA256: hash(userMarker)}, "observe-usage")
			if err != nil {
				t.Fatal(err)
			}
			var user monthlysource.UsageWindowObservation
			if json.Unmarshal(userRaw, &user) != nil || token.Request != user.Request || !reflect.DeepEqual(token.Totals, user.Rows) {
				t.Fatal("token totals differ from independently retained original user window", day, phase)
			}
			if phase == "a" {
				for _, r := range token.Totals {
					records += r.Records
				}
			}
			for file, value := range map[string][]byte{markerPath: marker, filepath.Join(dir, "observation.json"): payload, filepath.Join(userDir, "manifest.json"): userMarker, filepath.Join(userDir, "observation.json"): userRaw} {
				before[file] = hash(value)
			}
		}
		p.Windows = append(p.Windows, pair)
	}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	preflight, err := assembleTokenMonth(context.Background(), workspace, "new-local-only", p, raw, false)
	if err != nil {
		t.Fatal(err)
	}
	created, err := assembleTokenMonth(context.Background(), workspace, "new-local-only", p, raw, true)
	if err != nil || created.ReportSHA256 != preflight.ReportSHA256 || created.ArchiveSHA256 != preflight.ArchiveSHA256 {
		t.Fatal("offline preflight and archive differ", err)
	}
	archive := read(filepath.Join(workspace, "new-local-only", "token-evidence.json"))
	_, report, err := monthlysource.DecodeTokenUsageMonthArchive(context.Background(), archive, time.Now())
	if err != nil || records != 221 || report.ConsumeQuota != 7480870 || report.RefundQuota != 0 || len(report.Rows) != 2 {
		t.Fatal("retained real sample month does not reconcile", records, err)
	}
	for _, r := range report.Rows {
		want := map[int64]int64{348: 28375, 391: 7452495}
		q, ok := want[r.TokenID]
		if !ok || r.ConsumeQuota != q || r.RefundQuota != 0 {
			t.Fatal("wrong real token attribution")
		}
	}
	for file, expected := range before {
		b, err := os.ReadFile(file)
		if err != nil || hash(b) != expected {
			t.Fatal("retained original changed")
		}
	}
	t.Log("user 148 September: 221 logs, consume 7480870 quota, token 348=28375 / 391=7452495; double source and independent user windows matched, originals unchanged; not bill approval")
}
