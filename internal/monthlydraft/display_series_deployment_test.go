package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func displaySeriesDeploymentFixture(t *testing.T) (displaySeriesFixture, DeploymentConfig, string) {
	t.Helper()
	f := newDisplaySeriesFixture(t)
	root := f.g.root.Name()
	if err := os.Chmod(filepath.Join(root, "receiver.db"), 0600); err != nil {
		t.Fatal(err)
	}
	index := "display-series.json"
	raw, err := json.Marshal(AutomaticDisplaySeriesIndex{Version: 1, Ranges: f.ranges})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, index), raw, 0600); err != nil {
		t.Fatal(err)
	}
	// Predecessor preparation is NOT permission to publish those months.
	return f, DeploymentConfig{Rollout: GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: "2026-09"}}}, ReceiverPath: filepath.Join(root, "receiver.db"), FundingPath: f.fundingPath, AuthorityPath: f.authorityPath, EvidenceRoot: root}, index
}

func TestAutomaticDisplaySeriesDeploymentOptInActualSubmitPublishStopRestart(t *testing.T) {
	f, cfg, index := displaySeriesDeploymentFixture(t)
	open := func() *Deployment {
		d, err := OpenRetainedAutomaticDisplaySeriesDeployment(context.Background(), cfg, "scope", index, displaySeriesPublisher(f.auth))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { closeDeploymentTest(t, d) })
		d.runtime.worker.pollMin = 5 * time.Millisecond
		d.runtime.worker.pollMax = 20 * time.Millisecond
		return d
	}
	d := open()
	if d.runtime.Phase() != "disabled" {
		t.Fatal("constructor started work")
	}
	files, err := filepath.Glob(filepath.Join(f.g.root.Name(), "display-source-*.json"))
	if err != nil || len(files) != 0 {
		t.Fatal("constructor prepared money before explicit start", err)
	}
	if err := d.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if view, err := d.SubmitMonthly(context.Background(), 1, 1, "2026-09"); err != nil || view.Month != "2026-09" {
		t.Fatal("actual runtime could not submit", view, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var count int64
		if err := f.auth.Model(&monitor.PortalMonthlyBill{}).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count == 1 && d.runtime.worker.Status().Published == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("runtime did not publish", d.runtime.worker.Status())
		}
		time.Sleep(5 * time.Millisecond)
	}
	var record monitor.PortalMonthlyBill
	if err := f.auth.First(&record, "group_id=1 AND month='2026-09'").Error; err != nil {
		t.Fatal(err)
	}
	// The internal workflow API deliberately does not claim availability:
	// Monitor's customer route checks the immutable publication first. A saved
	// journal reached directly through this internal API reports needs_review.
	if view, err := d.ReadMonthly(context.Background(), 1, 1, "2026-09"); err != nil || view.State != "needs_review" {
		t.Fatal("internal workflow contract changed", view, err)
	}
	if _, err := d.SubmitMonthly(context.Background(), 1, 1, "2026-07"); err == nil {
		t.Fatal("preparation range silently expanded customer rollout")
	}
	stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := d.Close(stop); err != nil {
		t.Fatal(err)
	}
	cancel()
	frozen := map[string][]byte{}
	files, err = filepath.Glob(filepath.Join(f.g.root.Name(), "display-source-*.json"))
	if err != nil || len(files) != 3 {
		t.Fatal("whole fixed range was not retained", err)
	}
	for _, name := range files {
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		frozen[name] = b
	}
	restarted := open()
	if err := restarted.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if view, err := restarted.ReadMonthly(context.Background(), 1, 1, "2026-09"); err != nil || view.State != "needs_review" {
		t.Fatal("restart lost issued publication", view, err)
	}
	stop, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	if err := restarted.Close(stop); err != nil {
		t.Fatal(err)
	}
	cancel()
	var after monitor.PortalMonthlyBill
	if err := f.auth.First(&after, "group_id=1 AND month='2026-09'").Error; err != nil || after != record {
		t.Fatal("service restart changed customer bill", err)
	}
	for name, b := range frozen {
		after, err := os.ReadFile(name)
		if err != nil || !bytes.Equal(b, after) {
			t.Fatal("service restart changed retained source", err)
		}
	}
}

func TestAutomaticDisplaySeriesDeploymentRejectsUnsafeOrMismatchedIndex(t *testing.T) {
	f, cfg, index := displaySeriesDeploymentFixture(t)
	path := filepath.Join(f.g.root.Name(), index)
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"missing", "unknown_field", "permissions", "symlink", "wrong_org", "unsupported_month", "duplicate"} {
		t.Run(kind, func(t *testing.T) {
			if err := os.WriteFile(path, original, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0600); err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = os.Remove(path)
				if err := os.WriteFile(path, original, 0600); err != nil {
					t.Error(err)
				}
			}()
			var doc AutomaticDisplaySeriesIndex
			if err := json.Unmarshal(original, &doc); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "unknown_field":
				if err := os.WriteFile(path, []byte(`{"version":1,"ranges":[],"customer_from":"2026-09"}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "permissions":
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				copy := filepath.Join(f.g.root.Name(), "index-backup.json")
				if err := os.WriteFile(copy, original, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(copy, path); err != nil {
					t.Fatal(err)
				}
			case "wrong_org":
				doc.Ranges[0].GroupID = 2
			case "unsupported_month":
				doc.Ranges[0].Through = "2026-08"
			case "duplicate":
				doc.Ranges = append(doc.Ranges, doc.Ranges[0])
			}
			if kind == "wrong_org" || kind == "unsupported_month" || kind == "duplicate" {
				raw, _ := json.Marshal(doc)
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if d, err := OpenRetainedAutomaticDisplaySeriesDeployment(context.Background(), cfg, "scope", index, displaySeriesPublisher(f.auth)); err == nil || d != nil {
				if d != nil {
					closeDeploymentTest(t, d)
				}
				t.Fatal("unsafe range index accepted", err)
			}
			displaySeriesCount(t, f, 0)
		})
	}
	// Failed opens release their resources and do not poison a corrected start.
	d, err := OpenRetainedAutomaticDisplaySeriesDeployment(context.Background(), cfg, "scope", index, displaySeriesPublisher(f.auth))
	if err != nil {
		t.Fatal("failed-open cleanup leaked the writer lock", err)
	}
	closeDeploymentTest(t, d)
}
