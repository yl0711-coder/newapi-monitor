package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestModelDataOfflineSnapshotDoesNotRequireProduction(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	for _, offline := range []bool{false, true} {
		m.cfg.LocalSnapshotOnly = offline
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/data?view=observed", nil)
		m.serveData(c)
		var result struct {
			Enabled  bool      `json:"enabled"`
			Snapshot *Snapshot `json:"snapshot"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if w.Code != http.StatusOK || result.Enabled != offline || (result.Snapshot != nil) != offline {
			t.Fatalf("offline=%v response=%s", offline, w.Body.String())
		}
		if offline && (result.Snapshot.SamplingActive || result.Snapshot.DataComplete) {
			t.Fatal("offline snapshot must not imply active or complete sampling")
		}
		if offline && !result.Snapshot.LocalSnapshotOnly {
			t.Fatal("offline snapshot must explicitly identify the preview mode")
		}
		if m.prodDB != nil || m.Enabled() {
			t.Fatal("reading a local snapshot must not enable the production source")
		}
	}
}

func TestSnapshotFinalizedWindowDoesNotDriftBetweenWorkerRuns(t *testing.T) {
	m := newTestMonitor(t)
	now := int64(1_800_000_000)
	end := metricFinalizeTarget(now)
	if err := m.storeDB.Create(&MetricFinalizeState{ID: 1, CoverageFromTs: end - 3600, NextTs: end, SemanticsVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	for _, offset := range []int64{0, 60, 600, 22 * 3600} {
		s, err := m.computeSnapshot(60, now+offset)
		if err != nil {
			t.Fatal(err)
		}
		if !s.DataComplete || s.WindowToTs != end || s.WindowFromTs != end-3600 || s.View != "finalized" {
			t.Fatalf("signed window drifted at offset %d: %+v", offset, s)
		}
		if s.Summary.WindowMinutes != s.WindowMinutes {
			t.Fatal("summary and snapshot must expose the same actual window")
		}
		if offset >= 600 && !s.FinalizationDelayed {
			t.Fatal("old signed result must be labelled delayed")
		}
	}
}

func TestSnapshotObservedViewUsesCurrentFactsWithoutClaimingCompleteness(t *testing.T) {
	m := newTestMonitor(t)
	now := int64(1_800_000_000)
	if err := m.storeDB.Create(&ChannelSnap{ID: 1, Status: 1, Models: "m", Groups: "g"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&MetricSample{BucketTs: now - 120, ChannelID: 1, ModelName: "m", Grp: "g", Success: 3, Tokens: 600}).Error; err != nil {
		t.Fatal(err)
	}
	observed, err := m.getSnapshotView(60, now, true)
	if err != nil {
		t.Fatal(err)
	}
	finalized, err := m.GetSnapshot(60, now)
	if err != nil {
		t.Fatal(err)
	}
	if observed == finalized || observed.View != "observed" || finalized.View != "finalized" || observed.DataComplete || observed.CompareAvailable {
		t.Fatal("views shared cache or observed data claimed finality")
	}
	if observed.Summary.Total != 3 || observed.WindowToTs != now || len(observed.ByModel) != 1 || len(finalized.ByModel) != 0 {
		t.Fatalf("observed facts hidden or leaked into finalized view: observed=%+v finalized=%+v", observed, finalized)
	}
}
