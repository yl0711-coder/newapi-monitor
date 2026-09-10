package monitor

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func isolationLegacyRouter(m *Monitor) *gin.Engine {
	r := gin.New()
	r.POST("/access", m.ingestNginx)
	r.POST("/error", m.ingestNginxErrors)
	r.POST("/evidence", m.ingestNginxEvidence)
	r.POST("/reject", m.ingestRejectionsV2)
	r.POST("/reject-v1", m.ingestRejections)
	return r
}

func isolationLegacyPost(m *Monitor, lane string, body []byte) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/"+lane, bytes.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+m.cfg.IngestToken)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	isolationLegacyRouter(m).ServeHTTP(w, r)
	return w
}

func TestECSLogIsolatedReceiverRejectsLegacyEvenWithValidToken(t *testing.T) {
	m := newECSLogTestMonitor(t)
	m.cfg.NginxAllowedNodes = []string{"master"}
	for _, lane := range []string{"access", "error", "evidence", "reject", "reject-v1"} {
		body := []byte(`{}`)
		if lane != "reject-v1" {
			body = ecsLaneTestBody(t, "master", lane, 1)
		}
		if w := isolationLegacyPost(m, lane, body); w.Code != http.StatusForbidden {
			t.Fatalf("legacy %s reached isolated store: %d %s", lane, w.Code, w.Body.String())
		}
	}
}

func TestECSLogIndependentReceiverDoesNotChangePrimaryFacts(t *testing.T) {
	// Both stores are synthetic and local. Primary uses the unchanged legacy
	// path; candidate uses task authentication and a separate evidence DB.
	primary := newTestMonitor(t)
	t.Cleanup(primary.Close)
	enableTestNginxEvidence(t, primary)
	primary.cfg.NginxErrorEnabled = true
	primary.cfg.NginxAllowedNodes = []string{"master"}
	for _, lane := range []string{"access", "error", "evidence", "reject"} {
		if w := isolationLegacyPost(primary, lane, ecsLaneTestBody(t, "master", lane, 7)); w.Code != 200 {
			t.Fatalf("primary %s: %d %s", lane, w.Code, w.Body.String())
		}
	}
	fact := UsageHourFact{HourTs: time.Now().Unix() / 3600 * 3600, UserID: 7, ChannelID: 11, Grp: "fixture", ModelName: "fixture", TokenID: 13, Requests: 7, ConsumeQuota: 1234567}
	if err := primary.storeDB.Create(&fact).Error; err != nil {
		t.Fatal(err)
	}
	candidate := newECSLogTestMonitor(t)
	candidate.cfg.ECSLogAudience = "fixture-shadow-candidate"
	candidate.cfg.ECSArchiveEnabled = true
	r := newECSArchiveReplayer(candidate, "isolated/")
	for _, lane := range []string{"access", "error", "evidence", "reject"} {
		source, key := registerECSTestSource(t, candidate, strings.Repeat("a", 32), lane)
		body := ecsLaneTestBody(t, source.Node, lane, 3)
		for attempt := 0; attempt < 2; attempt++ {
			if w := postSignedECS(candidate, source, key, body, nil); w.Code != 200 {
				t.Fatalf("candidate %s: %d", lane, w.Code)
			}
		}
		envelope, err := ecsarchive.Seal(candidate.cfg.ECSLogAudience, source.Node, lane, body, key)
		if err != nil {
			t.Fatal(err)
		}
		objectKey, err := ecsarchive.ObjectKey("isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("a", 32), envelope)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		object := ecsarchive.Object{Key: objectKey, Body: raw, ETag: "fixture", Modified: time.Now()}
		for attempt := 0; attempt < 2; attempt++ {
			if got := r.replay(context.Background(), object, time.Now()); !got.Accepted || !got.Duplicate {
				t.Fatalf("live/archive duplicate %s: %+v", lane, got)
			}
		}
		if got := newECSArchiveReplayer(primary, "isolated/").replay(context.Background(), object, time.Now()); got.Accepted {
			t.Fatal("candidate archive entered primary")
		}
		if w := postSignedECS(primary, source, key, body, nil); w.Code != 503 {
			t.Fatalf("primary ECS gate opened: %d", w.Code)
		}
	}
	assertIsolationCounts(t, primary, 7)
	assertIsolationCounts(t, candidate, 3)
	var got UsageHourFact
	if err := primary.storeDB.First(&got).Error; err != nil || got != fact {
		t.Fatalf("primary financial fact changed: %+v %v", got, err)
	}
	var count int64
	if err := candidate.storeDB.Model(&UsageHourFact{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("log pipeline invented usage/financial facts", count, err)
	}
}

func assertIsolationCounts(t *testing.T, m *Monitor, want int64) {
	t.Helper()
	for _, model := range []any{&NginxMinuteSample{}, &NginxErrorMinuteSample{}, &RejectionSample{}} {
		var count int64
		if err := m.storeDB.Model(model).Select("COALESCE(SUM(count),0)").Scan(&count).Error; err != nil || count != want {
			t.Fatalf("%T: %d want %d, %v", model, count, want, err)
		}
	}
	var count int64
	if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("evidence duplicated: %d %v", count, err)
	}
}

func TestECSLogAudienceBlocksCrossReceiverEvenWithSameSourceKey(t *testing.T) {
	a, b := newECSLogTestMonitor(t), newECSLogTestMonitor(t)
	a.cfg.ECSLogAudience, b.cfg.ECSLogAudience = "fixture-audience-A", "fixture-audience-B"
	pub, key := ecsTestKey(t)
	for _, lane := range []string{"access", "error", "evidence", "reject"} {
		in := testECSRegistration(strings.Repeat("c", 32), pub)
		in.Lane = lane
		for _, m := range []*Monitor{a, b} {
			if w := postECSRegistration(t, m, in, m.cfg.ECSLogBridgeToken); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
		}
		var source ECSLogSource
		if err := a.storeDB.First(&source, "lane = ?", lane).Error; err != nil {
			t.Fatal(err)
		}
		body := ecsLaneTestBody(t, source.Node, lane, 1)
		if w := postSignedECS(b, source, key, body, func(req *http.Request) {
			at := strconv.FormatInt(time.Now().Unix(), 10)
			req.Header.Set("X-Monitor-ECS-Time", at)
			req.Header.Set("X-Monitor-ECS-Signature", base64.StdEncoding.EncodeToString(ed25519.Sign(key, ecsLogSigningMessage(a.cfg.ECSLogAudience, source.Node, lane, req.URL.Path, at, body))))
		}); w.Code != 401 {
			t.Fatalf("cross-audience %s accepted: %d", lane, w.Code)
		}
		e, err := ecsarchive.Seal(a.cfg.ECSLogAudience, source.Node, lane, body, key)
		if err != nil {
			t.Fatal(err)
		}
		objectKey, err := ecsarchive.ObjectKey("isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("c", 32), e)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		o := ecsarchive.Object{Key: objectKey, Body: raw, ETag: "fixture", Modified: time.Now()}
		if got := newECSArchiveReplayer(b, "isolated/").replay(context.Background(), o, time.Now()); got.Status != 403 {
			t.Fatalf("cross-audience archive %+v", got)
		}
	}
}
