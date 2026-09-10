package ecslogagent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDeferredArchiveReceiptSafetyAndProgress(t *testing.T) {
	c, meta := testConfig(t)
	c.DeferredArchiveACK = true
	a := testAgent(t, c, meta, roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("deferred transport must not claim live ingestion")
		return response(503, `{}`), nil
	}))
	body := []byte(`{"node":"` + a.node + `","batch_id":"first"}`)
	send := func(capability bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", c.MonitorURL+"/internal/rejections/v2", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+a.token)
		if capability {
			r.Header.Set("X-Monitor-Archive-Ack", "1")
		}
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		return w
	}
	if send(true).Code != 503 {
		t.Fatal("missing archive accepted")
	}
	s := &memoryArchive{objects: map[string][]byte{}, fail: true}
	if err := a.ConfigureArchive(s, "isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("a", 32)); err != nil {
		t.Fatal(err)
	}
	a.leases["reject"] = time.Now().Add(time.Minute).Unix()
	if send(false).Code != 503 || send(true).Code != 503 {
		t.Fatal("unsupported client or failed storage accepted")
	}
	s.fail = false
	a.leases["reject"] = time.Now().Add(10 * time.Second).Unix()
	if send(true).Code != 503 || len(s.objects) != 0 {
		t.Fatal("insufficient authorization window accepted")
	}
	a.leases["reject"] = time.Now().Add(time.Minute).Unix()
	for i := 0; i < 2; i++ {
		w := send(true)
		var ack map[string]any
		if w.Code != 202 || json.Unmarshal(w.Body.Bytes(), &ack) != nil {
			t.Fatalf("receipt: %d %s", w.Code, w.Body.String())
		}
		sum := sha256.Sum256(body)
		if ack["version"] != float64(1) || ack["state"] != "durably_archived" || ack["node"] != a.node || ack["lane"] != "reject" || ack["batch_id"] != "first" || ack["body_sha256"] != hex.EncodeToString(sum[:]) || ack["ok"] != nil || ack["accepted"] != nil {
			t.Fatalf("invalid durability contract: %v", ack)
		}
	}
	if len(s.objects) != 1 {
		t.Fatal("retry created duplicate archive")
	}
	body = []byte(`{"node":"` + a.node + `","batch_id":"second"}`)
	if send(true).Code != 202 || len(s.objects) != 2 {
		t.Fatal("offline receiver blocked next batch")
	}
}
