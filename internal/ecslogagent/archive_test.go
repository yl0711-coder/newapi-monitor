package ecslogagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

type memoryArchive struct {
	objects map[string][]byte
	fail    bool
	lostACK bool
}

func (s *memoryArchive) Binding() string { return "fixture-archive" }
func (s *memoryArchive) Put(_ context.Context, key string, body []byte) error {
	if s.fail {
		return errors.New("storage offline")
	}
	if _, ok := s.objects[key]; ok {
		return ecsarchive.ErrExists
	}
	s.objects[key] = append([]byte(nil), body...)
	if s.lostACK {
		return errors.New("S3 ACK lost")
	}
	return nil
}
func (s *memoryArchive) Get(_ context.Context, key string) (ecsarchive.Object, error) {
	return ecsarchive.Object{Key: key, Body: s.objects[key]}, nil
}

func TestArchiveFrozenRetryRestartAndFailure(t *testing.T) {
	c, meta := testConfig(t)
	calls := 0
	a := testAgent(t, c, meta, roundTripFunc(func(*http.Request) (*http.Response, error) { calls++; return response(200, `{"ok":true}`), nil }))
	store := &memoryArchive{objects: map[string][]byte{}, fail: true}
	owner := "AROAABCDEFGHIJKLMNOPQ:" + strings.Repeat("a", 32)
	if err := a.ConfigureArchive(store, "isolated/", owner); err != nil {
		t.Fatal(err)
	}
	a.leases["reject"] = time.Now().Unix() + 600
	body := []byte(`{"node":"` + a.node + `","batch_id":"fixture-archive-first"}`)
	send := func() int {
		req := httptest.NewRequest("POST", c.MonitorURL+"/internal/rejections/v2", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+a.token)
		w := httptest.NewRecorder()
		a.ServeHTTP(w, req)
		return w.Code
	}
	if send() != 503 || calls != 0 {
		t.Fatal("live send occurred before external archive confirmation")
	}
	store.fail = false
	store.lostACK = true
	if send() != 503 || calls != 0 {
		t.Fatal("unconfirmed archive ACK forwarded")
	}
	store.lostACK = false
	if send() != 200 || calls != 1 || len(store.objects) != 1 {
		t.Fatal("immutable retry did not recover lost archive ACK")
	}
	var original []byte
	for _, raw := range store.objects {
		original = append([]byte(nil), raw...)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a = testAgent(t, c, meta, roundTripFunc(func(*http.Request) (*http.Response, error) { calls++; return response(200, `{"ok":true}`), nil }))
	if err := a.ConfigureArchive(store, "isolated/", owner); err != nil {
		t.Fatal(err)
	}
	a.leases["reject"] = time.Now().Unix() + 600
	if send() != 200 || len(store.objects) != 1 {
		t.Fatal("restart regenerated archive identity")
	}
	for _, raw := range store.objects {
		if !bytes.Equal(original, raw) {
			t.Fatal("restart changed frozen envelope")
		}
	}
	var first ecsarchive.Envelope
	if err := json.Unmarshal(original, &first); err != nil {
		t.Fatal(err)
	}
	body = []byte(`{"node":"` + a.node + `","batch_id":"fixture-archive-second"}`)
	if send() != 200 || len(store.objects) != 2 {
		t.Fatal("second archive missing")
	}
	for _, raw := range store.objects {
		e, err := ecsarchive.Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		if e.Hash != first.Hash && e.Previous != first.Hash {
			t.Fatal("chain skipped previous batch")
		}
	}
	if err := a.ConfigureArchive(store, "different/", owner); err == nil {
		t.Fatal("archive destination silently changed")
	}
	if err := os.Remove(a.archiveJournalPath()); err != nil {
		t.Fatal(err)
	}
	if err := a.ConfigureArchive(store, "isolated/", owner); err == nil {
		t.Fatal("missing archive journal silently reset")
	}
}
