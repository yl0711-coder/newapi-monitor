package ecslogagent

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func TestClosureLostACKRetryAndSealedJournal(t *testing.T) {
	c, meta := testConfig(t)
	c.DeferredArchiveACK, c.ArchiveClosure = true, true
	a := testAgent(t, c, meta, roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") }))
	store := &memoryArchive{objects: map[string][]byte{}}
	owner := "AROAABCDEFGHIJKLMNOPQ:" + strings.Repeat("a", 32)
	if err := a.ConfigureArchive(store, "isolated/", owner); err != nil {
		t.Fatal(err)
	}
	a.leases["reject"] = time.Now().Unix() + 600
	body := []byte(`{"node":"` + a.node + `","batch_id":"first-batch"}`)
	if err := a.archiveFrozen(context.Background(), "reject", body); err != nil {
		t.Fatal(err)
	}
	store.lostACK = true
	if a.closeArchive(context.Background()) == nil {
		t.Fatal("lost closure ACK hidden")
	}
	var sealedKey string
	var sealed []byte
	for k, raw := range store.objects {
		e, _ := ecsarchive.Decode(raw)
		if e.Kind == ecsarchive.CollectorClosure {
			sealedKey, sealed = k, append([]byte(nil), raw...)
		}
	}
	if sealedKey == "" {
		t.Fatal("closure not preserved")
	}
	store.lostACK = false
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	a = testAgent(t, c, meta, roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") }))
	if err := a.ConfigureArchive(store, "isolated/", owner); err != nil {
		t.Fatal(err)
	}
	a.leases["reject"] = time.Now().Unix() + 600
	if err := a.closeArchive(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.objects) != 2 || !bytes.Equal(sealed, store.objects[sealedKey]) {
		t.Fatal("closure retry changed identity")
	}
	if err := a.archiveFrozen(context.Background(), "reject", body); err != nil {
		t.Fatal("old retry rejected", err)
	}
	if a.archiveFrozen(context.Background(), "reject", []byte(`{"node":"`+a.node+`","batch_id":"late-new-batch"}`)) == nil {
		t.Fatal("closed lane accepted new batch")
	}
}

func TestClosureDisabledOrUnavailableNeverInventsProof(t *testing.T) {
	c, meta := testConfig(t)
	a := testAgent(t, c, meta, roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") }))
	if err := a.closeArchive(context.Background()); err != nil {
		t.Fatal("legacy shutdown changed", err)
	}
	a.cfg.ArchiveClosure = true
	if a.closeArchive(context.Background()) == nil {
		t.Fatal("no archive silently sealed")
	}
	store := &memoryArchive{objects: map[string][]byte{}}
	if err := a.ConfigureArchive(store, "isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("a", 32)); err != nil {
		t.Fatal(err)
	}
	if a.closeArchive(context.Background()) == nil || len(store.objects) != 0 {
		t.Fatal("unleased source sealed")
	}
	a.leases["reject"] = time.Now().Unix() + 600
	a.inflight.Store(1)
	if a.closeArchive(context.Background()) == nil || len(store.objects) != 0 {
		t.Fatal("inflight source sealed")
	}
}

func TestClosureRequiresReceiverCapability(t *testing.T) {
	c, meta := testConfig(t)
	c.DeferredArchiveACK, c.ArchiveClosure = true, true
	var a *Agent
	a = testAgent(t, c, meta, roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(200, `{"ok":true,"version":1,"node":"`+a.node+`","lane":"reject","audience":"`+c.Audience+`","lease_until":`+strconv.FormatInt(time.Now().Unix()+600, 10)+`}`), nil
	}))
	if a.Renew(context.Background(), "reject") == nil || a.lease("reject") != 0 {
		t.Fatal("old receiver silently accepted closure mode")
	}
}
