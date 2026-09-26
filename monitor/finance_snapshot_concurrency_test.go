package monitor

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestFinanceSnapshotReadDoesNotWaitForWriter(t *testing.T) {
	m := &Monitor{cfg: Settings{
		StorePath:                          filepath.Join(t.TempDir(), "monitor.db"),
		FinanceReportSnapshotShadowEnabled: true, FinanceReportSnapshotReadEnabled: true,
	}}
	now := time.Now().Truncate(time.Second)
	request := financeReportRequest{from: now.Add(-time.Hour), to: now, configurationHash: "cfg"}
	payload := []byte(`{"enabled":true}`)
	if err := m.persistFinanceReportSnapshotShadow(request.logicalKey(), "v1", payload, now); err != nil {
		t.Fatal(err)
	}
	m.financeSnapshotWriteMu.Lock()
	done := make(chan error, 1)
	go func() {
		got, _, _, ok, err := m.loadFinanceReportSnapshot(request, now)
		if err == nil && (!ok || !bytes.Equal(got, payload)) {
			err = fmt.Errorf("snapshot payload changed")
		}
		done <- err
	}()
	select {
	case err := <-done:
		m.financeSnapshotWriteMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		m.financeSnapshotWriteMu.Unlock()
		<-done
		t.Fatal("cached reader waited for the disk writer mutex")
	}
}

func TestFinanceSnapshotAtomicPublicationKeepsWholePayload(t *testing.T) {
	m := &Monitor{cfg: Settings{
		StorePath:                          filepath.Join(t.TempDir(), "monitor.db"),
		FinanceReportSnapshotShadowEnabled: true, FinanceReportSnapshotReadEnabled: true,
	}}
	now := time.Now().Truncate(time.Second)
	request := financeReportRequest{from: now.Add(-time.Hour), to: now, configurationHash: "cfg"}
	first, second := []byte(`{"amount":"1000000"}`), []byte(`{"amount":"2000000"}`)
	if err := m.persistFinanceReportSnapshotShadow(request.logicalKey(), "v1", first, now); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 100; i++ {
			payload := first
			if i%2 == 0 {
				payload = second
			}
			if err := m.persistFinanceReportSnapshotShadow(request.logicalKey(), "v1", payload, now); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	defer func() {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	for i := 0; i < 300; i++ {
		got, _, _, ok, err := m.loadFinanceReportSnapshot(request, now)
		if err != nil || !ok || (!bytes.Equal(got, first) && !bytes.Equal(got, second)) {
			t.Fatalf("partial or invalid published snapshot: ok=%t err=%v", ok, err)
		}
	}
}
