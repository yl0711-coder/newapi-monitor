package monitor

import (
	"path/filepath"
	"testing"
)

func TestOfflineSnapshotPreservesOpaqueUpstreamCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "monitor.db")
	cfg := Settings{LocalSnapshotOnly: true, SessionSecret: "local-only-secret"}
	m := &Monitor{cfg: cfg}
	if err := m.openStore(path); err != nil {
		t.Fatal(err)
	}
	row := ChannelUpstreamAccount{
		Domain: "opaque.example", Provider: upstreamProviderAICodeWith,
		Credential: "opaque-ciphertext-without-local-key", CredentialVersion: upstreamCredentialVersion,
		UsageBackfillDone: true,
	}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	m.Close()

	offline := &Monitor{cfg: cfg}
	if err := offline.openStore(path); err != nil {
		t.Fatalf("offline snapshot should not require production keys: %v", err)
	}
	var saved ChannelUpstreamAccount
	if err := offline.storeDB.First(&saved, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if saved.Credential != row.Credential || saved.CredentialVersion != row.CredentialVersion || !saved.UsageBackfillDone {
		t.Fatal("offline startup changed opaque credentials or completion evidence")
	}
	offline.Close()

	cfg.LocalSnapshotOnly = false
	online := &Monitor{cfg: cfg}
	if err := online.openStore(path); err == nil {
		online.Close()
		t.Fatal("online startup must still reject undecryptable credentials")
	}
}
