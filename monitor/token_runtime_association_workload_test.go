package monitor

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Use a private copy of retained entry evidence alongside the Token snapshot.
// Synthetic requests have real matching HMAC rows before their source window
// is published. No test mutation touches the retained recovery input.
func tokenRuntimeAssociationEvidence(t *testing.T, m *Monitor, start, rate int64) bool {
	t.Helper()
	if os.Getenv("TOKEN_TEST_ASSOCIATION_EVIDENCE") != "1" {
		return false
	}
	snapshot := os.Getenv("TOKEN_TEST_NGINX_SNAPSHOT")
	if snapshot == "" {
		t.Fatal("association workload requires retained entry evidence")
	}
	path := filepath.Join(t.TempDir(), "nginx.db")
	if err := tokenTestCopyFile(snapshot, path); err != nil {
		t.Fatal(err)
	}
	db, err := gorm.Open(sqlite.Open(path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = pool.Close() })
	if old, err := m.nginxEvidenceDB.DB(); err == nil {
		_ = old.Close()
	}
	m.nginxEvidenceDB = db
	// Cover the test's maximum 25-minute lifetime. Extra entry rows are not
	// source facts or coverage proofs; only the generator publishes log rows.
	count := int64(0)
	for window := int64(0); window < 10; window++ {
		n := rate * 300
		if window < 3 {
			n = 512
		}
		rows := make([]NginxRequestEvidence, 0, n)
		for offset := int64(1); offset <= n; offset++ {
			id := int64(1_000_000_000) + window*10000 + offset
			rows = append(rows, NginxRequestEvidence{EventID: fmt.Sprintf("synthetic-association-%d", id), Node: "synthetic-association",
				EventMS: (start + window*300 + 1) * 1000, HMACKeyID: m.cfg.NginxEvidenceHMACKeyID,
				OneAPIIDHMAC: nginxEvidenceIDHMAC(m.cfg.NginxEvidenceHMACKey, "oneapi-request-id", fmt.Sprintf("synthetic-%d", id)),
				Status:       200, Route: "/v1/messages", RequestLength: ptrTokenMetering(1000), RequestLengthState: "valid", RequestLengthVersion: "request-length-v1"})
		}
		if err := db.CreateInBatches(rows, 100).Error; err != nil {
			t.Fatal(err)
		}
		count += n
	}
	t.Logf("retained entry evidence privately copied; labelled synthetic HMAC entries=%d", count)
	return true
}
