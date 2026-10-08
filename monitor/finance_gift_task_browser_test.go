//go:build unix

package monitor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// Explicit local browser fixture. _test.go is excluded from release binaries.
// It creates fresh receiver/main copies, has no production clients and accepts
// connections only on loopback. The random one-use bootstrap mints a normal
// short-lived test session restricted to the handoff path, never a prod login.
func TestFinanceGiftTaskBrowserAcceptance(t *testing.T) {
	if os.Getenv("MONITOR_GIFT_BROWSER_ACCEPTANCE") != "1" {
		t.Skip("manual loopback browser acceptance only")
	}
	job, digest, backup := os.Getenv("MONITOR_GIFT_HTTP_SOURCE_JOB"), os.Getenv("MONITOR_GIFT_HTTP_SOURCE_SHA256"), os.Getenv("MONITOR_GIFT_HTTP_RECEIVER")
	if job == "" || digest == "" || backup == "" {
		t.Fatal("requires private closed source and receiver snapshots")
	}
	var plan FinanceGiftLocalPlan
	if _, err := giftLocalReadJSON(filepath.Join(job, "plan.json"), &plan); err != nil {
		t.Fatal(err)
	}
	m, r, _ := giftPreviewHTTPMonitor(t, job, digest, backup, plan.Targets[0].SourceEpoch)
	attachGiftAuthorizationStore(t, m)
	m.cfg.LocalSnapshotOnly = true
	m.cfg.FinanceGiftHandoffLocalExecutionEnabled = true
	money := giftMixedMonetarySnapshot(t, m.usageFactsDB)
	originalSource := giftLocalFileHash(t, filepath.Join(job, "usage-facts.db"))
	originalBackup := giftLocalFileHash(t, backup)
	var token [24]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	key := hex.EncodeToString(token[:])
	finished := make(chan struct{})
	var finishOnce, loginOnce sync.Once
	r.GET("/acceptance/"+key, func(c *gin.Context) {
		used := false
		loginOnce.Do(func() {
			used = true
			http.SetCookie(c.Writer, &http.Cookie{Name: sessionCookie, Value: m.signSession("local-browser-acceptance", roleRoot, time.Now().Unix()), Path: "/finance/gift-handoff", HttpOnly: true, SameSite: http.SameSiteStrictMode, MaxAge: 900})
		})
		if !used {
			c.Status(410)
			return
		}
		c.Header("Cache-Control", "no-store")
		c.Redirect(302, "/finance/gift-handoff/local")
	})
	r.POST("/acceptance/"+key+"/finish", func(c *gin.Context) { finishOnce.Do(func() { close(finished) }); c.Status(202) })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(r)
	server.Listener.Close()
	server.Listener = listener
	server.Start()
	defer server.Close()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if !m.financeGiftTask.shutdown(ctx) {
			t.Error("browser worker did not stop")
		}
	}()
	fmt.Printf("LOCAL_BROWSER_URL=%s/acceptance/%s\nLOCAL_BROWSER_FINISH=%s/acceptance/%s/finish\n", server.URL, key, server.URL, key)
	select {
	case <-finished:
	case <-time.After(15 * time.Minute):
		t.Fatal("browser acceptance timed out")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if !m.financeGiftTask.shutdown(ctx) {
		t.Fatal("browser worker did not stop")
	}
	if money != giftMixedMonetarySnapshot(t, m.usageFactsDB) || originalSource != giftLocalFileHash(t, filepath.Join(job, "usage-facts.db")) || originalBackup != giftLocalFileHash(t, backup) {
		t.Fatal("browser changed money or original inputs")
	}
	var rows []financeGiftHandoffAuthorization
	if err := m.storeDB.Find(&rows).Error; err != nil || len(rows) == 0 {
		t.Fatal("no browser authorization", err)
	}
	for _, row := range rows {
		p, err := decodeFinanceGiftAuthorization(row)
		if err != nil {
			t.Fatal(err)
		}
		progress, err := readFinanceGiftCommitProgress(context.Background(), m.financeFactsReadStore(), row.ID, p)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("browser durable result: %d/%d targets, %d rows, revoked=%t", progress.CommittedTargets, progress.AuthorizedTargets, progress.RowsUpdated, row.RevokedAt > 0)
	}
}
