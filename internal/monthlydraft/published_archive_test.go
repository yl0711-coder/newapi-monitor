package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func publishedArchiveFixture(t *testing.T) (displaySeriesFixture, monthlytasks.Task, monitor.PortalMonthlyBill) {
	t.Helper()
	f := newDisplaySeriesFixtureFrom(t, time.September)
	w, err := NewAutomaticDisplaySeriesLocalWorker(f.g, f.metadata, f.ranges, displaySeriesPublisher(f.auth))
	if err != nil {
		t.Fatal(err)
	}
	task, err := f.g.Submit(t.Context(), f.plans[0], f.inputs[0])
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := w.step(t.Context()); err != nil || !worked || w.Status().Published != 1 {
		t.Fatal("setup failed to save actual synthetic publication", worked, err, w.Status())
	}
	task, err = f.g.tasks.Get(t.Context(), task.Request.Owner, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	var bill monitor.PortalMonthlyBill
	if err := f.auth.First(&bill, "group_id=? AND month=?", task.Request.Owner.GroupID, task.Request.Month).Error; err != nil {
		t.Fatal(err)
	}
	return f, task, bill
}

func publishedArchiveDirectory(t *testing.T) string {
	t.Helper()
	path, err := filepath.EvalSymlinks(archiveDirectory(t))
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func copyPublishedFixture(t *testing.T, f displaySeriesFixture, task monthlytasks.Task) ArchiveReceipt {
	t.Helper()
	r, err := f.g.CopyPublishedGenerationArchive(t.Context(), f.auth, task.Request.Owner, task.ID, publishedArchiveDirectory(t))
	if err != nil {
		t.Fatal("published copy", err)
	}
	return r
}

func publishedPermitCount(t *testing.T, f displaySeriesFixture) int {
	t.Helper()
	var n int
	if err := f.g.receiver.QueryRow(`SELECT COUNT(*) FROM portal_monthly_published_release_permits`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func publishedCustomerHTTP(t *testing.T, f displaySeriesFixture) (func(string, string, string, []*http.Cookie) *httptest.ResponseRecorder, []*http.Cookie, []*http.Cookie) {
	t.Helper()
	m, err := monitor.New(monitor.Settings{StorePath: f.authorityPath, LocalSnapshotOnly: true, AlertsDisabled: true, PortalMonthlyStatementEnabled: true, SessionSecret: strings.Repeat("4", 64)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	hash, err := bcrypt.GenerateFromPassword([]byte("SyntheticPublishedArchive!2026"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.auth.Model(&monitor.CustomerGroup{}).Where("id IN ?", []int64{1, 2}).Update("portal_pw_admin", string(hash)).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	m.RegisterPortalRoutes(router)
	request := func(method, path, body string, cookies []*http.Cookie) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "http://127.0.0.1"+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "http://127.0.0.1")
		req.Header.Set("X-Monthly-Statistics", "nullable-v1")
		req.Header.Set("X-Monthly-Display", "cents-chain-v1")
		for _, c := range cookies {
			req.AddCookie(c)
		}
		out := httptest.NewRecorder()
		router.ServeHTTP(out, req)
		return out
	}
	login := func(email string) []*http.Cookie {
		out := request("POST", "/login", `{"email":"`+email+`","password":"SyntheticPublishedArchive!2026"}`, nil)
		if out.Code != 200 || len(out.Result().Cookies()) == 0 {
			t.Fatal("synthetic login", out.Code)
		}
		return out.Result().Cookies()
	}
	return request, login("owner@example.invalid"), login("other@example.invalid")
}

func publishedCustomerSnapshot(t *testing.T, request func(string, string, string, []*http.Cookie) *httptest.ResponseRecorder, own, foreign []*http.Cookie) ([]byte, map[string][]byte) {
	t.Helper()
	page := request("GET", "/api/monthly-statement?month=2026-09", "", own)
	var view struct {
		Data struct {
			Publication string `json:"publication"`
		} `json:"data"`
	}
	if page.Code != 200 || json.Unmarshal(page.Body.Bytes(), &view) != nil || view.Data.Publication == "" {
		t.Fatal("saved customer bill unavailable", page.Code)
	}
	if out := request("GET", "/api/monthly-statement?month=2026-09", "", foreign); out.Code == 200 {
		t.Fatal("cross-organization customer read succeeded")
	}
	book := request("GET", "/api/monthly-statement/download?month=2026-09&publication="+view.Data.Publication, "", own)
	if book.Code != 200 || !strings.Contains(book.Header().Get("Content-Type"), "spreadsheetml.sheet") {
		t.Fatal("customer XLSX unavailable", book.Code)
	}
	parts := retainedWorkbookParts(t, book.Body.Bytes())
	if len(parts["xl/worksheets/sheet1.xml"]) == 0 || len(parts["xl/worksheets/sheet2.xml"]) == 0 {
		t.Fatal("customer summary/member worksheets missing")
	}
	return bytes.Clone(page.Body.Bytes()), parts
}

func assertPublishedArchiveUnchanged(t *testing.T, f displaySeriesFixture, task monthlytasks.Task, bill monitor.PortalMonthlyBill, journal publicationRecord) {
	t.Helper()
	after, err := f.g.tasks.Get(t.Context(), task.Request.Owner, task.ID)
	if err != nil || after != task {
		t.Fatal("maintenance changed task/attempts/budget", err)
	}
	r, err := loadPublication(t.Context(), f.g.receiver, task.ID)
	if err != nil || r != journal {
		t.Fatal("maintenance changed immutable publication journal", err)
	}
	var actual monitor.PortalMonthlyBill
	if err := f.auth.First(&actual, "group_id=? AND month=?", bill.GroupID, bill.Month).Error; err != nil || actual != bill {
		t.Fatal("maintenance changed customer money or publication", err)
	}
	if err := monitor.ValidateMonthlyBillPublicationRecord(actual, time.Now()); err != nil {
		t.Fatal("customer bill no longer readable", err)
	}
}

func TestPublishedArchiveLatestFreshReleaseColdReadAndExactRestore(t *testing.T) {
	f, task, bill := publishedArchiveFixture(t)
	ctx := t.Context()
	request, own, foreign := publishedCustomerHTTP(t, f)
	view, book := publishedCustomerSnapshot(t, request, own, foreign)
	journal, err := loadPublication(ctx, f.g.receiver, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	original, err := f.g.loadRetainedGeneration(ctx, task.Request, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	funding, err := os.ReadFile(f.fundingPath)
	if err != nil {
		t.Fatal(err)
	}
	receipt := copyPublishedFixture(t, f, task)
	if err := VerifyGenerationArchive(ctx, receipt); err != nil {
		t.Fatal("v2 archive offline verification", err)
	}
	again, err := f.g.CopyPublishedGenerationArchive(ctx, f.auth, task.Request.Owner, task.ID, receipt.Directory)
	if err != nil || again != receipt {
		t.Fatal("copy replay changed receipt", err)
	}
	if err := f.g.ChangePublishedGenerationPayload(ctx, f.auth, receipt, task.Request.Owner, task.ID, false); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("release enabled without explicit opt-in", err)
	}
	if err := InitPublishedGenerationRetention(ctx, f.g.receiver); err != nil {
		t.Fatal(err)
	}
	if err := InitCompletedGenerationRetention(ctx, f.g.receiver); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("older initializer silently downgraded published barrier", err)
	}
	if _, err := f.g.receiver.Exec(`DELETE FROM portal_monthly_generation_inputs`); err == nil {
		t.Fatal("direct delete bypassed missing permit/reference")
	}
	for range 2 {
		if err := f.g.ChangePublishedGenerationPayload(ctx, f.auth, receipt, task.Request.Owner, task.ID, false); err != nil {
			t.Fatal("release/replay", err)
		}
		if evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 0 || evictionCount(t, f.g.receiver, "portal_monthly_generation_archives") != 1 || publishedPermitCount(t, f) != 0 {
			t.Fatal("release did not free exactly one hot slot or leaked permit")
		}
		cold, err := f.g.loadRetainedGeneration(ctx, task.Request, time.Now())
		if err != nil || !reflect.DeepEqual(cold, original) {
			t.Fatal("cold payload not exact", err)
		}
		if _, err := f.g.Run(ctx, task.Request.Owner, task.ID); err != nil {
			t.Fatal("actual ready audit failed after release", err)
		}
		assertPublishedArchiveUnchanged(t, f, task, bill, journal)
		coldView, coldBook := publishedCustomerSnapshot(t, request, own, foreign)
		if !bytes.Equal(view, coldView) || !reflect.DeepEqual(book, coldBook) {
			t.Fatal("release changed actual customer view/download")
		}
	}
	for range 2 {
		if err := f.g.ChangePublishedGenerationPayload(ctx, f.auth, receipt, task.Request.Owner, task.ID, true); err != nil {
			t.Fatal("exact restore/replay", err)
		}
	}
	restored, err := f.g.loadRetainedGeneration(ctx, task.Request, time.Now())
	if err != nil || !reflect.DeepEqual(restored, original) || evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 1 {
		t.Fatal("restore did not retain exact original input/artifact", err)
	}
	after, err := os.ReadFile(f.fundingPath)
	if err != nil || !bytes.Equal(funding, after) {
		t.Fatal("source evidence changed", err)
	}
	assertPublishedArchiveUnchanged(t, f, task, bill, journal)
}

func TestPublishedArchiveColdLatestPredecessorPublishesLaterMonthAfterRestart(t *testing.T) {
	f := newDisplaySeriesFixture(t)
	ctx := t.Context()
	w, err := NewAutomaticDisplaySeriesLocalWorker(f.g, f.metadata, f.ranges, displaySeriesPublisher(f.auth))
	if err != nil {
		t.Fatal(err)
	}
	old, err := f.g.Submit(ctx, f.plans[0], f.inputs[0])
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := w.step(ctx); err != nil || !worked {
		t.Fatal("publish first month", err)
	}
	old, err = f.g.tasks.Get(ctx, old.Request.Owner, old.ID)
	if err != nil {
		t.Fatal(err)
	}
	receipt := copyPublishedFixture(t, f, old)
	if err := InitPublishedGenerationRetention(ctx, f.g.receiver); err != nil {
		t.Fatal(err)
	}
	if err := f.g.ChangePublishedGenerationPayload(ctx, f.auth, receipt, old.Request.Owner, old.ID, false); err != nil {
		t.Fatal(err)
	}
	// Reopen handles and rebuild the worker; no in-memory retained payload or
	// completed task map is permitted to conceal a broken restart/cold reader.
	f.g, err = NewGeneration(generationDB(t, filepath.Join(f.g.root.Name(), "receiver.db"), false), f.g.funding, f.g.root.Name(), f.g.verifyCurrent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.g.Close() })
	f.g.receiver.SetMaxOpenConns(1)
	f.g.receiver.SetMaxIdleConns(1)
	ro, err := gorm.Open(sqlite.Open("file:"+f.authorityPath+"?mode=ro&_pragma=busy_timeout(500)"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	mainDB, err := ro.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mainDB.Close() })
	if _, _, _, err := f.g.auditPublishedArchive(ctx, ro, old.Request.Owner, old.ID); err != nil {
		t.Fatal("read-only main after restart", err)
	}
	load, err := NewRetainedAutomaticScopeLoader(f.g, "scope")
	if err != nil {
		t.Fatal(err)
	}
	meta, err := NewObservedAutomaticContext(f.g, load)
	if err != nil {
		t.Fatal(err)
	}
	w, err = NewAutomaticDisplaySeriesLocalWorker(f.g, meta, f.ranges, displaySeriesPublisher(f.auth))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.Submit(ctx, f.plans[2], f.inputs[2]); err != nil {
		t.Fatal(err)
	}
	if worked, err := w.step(ctx); err != nil || !worked || w.Status().Published != 1 {
		t.Fatal("actual later publication blocked by cold predecessor", err, w.Status())
	}
	displaySeriesCount(t, f, 2)
	if evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 2 {
		t.Fatal("later worker secretly restored predecessor")
	}
	after, err := f.g.tasks.Get(ctx, old.Request.Owner, old.ID)
	if err != nil || after != old {
		t.Fatal("later publication changed archived task", err)
	}
}

func TestPublishedArchiveRejectsUnpublishedForeignBrokenAndStaleEvidence(t *testing.T) {
	for _, kind := range []string{"draft_only", "foreign", "withdrawn", "corrupt_bill", "rehashed_payload", "journal_binding", "corrupt_evidence", "source_changed", "authority_changed", "partial_directory", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			f, task, bill := publishedArchiveFixture(t)
			owner, ctx := task.Request.Owner, t.Context()
			dir := publishedArchiveDirectory(t)
			switch kind {
			case "draft_only":
				// SETUP ONLY: emulate a draft before its publisher journal exists.
				if _, err := f.g.receiver.Exec(`DROP TRIGGER monthly_publication_no_delete`); err != nil {
					t.Fatal(err)
				}
				if _, err := f.g.receiver.Exec(`DELETE FROM portal_monthly_publication_jobs`); err != nil {
					t.Fatal(err)
				}
			case "foreign":
				owner.GroupID++
			case "withdrawn":
				if err := f.auth.Exec(`DELETE FROM portal_monthly_bills`).Error; err != nil {
					t.Fatal(err)
				}
			case "corrupt_bill":
				if err := f.auth.Exec(`UPDATE portal_monthly_bills SET payload='{}'`).Error; err != nil {
					t.Fatal(err)
				}
			case "corrupt_evidence":
				if err := f.auth.Exec(`UPDATE portal_monthly_bills SET evidence='{}'`).Error; err != nil {
					t.Fatal(err)
				}
			case "rehashed_payload":
				var payload map[string]json.RawMessage
				if err := json.Unmarshal([]byte(bill.Payload), &payload); err != nil {
					t.Fatal(err)
				}
				payload["month_end_balance"] = json.RawMessage(`"999.000000"`)
				raw, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.auth.Exec(`UPDATE portal_monthly_bills SET payload=?,payload_hash=?`, string(raw), generationHash(raw)).Error; err != nil {
					t.Fatal(err)
				}
			case "journal_binding":
				// SETUP ONLY: the normal saved-journal trigger forbids this.
				if _, err := f.g.receiver.Exec(`DROP TRIGGER monthly_publication_identity`); err != nil {
					t.Fatal(err)
				}
				if _, err := f.g.receiver.Exec(`UPDATE portal_monthly_publication_jobs SET evidence_hash=?`, strings.Repeat("f", 64)); err != nil {
					t.Fatal(err)
				}
			case "source_changed":
				if err := os.WriteFile(filepath.Join(f.g.root.Name(), "2026-09-original.json"), []byte(`[]`), 0600); err != nil {
					t.Fatal(err)
				}
			case "authority_changed":
				f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error { return ErrGenerationChanged }
			case "partial_directory":
				if err := os.WriteFile(filepath.Join(dir, "manifest.pending"), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if got, err := f.g.CopyPublishedGenerationArchive(ctx, f.auth, owner, task.ID, dir); err == nil || got != (ArchiveReceipt{}) {
				t.Fatal("unsafe copy returned receipt", kind, err)
			}
			after, err := f.g.tasks.Get(t.Context(), task.Request.Owner, task.ID)
			if err != nil || after != task || evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 1 {
				t.Fatal("rejected inspection mutated task or hot inputs", err)
			}
		})
	}
}

func TestPublishedArchiveReleaseRollbackAndChangedPublicationFence(t *testing.T) {
	f, task, bill := publishedArchiveFixture(t)
	ctx := t.Context()
	receipt := copyPublishedFixture(t, f, task)
	if err := InitPublishedGenerationRetention(ctx, f.g.receiver); err != nil {
		t.Fatal(err)
	}
	journal, err := loadPublication(ctx, f.g.receiver, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Fail AFTER reference insertion: reference, permit and delete must all roll
	// back together; a valid copy on disk never means the receiver was changed.
	if _, err := f.g.receiver.Exec(`CREATE TRIGGER test_published_release_failure BEFORE DELETE ON portal_monthly_generation_inputs BEGIN SELECT RAISE(ABORT,'synthetic disk/write interruption'); END`); err != nil {
		t.Fatal(err)
	}
	if err := f.g.ChangePublishedGenerationPayload(ctx, f.auth, receipt, task.Request.Owner, task.ID, false); err == nil {
		t.Fatal("injected delete failure accepted")
	}
	if evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 1 || evictionCount(t, f.g.receiver, "portal_monthly_generation_archives") != 0 || publishedPermitCount(t, f) != 0 {
		t.Fatal("failed release left partial state")
	}
	if _, err := f.g.receiver.Exec(`DROP TRIGGER test_published_release_failure`); err != nil {
		t.Fatal(err)
	}
	if err := f.auth.Exec(`UPDATE portal_monthly_bills SET publication_id=publication_id || 'reissued'`).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.g.ChangePublishedGenerationPayload(ctx, f.auth, receipt, task.Request.Owner, task.ID, false); err == nil {
		t.Fatal("stale archive authorized a changed publication")
	}
	if err := f.auth.Model(&monitor.PortalMonthlyBill{}).Where("group_id=? AND month=?", bill.GroupID, bill.Month).Update("publication_id", bill.PublicationID).Error; err != nil {
		t.Fatal(err)
	}
	if err := f.g.ChangePublishedGenerationPayload(ctx, f.auth, receipt, task.Request.Owner, task.ID, false); err != nil {
		t.Fatal("retry after rolled-back interruption", err)
	}
	assertPublishedArchiveUnchanged(t, f, task, bill, journal)
}

func TestPublishedArchiveCorruptionAndRestoreCapacityDoNotLoseOriginals(t *testing.T) {
	for _, kind := range []string{"archive_corrupt", "archive_missing", "hot_corrupt", "restore_capacity"} {
		t.Run(kind, func(t *testing.T) {
			f, task, bill := publishedArchiveFixture(t)
			ctx := t.Context()
			receipt := copyPublishedFixture(t, f, task)
			if err := InitPublishedGenerationRetention(ctx, f.g.receiver); err != nil {
				t.Fatal(err)
			}
			journal, err := loadPublication(ctx, f.g.receiver, task.ID)
			if err != nil {
				t.Fatal(err)
			}
			restore := kind == "restore_capacity"
			if restore {
				if err := f.g.ChangePublishedGenerationPayload(ctx, f.auth, receipt, task.Request.Owner, task.ID, false); err != nil {
					t.Fatal(err)
				}
				// SETUP ONLY: no money is calculated from these unrelated capacity
				// sentinels; the existing count limit must reject any restore.
				for i := range generationRecordLimit {
					if _, err := f.g.receiver.Exec(`INSERT INTO portal_monthly_generation_inputs VALUES(?,?,?,NULL,'')`, generationHash([]byte{byte(i)}), []byte(`{}`), []byte(`{}`)); err != nil {
						t.Fatal(err)
					}
				}
			} else if kind == "archive_corrupt" {
				if err := os.WriteFile(filepath.Join(receipt.Directory, task.Request.InputSHA256+".artifact"), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			} else if kind == "archive_missing" {
				if err := os.Remove(filepath.Join(receipt.Directory, "manifest.json")); err != nil {
					t.Fatal(err)
				}
			} else {
				// SETUP ONLY: simulate on-disk corruption outside normal guarded
				// writes. The production update barrier correctly forbids this.
				if _, err := f.g.receiver.Exec(`DROP TRIGGER monthly_generation_no_replace`); err != nil {
					t.Fatal(err)
				}
				if _, err := f.g.receiver.Exec(`UPDATE portal_monthly_generation_inputs SET artifact=? WHERE id=?`, []byte(`{}`), task.Request.InputSHA256); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.g.ChangePublishedGenerationPayload(ctx, f.auth, receipt, task.Request.Owner, task.ID, restore); err == nil {
				t.Fatal("unsafe change accepted", kind)
			}
			if restore && evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != generationRecordLimit {
				t.Fatal("failed restore changed capacity")
			}
			if !restore && evictionCount(t, f.g.receiver, "portal_monthly_generation_inputs") != 1 {
				t.Fatal("rejected release lost hot payload")
			}
			assertPublishedArchiveUnchanged(t, f, task, bill, journal)
		})
	}
}
