package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
)

// Exercise real handlers and audited member mutations against temporary SQLite.
// Saved amounts, account/token details and publication versions must not change
// as a side effect of reading a historical bill under a new current roster.
func TestMonthlyHistoricalBillSurvivesRosterChangesWithoutRegeneration(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	t.Cleanup(f.m.Close)
	ctx := context.Background()
	beforeScope, err := LoadMonthlyGenerationScope(ctx, f.m.storeDB, f.g.ID, f.g.PortalAuthVer)
	if err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	if err := f.m.RegisterPortalRoutesWithGeneration(r, monthlyGenerationTestService{func(context.Context, int64, int64, string, bool) (MonthlyGenerationView, error) {
		t.Fatal("saved historical bill triggered generation")
		return MonthlyGenerationView{}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	f.r = r
	detailPath := "/api/monthly-statement?month=" + f.e.Month
	downloadPath := "/api/monthly-statement/download?month=" + f.e.Month + "&publication=" + f.version
	detail := portalDo(r, http.MethodGet, detailPath, "", f.cookie)
	workbook := portalDo(r, http.MethodGet, downloadPath, "", f.cookie)
	if detail.Code != 200 || workbook.Code != 200 {
		t.Fatal("baseline unavailable", detail.Code, workbook.Code)
	}
	other := CustomerGroup{Name: "other synthetic organization", PortalEmail: "other@example.invalid"}
	if err := f.m.storeDB.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	otherCookie := &http.Cookie{Name: portalSessionCookie, Value: f.m.signPortalSession(other.ID, other.PortalAuthVer, f.now.Unix())}
	assertFrozen := func(t *testing.T) {
		t.Helper()
		w := portalDo(r, http.MethodGet, detailPath, "", f.cookie)
		if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), detail.Body.Bytes()) {
			t.Fatal("historical payload changed or disappeared", w.Code)
		}
		w = portalDo(r, http.MethodGet, downloadPath, "", f.cookie)
		if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), workbook.Body.Bytes()) {
			t.Fatal("historical workbook changed or disappeared", w.Code)
		}
		w = portalDo(r, http.MethodGet, "/api/monthly-statements", "", f.cookie)
		var listing struct {
			Data struct {
				Rows []monthlyStatementRow `json:"rows"`
			} `json:"data"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &listing) != nil || len(listing.Data.Rows) != 1 || listing.Data.Rows[0].Month != f.e.Month {
			t.Fatal("historical bill missing from list", w.Code, w.Body.String())
		}
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			suffix, body := "?month="+f.e.Month, ""
			if method == http.MethodPost {
				suffix, body = "", `{"month":"`+f.e.Month+`"}`
			}
			w = generationHTTPDo(r, method, suffix, body, f.cookie, nil)
			var state MonthlyGenerationView
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &state) != nil || state.State != "available" {
				t.Fatal("saved bill treated as a new generation request", w.Code, w.Body.String())
			}
		}
		var saved PortalMonthlyBill
		if err := f.m.storeDB.First(&saved, "group_id = ? AND month = ?", f.g.ID, f.e.Month).Error; err != nil || saved != f.old {
			t.Fatal("historical payload/evidence/version mutated", err)
		}
		monthlyRecoveryCounts(t, f.m, 1, 0)
	}
	assertOtherCannotRead := func(t *testing.T) {
		t.Helper()
		for _, path := range []string{detailPath, detailPath + "&group_id=" + strconv.FormatInt(f.g.ID, 10) + "&uid=43", downloadPath} {
			if w := portalDo(r, http.MethodGet, path, "", otherCookie); w.Code != 404 {
				t.Fatal("another organization accessed original publication", path, w.Code)
			}
		}
		w := portalDo(r, http.MethodGet, "/api/monthly-statements", "", otherCookie)
		var listing struct {
			Data struct {
				Rows []monthlyStatementRow `json:"rows"`
			} `json:"data"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &listing) != nil || len(listing.Data.Rows) != 0 {
			t.Fatal("another organization listed original publication", w.Code)
		}
	}
	t.Run("added member", func(t *testing.T) {
		if _, err := f.m.addUsageMember(ctx, TrackedUser{UserID: 44}, f.g.ID, usageMemberMutationMeta{RequestID: "history-add"}); err != nil {
			t.Fatal(err)
		}
		assertFrozen(t)
		assertOtherCannotRead(t)
		if _, err := f.m.PublishMonthlyBillForAuthorization(ctx, f.raw, MonthlyBillExpectedAuthorization{
			GroupID: f.g.ID, AuthVersion: f.g.PortalAuthVer, MemberFingerprint: beforeScope.MemberFingerprint, MemberManifest: beforeScope.MemberManifest,
		}); err == nil {
			t.Fatal("new publication accepted stale worker/member authorization")
		}
	})
	t.Run("original member moved to another organization", func(t *testing.T) {
		if _, err := f.m.correctUsageMemberCompany(ctx, 43, other.ID, usageMemberMutationMeta{RequestID: "history-move"}); err != nil {
			t.Fatal(err)
		}
		assertFrozen(t)
		assertOtherCannotRead(t)
	})
	t.Run("no current members", func(t *testing.T) {
		if _, err := f.m.removeUsageMember(ctx, 44, usageMemberMutationMeta{RequestID: "history-empty"}); err != nil {
			t.Fatal(err)
		}
		assertFrozen(t)
		assertOtherCannotRead(t)
	})
	t.Run("rejoined current member", func(t *testing.T) {
		if _, err := f.m.addUsageMember(ctx, TrackedUser{UserID: 44}, f.g.ID, usageMemberMutationMeta{RequestID: "history-rejoin"}); err != nil {
			t.Fatal(err)
		}
		assertFrozen(t)
		assertOtherCannotRead(t)
	})
	t.Run("revoked organization session", func(t *testing.T) {
		if err := f.m.storeDB.Model(&CustomerGroup{}).Where("id = ?", f.g.ID).Update("portal_auth_ver", f.g.PortalAuthVer+1).Error; err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{detailPath, downloadPath, "/api/monthly-statements"} {
			for _, cookies := range [][]*http.Cookie{nil, {f.cookie}} {
				if w := portalDo(r, http.MethodGet, path, "", cookies...); w.Code != 401 {
					t.Fatal("revoked or anonymous history access", w.Code)
				}
			}
		}
		if w := generationHTTPDo(r, http.MethodPost, "", `{"month":"`+f.e.Month+`"}`, f.cookie, nil); w.Code != 401 {
			t.Fatal("revoked generation access", w.Code)
		}
		f.cookie.Value = f.m.signPortalSession(f.g.ID, f.g.PortalAuthVer+1, f.now.Unix())
		assertFrozen(t)
	})
}

func TestMonthlyHistoricalPrototypeWithoutVerifiedScopeStaysUnavailable(t *testing.T) {
	for _, field := range []string{"member_manifest", "member_fingerprint"} {
		t.Run(field, func(t *testing.T) {
			f := newMonthlyRecoveryFixture(t)
			t.Cleanup(f.m.Close)
			if err := f.m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id = ? AND month = ?", f.g.ID, f.e.Month).Update(field, "").Error; err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{"/api/monthly-statement?month=" + f.e.Month, "/api/monthly-statement/download?month=" + f.e.Month + "&publication=" + f.version} {
				if w := portalDo(f.r, http.MethodGet, path, "", f.cookie); w.Code != 404 {
					t.Fatal("unverified prototype became readable", field, w.Code)
				}
			}
			w := portalDo(f.r, http.MethodGet, "/api/monthly-statements", "", f.cookie)
			var listing struct {
				Data struct {
					Rows []monthlyStatementRow `json:"rows"`
				} `json:"data"`
			}
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &listing) != nil || len(listing.Data.Rows) != 0 {
				t.Fatal("unverified prototype listed", field, w.Code)
			}
			scope, err := LoadMonthlyGenerationScope(context.Background(), f.m.storeDB, f.g.ID, f.g.PortalAuthVer)
			if err != nil {
				t.Fatal(err)
			}
			if saved, err := f.m.monthlyGenerationSavedBill(context.Background(), scope, f.e.Month); err != nil || saved != nil {
				t.Fatal("unverified prototype recognized as generated", field, err)
			}
		})
	}
}

func TestMonthlyHistoricalCorruptScopeNeverRegeneratesOrLeaksAmounts(t *testing.T) {
	for _, field := range []string{"member_manifest", "member_fingerprint"} {
		t.Run(field, func(t *testing.T) {
			f := newMonthlyRecoveryFixture(t)
			t.Cleanup(f.m.Close)
			if err := f.m.storeDB.Model(&PortalMonthlyBill{}).Where("group_id = ? AND month = ?", f.g.ID, f.e.Month).Update(field, "damaged-synthetic-digest").Error; err != nil {
				t.Fatal(err)
			}
			r := gin.New()
			if err := f.m.RegisterPortalRoutesWithGeneration(r, monthlyGenerationTestService{func(context.Context, int64, int64, string, bool) (MonthlyGenerationView, error) {
				t.Fatal("damaged saved bill triggered generation")
				return MonthlyGenerationView{}, nil
			}}); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{"/api/monthly-statement?month=" + f.e.Month, "/api/monthly-statement/download?month=" + f.e.Month + "&publication=" + f.version} {
				w := portalDo(r, http.MethodGet, path, "", f.cookie)
				if w.Code != 503 || bytes.Contains(w.Body.Bytes(), []byte("500.000000")) {
					t.Fatal("damaged saved scope served customer amounts", field, w.Code)
				}
			}
			for _, method := range []string{http.MethodGet, http.MethodPost} {
				suffix, body := "?month="+f.e.Month, ""
				if method == http.MethodPost {
					suffix, body = "", `{"month":"`+f.e.Month+`"}`
				}
				if w := generationHTTPDo(r, method, suffix, body, f.cookie, nil); w.Code != 503 {
					t.Fatal("damaged saved scope treated as available or missing", field, w.Code)
				}
			}
			w := portalDo(r, http.MethodGet, "/api/monthly-statements", "", f.cookie)
			var listing struct {
				Data struct {
					Rows        []monthlyStatementRow `json:"rows"`
					Unavailable []string              `json:"unavailable_months"`
				} `json:"data"`
			}
			if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &listing) != nil || len(listing.Data.Rows) != 0 || len(listing.Data.Unavailable) != 1 || listing.Data.Unavailable[0] != f.e.Month {
				t.Fatal("damaged month not isolated", field, w.Code)
			}
			monthlyRecoveryCounts(t, f.m, 1, 0)
		})
	}
}
