package monitor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestMonthlyOnDemandSavedBillSurvivesTaskRetirement(t *testing.T) {
	f := newMonthlyFunctionalFixture(t)
	cookie := &http.Cookie{Name: portalSessionCookie, Value: f.m.signPortalSession(f.a.ID, f.a.PortalAuthVer, time.Now().Unix())}
	var original PortalMonthlyBill
	if err := f.m.storeDB.First(&original, "group_id=? AND month=?", f.a.ID, f.latest).Error; err != nil {
		t.Fatal(err)
	}
	calls := 0
	err := f.m.RegisterLocalPortalMonthlyGenerationRoutes(f.r, monthlyGenerationTestService{func(context.Context, int64, int64, string, bool) (MonthlyGenerationView, error) {
		calls++
		return MonthlyGenerationView{}, errors.New("synthetic task/source retired")
	}})
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		for _, method := range []string{"GET", "POST"} {
			suffix, body := "?month="+f.latest, ""
			if method == "POST" {
				suffix, body = "", fmt.Sprintf(`{"month":%q}`, f.latest)
			}
			w := generationHTTPDo(f.r, method, suffix, body, cookie, nil)
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"state":"available"`) || calls != 0 {
				t.Fatal("saved bill regenerated or unavailable", w.Code, calls, w.Body.String())
			}
		}
	}
	var after PortalMonthlyBill
	if err := f.m.storeDB.First(&after, "group_id=? AND month=?", f.a.ID, f.latest).Error; err != nil || original != after {
		t.Fatal("frozen publication changed", err)
	}
	// A damaged saved bill must not be treated as missing or overwritten.
	w := generationHTTPDo(f.r, "POST", "", fmt.Sprintf(`{"month":%q}`, f.bad), cookie, nil)
	if w.Code != 503 || calls != 0 || strings.Contains(w.Body.String(), "synthetic") {
		t.Fatal("damaged bill recalculated or private error leaked", w.Code, calls)
	}
	// Other organizations' publications must not be reused.
	other := &http.Cookie{Name: portalSessionCookie, Value: f.m.signPortalSession(f.empty.ID, f.empty.PortalAuthVer, time.Now().Unix())}
	w = generationHTTPDo(f.r, "GET", "?month="+f.latest, "", other, nil)
	if w.Code != 503 || calls != 1 || strings.Contains(w.Body.String(), `"state":"available"`) {
		t.Fatal("cross-organization reuse", w.Code, calls, w.Body.String())
	}
	// An old signed session may not recover even an already-generated bill.
	if err := f.m.storeDB.Model(&CustomerGroup{}).Where("id=?", f.a.ID).Update("portal_auth_ver", f.a.PortalAuthVer+1).Error; err != nil {
		t.Fatal(err)
	}
	w = generationHTTPDo(f.r, "GET", "?month="+f.latest, "", cookie, nil)
	if w.Code != 401 || calls != 1 {
		t.Fatal("revoked session reused bill", w.Code, calls)
	}
}

func TestMonthlyOnDemandAvailabilityRequiresStoredPublication(t *testing.T) {
	f := newMonthlyFunctionalFixture(t)
	cookie := &http.Cookie{Name: portalSessionCookie, Value: f.m.signPortalSession(f.a.ID, f.a.PortalAuthVer, time.Now().Unix())}
	if err := f.m.storeDB.Delete(&PortalMonthlyBill{}, "group_id=? AND month=?", f.a.ID, f.latest).Error; err != nil {
		t.Fatal(err)
	}
	calls, save := 0, false
	state := "available"
	err := f.m.RegisterLocalPortalMonthlyGenerationRoutes(f.r, monthlyGenerationTestService{func(ctx context.Context, _, _ int64, month string, _ bool) (MonthlyGenerationView, error) {
		calls++
		if save {
			if _, err := ImportMonthlyBill(ctx, f.m.storeDB, monthlyTestRaw(t, f.evidence), true, time.Now()); err != nil {
				t.Fatal(err)
			}
		}
		return MonthlyGenerationView{Month: month, State: state}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	w := generationHTTPDo(f.r, "GET", "?month="+f.latest, "", cookie, nil)
	if w.Code != 503 || calls != 1 {
		t.Fatal("service forged availability", w.Code, calls)
	}
	state = "pending_verification"
	w = generationHTTPDo(f.r, "GET", "?month="+f.latest, "", cookie, nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"state":"pending_verification"`) {
		t.Fatal("draft incorrectly available", w.Code, w.Body.String())
	}
	// Completion racing submission is observed by the final read. The server
	// does not invoke submission twice and responds 200 for the now-saved bill.
	save = true
	w = generationHTTPDo(f.r, "POST", "", fmt.Sprintf(`{"month":%q}`, f.latest), cookie, nil)
	if w.Code != 200 || calls != 3 || !strings.Contains(w.Body.String(), `"state":"available"`) {
		t.Fatal("completion race not recovered", w.Code, calls, w.Body.String())
	}
	// An actual roster change must not make a frozen bill look ungenerated.
	if _, err := f.m.addUsageMember(context.Background(), TrackedUser{UserID: 999}, f.a.ID, usageMemberMutationMeta{RequestID: "saved-bill-new-member"}); err != nil {
		t.Fatal(err)
	}
	save = false
	w = generationHTTPDo(f.r, "GET", "?month="+f.latest, "", cookie, nil)
	if w.Code != 200 || calls != 3 || !strings.Contains(w.Body.String(), `"state":"available"`) {
		t.Fatal("changed membership hid a saved publication", w.Code, calls, w.Body.String())
	}
}
