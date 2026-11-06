package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

func contextDirectoryLabels(t *testing.T, f capturedHistoryFixture) {
	t.Helper()
	if err := f.auth.Model(&monitor.TrackedUser{}).Where("group_id=?", f.req.Owner.GroupID).Update("username", "shared-label").Error; err != nil {
		t.Fatal(err)
	}
}

func sameSecondHistoryTail(o *monthlysource.WalletTailObservation, phase int) {
	o.StartedAtMS += 37
	o.CompletedAtMS += 37
	u := o.Request.UserID
	first := o.CompletedAtMS/1000 - int64(phase)*360
	id := int64(90000000) + u*10
	o.Funding = []monthlysource.FundingLog{{ID: id + 1, UserID: u, CreatedAt: first, Type: 1, Content: "使用在线充值成功，充值金额: $1.000000，支付金额：1.00"}}
	consume, refund := int64(200000), int64(50000)
	o.Usage = []monthlysource.UsageWindowRow{
		{UserID: u, Type: 2, BillingSource: "wallet", Records: 1, Quota: consume, MinQuota: consume, MaxQuota: consume, MinID: id + 3, MaxID: id + 3, FirstAt: first, LastAt: first},
		{UserID: u, Type: 6, BillingSource: "wallet", Records: 1, Quota: refund, MinQuota: refund, MaxQuota: refund, MinID: id + 4, MaxID: id + 4, FirstAt: first, LastAt: first},
	}
	funds := int64(500000)
	if phase == 1 {
		consume = 550000
		funds += 750000
		o.Funding = append(o.Funding, monthlysource.FundingLog{ID: id + 2, UserID: u, CreatedAt: o.CompletedAtMS / 1000, Type: 1, Content: "使用在线充值成功，充值金额: $1.500000，支付金额：1.50"})
		o.Usage[0].Records = 2
		o.Usage[0].Quota = consume
		o.Usage[0].MinQuota = 200000
		o.Usage[0].MaxQuota = 350000
		o.Usage[0].MaxID = id + 5
		o.Usage[0].LastAt = o.CompletedAtMS / 1000
	}
	*o.User.BalanceQuota = funds - consume + refund
	*o.User.UsedQuota = 1000000 + consume - refund
}

func TestAutomaticHistoryContextInputPreservesCurrentViewAndSameSecondMoney(t *testing.T) {
	f, _, ref := automaticHistoryOriginalFixtureWithTail(t, sameSecondHistoryTail)
	contextDirectoryLabels(t, f)
	got, err := f.f.g.prepareAutomaticHistoryContextInput(t.Context(), f.auth, ref, f.req)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.accounts) != 2 {
		t.Fatal("lost accounts")
	}
	for _, a := range got.accounts {
		if a.CompletedAtMS%1000 != 37 || a.Username != "shared-label" || a.BalanceQuota != 750000 || a.UsedQuota != 1500000 ||
			a.MonthConsumeQuota != 1500000 || a.MonthRefundQuota != 500000 || a.BridgeConsumeQuota != 550000 || a.BridgeRefundQuota != 50000 ||
			a.OriginBalanceQuota != 1000000 || !a.OriginRequiresResolution || len(a.Funding) != 2 {
			t.Fatal("view cut off/retimed or origin hidden", a)
		}
		if a.Funding[1].At != a.CompletedAtMS/1000 || a.Funding[1].DeltaQuota != 750000 {
			t.Fatal("same-second credit excluded", a)
		}
	}
	again, err := f.f.g.prepareAutomaticHistoryContextInput(t.Context(), f.auth, ref, f.req)
	if err != nil || !reflect.DeepEqual(got.accounts, again.accounts) || !bytes.Equal(got.directory, again.directory) || !bytes.Equal(got.input.observed.Preparation, again.input.observed.Preparation) {
		t.Fatal("replay changed source money", err)
	}
	if _, err := json.Marshal(got); err == nil {
		t.Fatal("context intermediate became approval JSON")
	}
	if err := json.Unmarshal([]byte(`{}`), &got); err == nil || len(got.accounts) != 0 {
		t.Fatal("JSON retained old context")
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistoryContextInputIncludesLaterClosedMonths(t *testing.T) {
	f, boundary, _ := automaticHistoryOriginalFixtureWithTail(t, sameSecondHistoryTail, "2026-08")
	contextDirectoryLabels(t, f)
	req := SourcePreparationRequest{Owner: f.req.Owner, Month: "2026-08"}
	seedHistoryEmptyMonth(t, f, req, false)
	p, err := newAutomaticHistoryPreparation(f.wallet, f.spool)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := collectCapturedSources(t.Context(), p.raw, p.core, req); err != nil {
		t.Fatal(err)
	}
	older, sealed, err := collectCapturedSources(t.Context(), p.raw, p.core, req)
	if err != nil || !sealed {
		t.Fatal("older raw dependency missing", err)
	}
	ref := writeAutomaticHistoryManifest(t, f.f.root, automaticHistorySources{Version: 1, Mode: automaticHistorySourcesMode, Owner: req.Owner, SourceIdentity: f.wallet.funding.sourceIdentity, Boundary: boundary, Months: []CapturedSourceFiles{older, boundary}})
	got, err := f.f.g.prepareAutomaticHistoryContextInput(t.Context(), f.auth, ref, req)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range got.accounts {
		if a.Month != "2026-08" || a.MonthConsumeQuota != 0 || a.MonthRefundQuota != 0 || a.BridgeConsumeQuota != 2050000 || a.BridgeRefundQuota != 550000 || a.BalanceQuota != 750000 || a.UsedQuota != 1500000 {
			t.Fatal("later month omitted or bridge netted incorrectly", a)
		}
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistoryContextInputDirectoryReadFailureReturnsNoPartialData(t *testing.T) {
	f, _, ref := automaticHistoryOriginalFixture(t)
	contextDirectoryLabels(t, f)
	// Source normalization is local-only. Fail the second metadata read via a
	// test callback; previously read labels must not escape as partial context.
	calls := 0
	key := fmt.Sprintf("context-directory-change-%p", f.auth)
	if err := f.auth.Callback().Query().After("gorm:query").Register(key, func(db *gorm.DB) {
		if db.Statement.Table != "customer_groups" {
			return
		}
		calls++
		if calls == 2 {
			_ = db.AddError(errors.New("synthetic label view unavailable"))
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.auth.Callback().Query().Remove(key) })
	if got, err := f.f.g.prepareAutomaticHistoryContextInput(t.Context(), f.auth, ref, f.req); err == nil || len(got.accounts) != 0 || len(got.directory) != 0 {
		t.Fatal("partial directory/context exposed", err)
	}
	generationAdmissionCounts(t, f.f.receiver, 0, 0)
}

func TestAutomaticHistoryContextInputRejectsCrossOwnerAndCancellation(t *testing.T) {
	f, _, ref := automaticHistoryOriginalFixture(t)
	contextDirectoryLabels(t, f)
	raw, err := captureAutomaticHistoryDirectory(t.Context(), f.auth, f.req.Owner)
	if err != nil {
		t.Fatal(err)
	}
	d, err := decodeAutomaticHistoryDirectory(raw, f.req.Owner)
	if err != nil {
		t.Fatal(err)
	}
	audited, err := f.f.g.auditAutomaticHistorySources(t.Context(), ref, f.req)
	if err != nil {
		t.Fatal(err)
	}
	d.Owner.GroupID++
	if got, err := projectAutomaticHistoryView(t.Context(), audited, d, f.req.Month); err == nil || len(got) != 0 {
		t.Fatal("cross-owner data joined")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := f.f.g.prepareAutomaticHistoryContextInput(ctx, f.auth, ref, f.req); !errors.Is(err, context.Canceled) || len(got.accounts) != 0 {
		t.Fatal("cancelled generation returned data", err)
	}
}
