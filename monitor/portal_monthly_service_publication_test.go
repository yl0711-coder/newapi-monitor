package monitor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMonthlyServicePublicationGuardAuthorizationAndImmutability(t *testing.T) {
	m, _, _ := newPortalTestMonitor(t)
	t.Cleanup(m.Close)
	g := CustomerGroup{Name: "synthetic service publication", PortalEmail: "owner@example.invalid", PortalAuthVer: 1}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 43, GroupID: g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	e := monthlyFlatEvidence(g.ID, start.Format("2006-01"), 500000, now)
	raw := monthlyTestRaw(t, e)
	scope, err := loadMonthlyBillAuthorization(ctx, m.storeDB, g.ID)
	if err != nil {
		t.Fatal(err)
	}
	expected := MonthlyBillExpectedAuthorization{GroupID: g.ID, AuthVersion: 1, MemberFingerprint: scope.Fingerprint, MemberManifest: scope.Manifest}
	for _, service := range []*Monitor{nil, {}, m} {
		if _, err := service.PublishMonthlyBillForAuthorization(ctx, raw, expected); err == nil {
			t.Fatal("disabled/missing store published")
		}
	}
	m.cfg.PortalMonthlyStatementEnabled = true
	if _, err := m.PublishMonthlyBillForAuthorization(nil, raw, expected); err == nil { //nolint:staticcheck // Intentionally exercise the invalid-context guard, not a normal call.
		t.Fatal("nil context accepted")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := m.PublishMonthlyBillForAuthorization(canceled, raw, expected); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored", err)
	}
	if err := m.storeDB.Model(&CustomerGroup{}).Where("id=?", g.ID).Update("portal_auth_ver", 2).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.PublishMonthlyBillForAuthorization(ctx, raw, expected); err == nil {
		t.Fatal("revoked worker authorization published")
	}
	var count int64
	if err := m.storeDB.Model(&PortalMonthlyBill{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatal("failed publication left a bill", count, err)
	}
	expected.AuthVersion = 2
	if _, err := m.PublishMonthlyBillForAuthorization(ctx, raw, expected); err != nil {
		t.Fatal(err)
	}
	var before, after PortalMonthlyBill
	if err := m.storeDB.First(&before, "group_id=? AND month=?", g.ID, e.Month).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.PublishMonthlyBillForAuthorization(ctx, raw, expected); err != nil {
		t.Fatal("identical retry failed", err)
	}
	e.Users[0].OpeningQuota++
	e.Users[0].ClosingQuota++
	e.Users[0].ReverseClosingQuota++
	if _, err := m.PublishMonthlyBillForAuthorization(ctx, monthlyTestRaw(t, e), expected); err == nil {
		t.Fatal("retry replaced immutable customer amounts")
	}
	if err := m.storeDB.First(&after, "group_id=? AND month=?", g.ID, e.Month).Error; err != nil || after != before {
		t.Fatal("saved bill changed", err)
	}
}
