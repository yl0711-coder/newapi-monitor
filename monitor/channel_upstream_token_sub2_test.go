package monitor

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func sub2TokenFixture(t *testing.T) (ChannelUpstreamAccount, []sub2PricingUsageItem, int64, []byte) {
	t.Helper()
	a := tokenCompatSub2Item(t, `,"api_key_id":575`)
	b := tokenCompatSub2Item(t, `,"api_key_id":574`)
	b.ID++
	b.ActualCostMicros = 234567
	c := a
	c.ID += 2
	c.CreatedAt += 3600
	return ChannelUpstreamAccount{Domain: "sub2.example", Provider: upstreamProviderSub2API, BaseURL: "https://sub2.example", Account: "fixture"},
		[]sub2PricingUsageItem{a, b, c}, cstDayStart(a.CreatedAt), []byte("0123456789abcdef0123456789abcdef")
}

func TestSub2TokenEvidenceConservesAccountMoneyAndHourIdentity(t *testing.T) {
	account, items, day, key := sub2TokenFixture(t)
	rows, err := buildSub2TokenEvidence(account, items, day, day+86400, key, "v1")
	if err != nil || len(rows) != 3 {
		t.Fatalf("build: rows=%d err=%v", len(rows), err)
	}
	legacy, err := buildSub2PricingEvidence(account, items, day+86400)
	if err != nil {
		t.Fatal(err)
	}
	var total, oldTotal, requests int64
	for _, row := range rows {
		total += row.ChargeUnits
		requests += row.Requests
		if row.ChargeUnitsPerUSD != "1000000" || row.ChargeUnit != sub2TokenChargeUnit || row.SourceRefKind != sub2TokenSourceKind || row.ContentHash != channelCostEvidenceRowHash(row) {
			t.Fatal("lost accounting unit or evidence identity")
		}
	}
	for _, row := range legacy {
		oldTotal += row.FinalQuota
	}
	if total != 481479 || total != oldTotal || requests != 3 || rows[0].HourTs == rows[2].HourTs {
		t.Fatal("token split changed legacy cost or merged different hours")
	}
	// Effective/group ratios are not multipliers on the already final cost.
	if len(legacy) != 2 {
		t.Fatal("fixture must exercise two tokens sharing one account dimension")
	}
	again, err := buildSub2TokenEvidence(account, []sub2PricingUsageItem{items[2], items[0], items[1]}, day, day+86400, key, "v1")
	if err != nil || !reflect.DeepEqual(rows, again) {
		t.Fatal("input order changed token evidence")
	}
	account.Account = "different-account"
	other, err := buildSub2TokenEvidence(account, items, day, day+86400, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range rows {
		for _, b := range other {
			if a.SourceRef == b.SourceRef {
				t.Fatal("account-scoped token ID leaked across accounts")
			}
		}
	}
}

func TestSub2TokenEvidenceRejectsPartialIdentityAndOverflow(t *testing.T) {
	account, base, day, key := sub2TokenFixture(t)
	for _, tt := range []struct {
		name string
		edit func([]sub2PricingUsageItem)
	}{
		{"missing", func(v []sub2PricingUsageItem) { v[1].UpstreamTokenID = ""; v[1].UpstreamTokenState = "missing" }},
		{"invalid", func(v []sub2PricingUsageItem) { v[1].UpstreamTokenState = "invalid" }},
		{"noncanonical", func(v []sub2PricingUsageItem) { v[1].UpstreamTokenID = "00574" }},
		{"duplicate", func(v []sub2PricingUsageItem) { v[1].ID = v[0].ID }},
		{"outside", func(v []sub2PricingUsageItem) { v[1].CreatedAt = day + 86400 }},
		{"negative", func(v []sub2PricingUsageItem) { v[1].ActualCostMicros = -1 }},
		{"inexact", func(v []sub2PricingUsageItem) { v[1].ActualCostKnown = false }},
		{"overflow", func(v []sub2PricingUsageItem) { v[1].ActualCostMicros = math.MaxInt64 }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			items := append([]sub2PricingUsageItem(nil), base...)
			tt.edit(items)
			rows, err := buildSub2TokenEvidence(account, items, day, day+86400, key, "v1")
			if err == nil || rows != nil {
				t.Fatal("bad row exposed partial money")
			}
		})
	}
}

func sub2TokenPage(items []sub2PricingUsageItem, number int) sub2PricingPage {
	return sub2PricingPage{Items: items, Total: 3, Page: number, PageSize: 2, Pages: 2}
}

func TestSub2TokenCheckpointResumeRetryAndLegacyIsolation(t *testing.T) {
	account, items, day, key := sub2TokenFixture(t)
	firstPage := sub2TokenPage(items[:2], 1)
	first, err := advanceSub2TokenCheckpoint(nil, account, firstPage, day, day+86400, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	retained := bytes.Clone(first)
	for i := 0; i < 3; i++ {
		retry, err := advanceSub2TokenCheckpoint(first, account, firstPage, day, day+86401, key, "v1")
		if err != nil || !bytes.Equal(first, retry) {
			t.Fatal("same page retry changed persisted money/cursor")
		}
	}
	// No in-memory state is needed to resume the next page.
	second, err := advanceSub2TokenCheckpoint(bytes.Clone(first), account, sub2TokenPage(items[2:], 2), day, day+86401, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	cp, err := decodeSub2TokenCheckpoint(second, account, day, key, "v1")
	if err != nil || cp.NextPage != 3 || len(cp.SeenRows) != 3 || len(cp.Rows) != 3 {
		t.Fatalf("resume: %+v %v", cp, err)
	}
	var total int64
	for _, row := range cp.Rows {
		total += row.ChargeUnits
	}
	if total != 481479 || !bytes.Equal(first, retained) {
		t.Fatal("resume duplicated charges or mutated old checkpoint")
	}
	// Old aggregate checkpoints cannot prove token ownership and are rejected.
	legacy, _ := json.Marshal(ChannelUpstreamPricingPageCheckpoint{Domain: account.Domain, Total: 3, NextPage: 2})
	if _, err := decodeSub2TokenCheckpoint(legacy, account, day, key, "v1"); err == nil {
		t.Fatal("legacy account checkpoint accepted as token checkpoint")
	}
	for _, name := range []string{"key", "key-id", "account", "day", "tamper"} {
		t.Run(name, func(t *testing.T) {
			a, d, k, id, raw := account, day, key, "v1", bytes.Clone(first)
			switch name {
			case "key":
				k = []byte("abcdef0123456789abcdef0123456789")
			case "key-id":
				id = "v2"
			case "account":
				a.Account = "other"
			case "day":
				d += 86400
			case "tamper":
				raw = bytes.Replace(raw, []byte(`"Total":3`), []byte(`"Total":4`), 1)
			}
			if _, err := decodeSub2TokenCheckpoint(raw, a, d, k, id); err == nil {
				t.Fatal("incompatible/tampered checkpoint accepted")
			}
		})
	}
}

func TestSub2TokenCheckpointRejectsChangedIdentityAndOverlappingPages(t *testing.T) {
	account, items, day, key := sub2TokenFixture(t)
	page := sub2TokenPage(items[:2], 1)
	first, err := advanceSub2TokenCheckpoint(nil, account, page, day, day+86400, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"changed-token", "changed-tokens", "overlap", "missing-id", "total-changed", "skip"} {
		t.Run(name, func(t *testing.T) {
			p := sub2TokenPage(append([]sub2PricingUsageItem(nil), items[2:]...), 2)
			prev := bytes.Clone(first)
			switch name {
			case "changed-token", "changed-tokens":
				p = sub2TokenPage(append([]sub2PricingUsageItem(nil), items[:2]...), 1)
				if name == "changed-token" {
					p.Items[0].UpstreamTokenID = "999"
				} else {
					p.Items[0].PromptTokens++
				}
			case "overlap":
				p.Items[0] = items[0]
			case "missing-id":
				p.Items[0].UpstreamTokenState = "missing"
			case "total-changed":
				p.Total = 4
			case "skip":
				prev = nil
			}
			out, err := advanceSub2TokenCheckpoint(prev, account, p, day, day+86401, key, "v1")
			if err == nil || out != nil || (prev != nil && !bytes.Equal(prev, first)) {
				t.Fatal("inconsistent page advanced partial money or cursor")
			}
		})
	}
}

func TestSub2TokenCheckpointEmptyAndBounded(t *testing.T) {
	account, _, day, key := sub2TokenFixture(t)
	page := sub2PricingPage{Page: 1, PageSize: 100}
	encoded, err := advanceSub2TokenCheckpoint(nil, account, page, day, day+86400, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	cp, err := decodeSub2TokenCheckpoint(encoded, account, day, key, "v1")
	if err != nil || len(cp.Rows) != 0 || cp.Total != 0 || cp.NextPage != 2 {
		t.Fatal("empty scan must be explicit and resumable", err)
	}
	page.Total = sub2TokenMaxScanRows + 1
	if _, err := advanceSub2TokenCheckpoint(nil, account, page, day, day+86400, key, "v1"); err == nil {
		t.Fatal("unbounded scan")
	}
	if _, err := decodeSub2TokenCheckpoint(make([]byte, upstreamPricingMaxCheckpointBytes+1), account, day, key, "v1"); err == nil {
		t.Fatal("unbounded decode")
	}
	account.Account = ""
	if _, err := buildSub2TokenEvidence(account, nil, day, day+86400, key, "v1"); err == nil {
		t.Fatal("unknown account identity")
	}
}

func TestSub2TokenCheckpointRejectsLogicallyInvalidSignedState(t *testing.T) {
	account, items, day, key := sub2TokenFixture(t)
	encoded, err := advanceSub2TokenCheckpoint(nil, account, sub2TokenPage(items[:2], 1), day, day+86400, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"requests", "money", "scope", "duplicate", "seen", "pages", "unit", "negative"} {
		t.Run(name, func(t *testing.T) {
			cp, err := decodeSub2TokenCheckpoint(encoded, account, day, key, "v1")
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "requests":
				cp.Rows[0].Requests++
			case "money":
				cp.Rows[0].ChargeUnits++
			case "scope":
				cp.Rows[0].AccountEpoch = "other"
			case "duplicate":
				cp.Rows = append(cp.Rows, cp.Rows[0])
			case "seen":
				cp.SeenRows = nil
			case "pages":
				cp.PageHashes[3] = cp.PageHashes[1]
				delete(cp.PageHashes, 1)
			case "unit":
				cp.Rows[0].ChargeUnitsPerUSD = "500000"
			case "negative":
				cp.Rows[0].ChargeUnits = -1
				cp.Rows[0].ContentHash = channelCostEvidenceRowHash(cp.Rows[0])
			}
			payload, _ := json.Marshal(cp)
			bad, _ := json.Marshal(sub2TokenCheckpointEnvelope{Payload: payload, MAC: sub2TokenCheckpointMAC(key, payload)})
			if _, err := decodeSub2TokenCheckpoint(bad, account, day, key, "v1"); err == nil {
				t.Fatal("invalid signed state accepted")
			}
		})
	}
}

func TestSub2TokenCheckpointMultiPageFileResumeConservesEveryHour(t *testing.T) {
	account, base, day, key := sub2TokenFixture(t)
	const pageSize, count = 7, 41
	items := make([]sub2PricingUsageItem, count)
	want := make(map[int64]int64)
	for i := range items {
		item := base[0]
		item.ID = int64(1000 + i)
		item.UpstreamTokenID = strconv.Itoa(574 + i%3)
		item.CreatedAt = day + int64(i%5)*3600 + int64(i)
		item.ActualCostMicros = int64(i%9) * 12345 // includes genuine zero charges
		items[i] = item
		want[item.CreatedAt-item.CreatedAt%3600] += item.ActualCostMicros
	}
	path := filepath.Join(t.TempDir(), "shadow-checkpoint.json")
	var previous []byte
	pages := sub2TokenPageCount(count, pageSize)
	for number := 1; number <= pages; number++ {
		end := number * pageSize
		if end > count {
			end = count
		}
		page := sub2PricingPage{Items: items[(number-1)*pageSize : end], Total: count, Page: number, PageSize: pageSize, Pages: pages}
		encoded, err := advanceSub2TokenCheckpoint(previous, account, page, day, day+86400, key, "v1")
		if err != nil {
			t.Fatal(err)
		}
		// A private synthetic file models loss of process memory, not a live DB.
		if err := os.WriteFile(path, encoded, 0600); err != nil {
			t.Fatal(err)
		}
		previous, err = os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		retry, err := advanceSub2TokenCheckpoint(previous, account, page, day, day+86401, key, "v1")
		if err != nil || !bytes.Equal(previous, retry) {
			t.Fatal("resumed retry changed checkpoint")
		}
	}
	cp, err := decodeSub2TokenCheckpoint(previous, account, day, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[int64]int64)
	var requests int64
	for _, row := range cp.Rows {
		got[row.HourTs] += row.ChargeUnits
		requests += row.Requests
	}
	if !reflect.DeepEqual(got, want) || requests != count || cp.NextPage != pages+1 {
		t.Fatal("multi-page resume/retry changed per-hour charges or request count")
	}
}
