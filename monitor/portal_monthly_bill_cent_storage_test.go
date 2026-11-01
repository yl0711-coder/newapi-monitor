package monitor

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func monthlyCentStorageFixture(t *testing.T) (monthlyStatement, monthlyBillCentMonth, PortalMonthlyBill) {
	t.Helper()
	p := monthlyCentStatementFixture(t, true)
	plan, err := monthlyPlanBillCentMonth(99, monthlyDigest([]byte("synthetic-origin")), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := monthlyEncodeCentBill(p, plan)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, len(p.Members))
	for i, member := range p.Members {
		ids[i] = member.UserID
	}
	return p, plan, PortalMonthlyBill{GroupID: 99, Month: p.Month, Payload: string(payload), PayloadHash: monthlyDigest(payload),
		MemberFingerprint: portalMemberFingerprintFromIDs(ids), MemberManifest: monthlyDigest([]byte("synthetic-manifest")),
		PublicationID: monthlyCentBillPublicationPrefix + "synthetic-issuance"}
}

func monthlyCentStorageReplacePlan(t *testing.T, record *PortalMonthlyBill, rawPlan []byte) {
	t.Helper()
	var envelope monthlyCentBillEnvelope
	if json.Unmarshal([]byte(record.Payload), &envelope) != nil {
		t.Fatal("invalid test envelope")
	}
	var compressed bytes.Buffer
	w := gzip.NewWriter(&compressed)
	if _, err := w.Write(rawPlan); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	envelope.Presentation = base64.RawStdEncoding.EncodeToString(compressed.Bytes())
	monthlyCentStorageReplaceEnvelope(t, record, envelope)
}

func monthlyCentStorageReplaceEnvelope(t *testing.T, record *PortalMonthlyBill, envelope monthlyCentBillEnvelope) {
	t.Helper()
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	record.Payload, record.PayloadHash = string(raw), monthlyDigest(raw)
}

func TestMonthlyBillCentStorageRoundTripAndSQLiteRestart(t *testing.T) {
	p, plan, record := monthlyCentStorageFixture(t)
	before, _ := json.Marshal(p)
	again, err := monthlyEncodeCentBill(p, plan)
	if err != nil || record.Payload != string(again) {
		t.Fatal("nondeterministic encoding", err)
	}
	path := filepath.Join(t.TempDir(), "synthetic-bills.db")
	open := func() *gorm.DB {
		db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
		if err != nil {
			t.Fatal(err)
		}
		sqlDB, err := db.DB()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = sqlDB.Close() })
		return db
	}
	db := open()
	if err := db.AutoMigrate(&PortalMonthlyBill{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&record).Error; err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open()
	var saved PortalMonthlyBill
	if err := reopened.First(&saved, "group_id = ? AND month = ?", record.GroupID, record.Month).Error; err != nil {
		t.Fatal(err)
	}
	retained, err := monthlyReadCentBill(saved)
	retainedSource, _ := json.Marshal(retained.Source)
	if err != nil || !bytes.Equal(before, retainedSource) || !reflect.DeepEqual(plan, retained.Plan) {
		t.Fatal("saved money changed after restart", err)
	}
	if saved.Payload != record.Payload || saved.PayloadHash != record.PayloadHash {
		t.Fatal("reading changed the stored payload")
	}
	if _, err := readLegacyMonthlyBill(saved); err == nil {
		t.Fatal("legacy reader silently served different cent values for a new-format bill")
	}
	after, _ := json.Marshal(p)
	if !bytes.Equal(before, after) {
		t.Fatal("encoding changed original source money")
	}
}

func TestMonthlyBillCentStorageRejectsDamageAndForeignOwnership(t *testing.T) {
	_, plan, base := monthlyCentStorageFixture(t)
	for name, mutate := range map[string]func(*PortalMonthlyBill){
		"checksum":       func(r *PortalMonthlyBill) { r.PayloadHash = monthlyDigest([]byte("wrong")) },
		"owner":          func(r *PortalMonthlyBill) { r.GroupID++ },
		"month":          func(r *PortalMonthlyBill) { r.Month = "2026-08" },
		"fingerprint":    func(r *PortalMonthlyBill) { r.MemberFingerprint = monthlyDigest([]byte("other users")) },
		"manifest":       func(r *PortalMonthlyBill) { r.MemberManifest = "" },
		"downgrade":      func(r *PortalMonthlyBill) { r.PublicationID = monthlyFrozenPublicationPrefix + "old" },
		"empty_issuance": func(r *PortalMonthlyBill) { r.PublicationID = monthlyCentBillPublicationPrefix },
		"too_large": func(r *PortalMonthlyBill) {
			r.Payload = strings.Repeat(" ", monthlyCentBillPayloadLimit+1)
			r.PayloadHash = monthlyDigest([]byte(r.Payload))
		},
		"wrong_total_with_valid_hash": func(r *PortalMonthlyBill) {
			p := plan
			p.Organization.Closing = "1000.000000"
			p.Digest = monthlyBillCentMonthDigest(p)
			raw, _ := json.Marshal(p)
			monthlyCentStorageReplacePlan(t, r, raw)
		},
		"uncompressed_bomb": func(r *PortalMonthlyBill) {
			monthlyCentStorageReplacePlan(t, r, bytes.Repeat([]byte(" "), monthlyCentBillPayloadLimit+1))
		},
		"duplicate_plan_field": func(r *PortalMonthlyBill) {
			raw, _ := json.Marshal(plan)
			raw = append([]byte(`{"version":99,`), raw[1:]...)
			monthlyCentStorageReplacePlan(t, r, raw)
		},
		"unknown_plan_field": func(r *PortalMonthlyBill) {
			raw, _ := json.Marshal(plan)
			raw = append([]byte(`{"unexpected":1,`), raw[1:]...)
			monthlyCentStorageReplacePlan(t, r, raw)
		},
	} {
		t.Run(name, func(t *testing.T) {
			record := base
			mutate(&record)
			if _, err := monthlyReadCentBill(record); err == nil {
				t.Fatal("damaged/foreign bill accepted")
			}
		})
	}
	for _, name := range []string{"format", "base64", "crc", "second_stream", "trailing_bytes", "unknown_envelope", "duplicate_envelope"} {
		t.Run(name, func(t *testing.T) {
			record := base
			var envelope monthlyCentBillEnvelope
			if json.Unmarshal([]byte(record.Payload), &envelope) != nil {
				t.Fatal("invalid test envelope")
			}
			compressed, err := base64.RawStdEncoding.DecodeString(envelope.Presentation)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "format":
				envelope.Format = "unsupported"
			case "base64":
				envelope.Presentation = "@invalid"
			case "crc":
				compressed[len(compressed)-1] ^= 1
				envelope.Presentation = base64.RawStdEncoding.EncodeToString(compressed)
			case "second_stream":
				envelope.Presentation = base64.RawStdEncoding.EncodeToString(append(compressed, compressed...))
			case "trailing_bytes":
				envelope.Presentation = base64.RawStdEncoding.EncodeToString(append(compressed, 0))
			}
			monthlyCentStorageReplaceEnvelope(t, &record, envelope)
			if name == "unknown_envelope" || name == "duplicate_envelope" {
				prefix := `{"unexpected":1,`
				if name == "duplicate_envelope" {
					prefix = `{"format":"unsupported",`
				}
				record.Payload = prefix + record.Payload[1:]
				record.PayloadHash = monthlyDigest([]byte(record.Payload))
			}
			if _, err := monthlyReadCentBill(record); err == nil {
				t.Fatal("bad envelope/compressed plan accepted")
			}
		})
	}
}

func TestMonthlyBillCentValidationRejectsRehashedMoneyAlterations(t *testing.T) {
	p, plan, _ := monthlyCentStorageFixture(t)
	for name, mutate := range map[string]func(*monthlyBillCentMonth){
		"version":             func(p *monthlyBillCentMonth) { p.Version++ },
		"source":              func(p *monthlyBillCentMonth) { p.SourceSHA256 = monthlyDigest([]byte("other")) },
		"consumption_version": func(p *monthlyBillCentMonth) { p.Consumption.Version++ },
		"consumption_source":  func(p *monthlyBillCentMonth) { p.Consumption.SourceSHA256 = monthlyDigest([]byte("other")) },
		"root_charge":         func(p *monthlyBillCentMonth) { p.Consumption.Organization.Amount = "0.020000" },
		"account_owner":       func(p *monthlyBillCentMonth) { p.Members[0].UserID++ },
		"account_consumption": func(p *monthlyBillCentMonth) { p.Consumption.Members[0].Consumption.Amount = "0.020000" },
		"token_owner":         func(p *monthlyBillCentMonth) { p.Consumption.Members[0].Tokens[0].ID++ },
		"token_charge":        func(p *monthlyBillCentMonth) { p.Consumption.Members[0].Tokens[0].Amount = "0.020000" },
		"missing_token":       func(p *monthlyBillCentMonth) { p.Consumption.Members[0].Tokens = nil },
		"account_opening":     func(p *monthlyBillCentMonth) { p.Members[0].Wallet.Opening = "999.000000" },
		"source_opening":      func(p *monthlyBillCentMonth) { p.Members[0].Wallet.SourceOpening = "999.000000" },
		"gift":                func(p *monthlyBillCentMonth) { p.Members[0].Wallet.Gift = "0.010000" },
		"credit_total":        func(p *monthlyBillCentMonth) { p.Members[0].Wallet.CreditTotal = "0.010000" },
		"source_closing":      func(p *monthlyBillCentMonth) { p.Organization.SourceClosing = "99.000000" },
	} {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(plan)
			var changed monthlyBillCentMonth
			if json.Unmarshal(raw, &changed) != nil {
				t.Fatal("invalid synthetic fixture")
			}
			mutate(&changed)
			changed.Digest = monthlyBillCentMonthDigest(changed)
			if monthlyBillCentMatchesSource(p, changed) {
				t.Fatal("rehash accepted inconsistent source/ownership/money")
			}
		})
	}
}

func TestMonthlyBillCentStorageCapacity(t *testing.T) {
	p := monthlyCentCapacitySource(t)
	plan, err := monthlyPlanBillCentMonth(99, monthlyDigest([]byte("synthetic-origin")), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(p)
	payload, err := monthlyEncodeCentBill(p, plan)
	if err != nil || len(payload) > monthlyCentBillPayloadLimit {
		t.Fatal("full hierarchy did not fit existing payload budget", len(payload), err)
	}
	ids := make([]int64, len(p.Members))
	for i, m := range p.Members {
		ids[i] = m.UserID
	}
	record := PortalMonthlyBill{GroupID: 99, Month: p.Month, Payload: string(payload), PayloadHash: monthlyDigest(payload),
		MemberFingerprint: portalMemberFingerprintFromIDs(ids), MemberManifest: monthlyDigest([]byte("synthetic-manifest")),
		PublicationID: monthlyCentBillPublicationPrefix + "synthetic-capacity"}
	retained, err := monthlyReadCentBill(record)
	if err != nil || !reflect.DeepEqual(plan, retained.Plan) {
		t.Fatal("capacity bill failed to retain original display plan", err)
	}
	view, err := monthlyCentBillView(retained)
	if err != nil {
		t.Fatal(err)
	}
	rows := 0
	for _, m := range view.Members {
		rows += len(m.TokenConsumption)
	}
	if rows != monthlyTokenBillLimit {
		t.Fatal("capacity view lost token rows", rows)
	}
	after, _ := json.Marshal(p)
	if !bytes.Equal(before, after) {
		t.Fatal("capacity encoding rewrote source")
	}
	t.Logf("source=%d bytes, retained=%d bytes, token rows=%d", len(before), len(payload), rows)
}
