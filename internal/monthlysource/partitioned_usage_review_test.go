package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

// Synthetic original USER projections, not source SQL or raw token evidence.
// Bulk insertion is setup only; production review rereads every stored row.
func partitionedUsageFixture(t *testing.T, users int) (*sql.DB, string, CapturedMonthSelection, []byte) {
	t.Helper()
	db, path := usageTestStore(t)
	raw := capturedPartitionFixture(t, users)
	var s CapturedMonthSelection
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(t.Context(), `INSERT INTO portal_monthly_usage_observations VALUES(?,?,?,?,?,?,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	start, _ := time.ParseInLocation("2006-01", s.Scope.Funding.Month, fundingMonthZone)
	for i := range s.Tokens {
		a := &s.Tokens[i]
		for day := range a.Usage {
			at := start.AddDate(0, 0, day).Unix()
			consume := usageMonthRow(a.UserID, at+1, 2, "wallet", 9007199254740993+a.UserID)
			refund := usageMonthRow(a.UserID, at+2, 6, "wallet", a.UserID+int64(day))
			consume.MinID, consume.MaxID = a.UserID*1000+int64(day)*2, a.UserID*1000+int64(day)*2
			refund.MinID, refund.MaxID = consume.MinID+1, consume.MaxID+1
			o := usageMonthCapture(s.Scope.Funding, a.UserID, at, at+86400, []UsageWindowRow{consume, refund})
			v := UsageWindowVersion{}
			for phase, observation := range []UsageWindowObservation{o, usageMonthNewer(o)} {
				body, err := usageWindowBytes(observation)
				if err != nil {
					t.Fatal(err)
				}
				id := digest(body)
				if _, err := stmt.ExecContext(t.Context(), id, observation.Request.SourceIdentity, a.UserID, at, at+86400, observation.StartedAtMS, observation.CompletedAtMS, body); err != nil {
					t.Fatal(err)
				}
				if phase == 0 {
					v.OlderID = id
				} else {
					v.NewerID = id
				}
			}
			a.Usage[day] = v
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	raw, err = json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return db, path, s, raw
}

func reviewPartitionFixture(t *testing.T, db *sql.DB, s CapturedMonthSelection, raw []byte) (UsageMonthObservation, error) {
	t.Helper()
	a, err := PartitionCapturedUsageIndex(t.Context(), raw, time.Now())
	if err != nil {
		return UsageMonthObservation{}, err
	}
	return ReviewPartitionedCapturedUsage(t.Context(), db, digest(raw), s.Scope, a)
}

func TestPartitionedUsageReviewLargeMonthExactMoneyAndRestart(t *testing.T) {
	db, path, s, raw := partitionedUsageFixture(t, 200)
	versions := []UsageWindowVersion{}
	for _, a := range s.Tokens {
		versions = append(versions, a.Usage...)
	}
	if _, err := ReviewUsageMonth(t.Context(), db, s.Scope.Funding, versions); err == nil {
		t.Fatal("legacy 4096 window limit silently widened")
	}
	changes := adaptiveTokenCount(t, db, `SELECT total_changes()`)
	started := time.Now()
	o, err := reviewPartitionFixture(t, db, s, raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("6200 original projection pairs audited in", time.Since(started))
	r := o.Report()
	if len(r.Accounts) != 200 {
		t.Fatal("partial organization returned", len(r.Accounts))
	}
	for i, a := range r.Accounts {
		id := s.Scope.Funding.UserIDs[i]
		if a.UserID != id || a.Totals == nil || len(a.Totals.Sources) != 1 || len(a.Windows) != 31 || a.MatchingSeconds != r.To-r.From || a.ObservedSeconds != r.To-r.From {
			t.Fatal("partial or mixed account", id)
		}
		money := a.Totals.Sources[0]
		if money.ConsumeQuota != 31*(9007199254740993+id) || money.RefundQuota != 31*id+465 || money.ConsumeRecords != 31 || money.RefundRecords != 31 {
			t.Fatal("lost integer precision/amounts", id, money)
		}
	}
	if adaptiveTokenCount(t, db, `SELECT total_changes()`) != changes || db.Stats().InUse != 0 {
		t.Fatal("review wrote source or retained a transaction")
	}
	if _, err := json.Marshal(o); err == nil {
		t.Fatal("review serialized as approval")
	}
	o.Report().Accounts[0].Totals.Sources[0].ConsumeQuota = 0
	if o.Report().Accounts[0].Totals.Sources[0].ConsumeQuota == 0 {
		t.Fatal("report mutation changed audit state")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=query_only(1)")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.SetMaxOpenConns(1)
	again, err := reviewPartitionFixture(t, reopened, s, raw)
	if err != nil || !reflect.DeepEqual(r, again.Report()) {
		t.Fatal("read-only restart changed original amounts", err)
	}
}

func TestPartitionedUsageReviewMatchesLegacyReport(t *testing.T) {
	db, _, s, raw := partitionedUsageFixture(t, 2)
	versions := append(append([]UsageWindowVersion{}, s.Tokens[0].Usage...), s.Tokens[1].Usage...)
	old, err := ReviewUsageMonth(t.Context(), db, s.Scope.Funding, versions)
	if err != nil {
		t.Fatal(err)
	}
	new, err := reviewPartitionFixture(t, db, s, raw)
	if err != nil || !reflect.DeepEqual(old.Report(), new.Report()) {
		t.Fatal("new path changed audit/format", err)
	}
}

func TestPartitionedUsageReviewRejectsWrongMoneyCoverageAndScope(t *testing.T) {
	for _, kind := range []string{"opposite_changes", "gap", "overlap", "foreign_user", "missing_account", "overflow", "missing_original", "corrupt_original"} {
		t.Run(kind, func(t *testing.T) {
			db, _, s, _ := partitionedUsageFixture(t, 2)
			for i := 0; i < 2; i++ {
				v := &s.Tokens[i].Usage[0]
				o, err := LoadUsageWindowObservation(t.Context(), db, v.OlderID)
				if err != nil {
					t.Fatal(err)
				}
				n := usageMonthNewer(o)
				switch kind {
				case "opposite_changes":
					delta := int64(1 - i*2)
					n.Rows[0].Quota += delta
					n.Rows[0].MinQuota += delta
					n.Rows[0].MaxQuota += delta
				case "gap":
					o.Request.From += 1
					n.Request.From += 1
				case "overlap":
					v = &s.Tokens[i].Usage[1]
					o.Request.From += 3600
					n.Request.From += 3600
					o.Rows, n.Rows = []UsageWindowRow{}, []UsageWindowRow{}
				case "foreign_user":
					o.Request.UserID, o.Wallet.UserID = 999, 999
					n.Request.UserID, n.Wallet.UserID = 999, 999
					for j := range o.Rows {
						o.Rows[j].UserID, n.Rows[j].UserID = 999, 999
					}
				case "missing_account":
					o.Wallet.Exists, n.Wallet.Exists = false, false
					o.Wallet.BalanceQuota, o.Wallet.UsedQuota = nil, nil
					n.Wallet.BalanceQuota, n.Wallet.UsedQuota = nil, nil
				case "overflow":
					o.Rows[0].Quota, o.Rows[0].MinQuota, o.Rows[0].MaxQuota = math.MaxInt64, math.MaxInt64, math.MaxInt64
					n.Rows[0].Quota, n.Rows[0].MinQuota, n.Rows[0].MaxQuota = math.MaxInt64, math.MaxInt64, math.MaxInt64
				case "missing_original", "corrupt_original":
					if _, err := db.Exec(`DROP TRIGGER monthly_usage_no_delete; DROP TRIGGER monthly_usage_no_update`); err != nil {
						t.Fatal(err)
					}
					query := `DELETE FROM portal_monthly_usage_observations WHERE id=?`
					if kind == "corrupt_original" {
						query = `UPDATE portal_monthly_usage_observations SET payload='{}' WHERE id=?`
					}
					if _, err := db.Exec(query, v.NewerID); err != nil {
						t.Fatal(err)
					}
				}
				if kind == "missing_original" || kind == "corrupt_original" {
					break
				}
				*v = usageMonthSavePair(t, db, o, n)
			}
			raw, _ := json.Marshal(s)
			o, err := reviewPartitionFixture(t, db, s, raw)
			if err == nil || len(o.Report().Accounts) != 0 || db.Stats().InUse != 0 {
				t.Fatal("bad source returned partial usable result", kind, err)
			}
		})
	}
}

func TestPartitionedUsageReviewRejectsIdentityMissingPartBusyAndCancellation(t *testing.T) {
	db, _, s, raw := partitionedUsageFixture(t, 2)
	for _, kind := range []string{"hash", "scope", "missing", "busy", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			a, err := PartitionCapturedUsageIndex(t.Context(), raw, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			expected, scope, ctx := digest(raw), s.Scope, t.Context()
			switch kind {
			case "hash":
				expected = digest([]byte("other original"))
			case "scope":
				scope.AuthVersion++
			case "missing":
				for key := range a.Parts {
					delete(a.Parts, key)
					break
				}
			case "busy":
				usageMonthSlots <- struct{}{}
				defer func() { <-usageMonthSlots }()
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			o, err := ReviewPartitionedCapturedUsage(ctx, db, expected, scope, a)
			if err == nil || len(o.Report().Accounts) != 0 || db.Stats().InUse != 0 {
				t.Fatal("invalid request returned proof or leaked connection", kind, err)
			}
			if kind == "busy" && !errors.Is(err, ErrUsageMonthBusy) {
				t.Fatal("busy classification lost", err)
			}
		})
	}
}

func TestPartitionedUsageReviewFullSelectionStillAuditsFundingWalletAndShape(t *testing.T) {
	db, _, key, scope := capturedSelectionFixture(t, false)
	assembled, err := BuildCapturedMonthSelection(t.Context(), db, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"valid", "wallet", "funding", "reordered_v2", "bad_shape"} {
		t.Run(kind, func(t *testing.T) {
			var copy CapturedMonthSelection
			if err := json.Unmarshal(assembled.Selection, &copy); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "wallet":
				copy.WalletIDs[0] = digest([]byte("missing wallet"))
			case "funding":
				copy.Funding[0].OlderScanID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			case "reordered_v2":
				copy.Version, copy.Mode = 2, RecoveredCapturedMonthSelectionMode
				copy.Tokens[0].Usage[1], copy.Tokens[0].Usage[0] = copy.Tokens[0].Usage[0], copy.Tokens[0].Usage[1]
			case "bad_shape":
				copy.Version, copy.Mode = 2, RecoveredCapturedMonthSelectionMode
				a, err := LoadUsageWindowObservation(t.Context(), db, copy.Tokens[0].Usage[0].OlderID)
				if err != nil {
					t.Fatal(err)
				}
				end := a.Request.To
				a.Rows = []UsageWindowRow{}
				a.Request.To = a.Request.From + 8*3600
				first := usageMonthSavePair(t, db, a, usageMonthNewer(a))
				a.Request.From, a.Request.To = a.Request.To, end
				second := usageMonthSavePair(t, db, a, usageMonthNewer(a))
				copy.Tokens[0].Usage = append([]UsageWindowVersion{first, second}, copy.Tokens[0].Usage[1:]...)
				copy.Tokens[0].Parents = append(copy.Tokens[0].Parents, digest([]byte("extra parent a")), digest([]byte("extra parent b")))
			}
			raw, _ := json.Marshal(copy)
			a, err := PartitionCapturedUsageIndex(t.Context(), raw, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if kind == "bad_shape" {
				if _, err := ReviewPartitionedCapturedUsage(t.Context(), db, digest(raw), scope, a); err != nil {
					t.Fatal("shape fixture must otherwise have complete matching usage", err)
				}
			}
			err = VerifyPartitionedCapturedObservationSelection(t.Context(), db, digest(raw), scope, a)
			if (kind == "valid" || kind == "reordered_v2") && err != nil {
				// Reordering complete windows is valid, just as on the legacy path.
				t.Fatal("unchanged originals rejected", err)
			}
			if kind != "valid" && kind != "reordered_v2" && err == nil {
				t.Fatal("usage identity bypassed wallet/funding/shape audit")
			}
		})
	}
}

func TestPartitionedUsageReviewSharedSnapshotAndCumulativeBytes(t *testing.T) {
	db, path, s, _ := partitionedUsageFixture(t, 2)
	if _, err := db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	writer, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	start, _ := time.ParseInLocation("2006-01", s.Scope.Funding.Month, fundingMonthZone)
	r := UsageMonthReport{From: start.Unix(), To: start.AddDate(0, 1, 0).Unix(), Accounts: []UsageMonthAccountReport{{UserID: 201}, {UserID: 202}}}
	used := 0
	if err := appendUsageMonthWindows(t.Context(), tx, s.Scope.Funding, s.Tokens[0].Usage, time.Now(), &r, &used); err != nil {
		t.Fatal(err)
	}
	firstBytes := used
	// A competing synthetic writer damages a later account AFTER this audit
	// has read its first account. All parts must stay on the first snapshot.
	if _, err := writer.Exec(`DROP TRIGGER monthly_usage_no_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Exec(`UPDATE portal_monthly_usage_observations SET payload='{}' WHERE id=?`, s.Tokens[1].Usage[0].NewerID); err != nil {
		t.Fatal(err)
	}
	if err := appendUsageMonthWindows(t.Context(), tx, s.Scope.Funding, s.Tokens[1].Usage, time.Now(), &r, &used); err != nil || used <= firstBytes {
		t.Fatal("parts changed database view or reset byte counter", err)
	}
	used = usageMonthBytesLimit - 1
	if err := appendUsageMonthWindows(t.Context(), tx, s.Scope.Funding, s.Tokens[1].Usage[:1], time.Now(), &r, &used); err == nil {
		t.Fatal("next part received a fresh global byte budget")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadUsageWindowObservation(t.Context(), db, s.Tokens[1].Usage[0].NewerID); err == nil {
		t.Fatal("competing mutation was not committed; snapshot test invalid")
	}
}
