package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Sparse old slots represent a day whose source query really failed. These
// direct writes SET UP synthetic retained data only; production code must not
// delete/fill immutable slots or infer original observations from aggregates.
func syntheticRecoveredSlot(t *testing.T, db *sql.DB, plan FundingMonthCapturePlan, ordinal int, o TokenUsageWindowObservation) {
	t.Helper()
	want, err := tokenCaptureRequest(plan, ordinal)
	if err != nil || want != o.Request || ValidateTokenUsageWindowObservation(o) != nil {
		t.Fatal("invalid retained fixture", err)
	}
	u, err := UsageObservationFromToken(o)
	if err != nil {
		t.Fatal(err)
	}
	projection, err := SaveUsageWindowObservation(t.Context(), db, u)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(o)
	if _, err := db.Exec(`INSERT INTO portal_monthly_token_capture_slots(job_key,ordinal,user_id,sha256,usage_id,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?,?,?,?)`, plan.Key, ordinal, o.Request.UserID, digest(raw), projection, o.StartedAtMS, o.CompletedAtMS, raw); err != nil {
		t.Fatal(err)
	}
}

func recoveredMonthFixture(t *testing.T, parentFirst bool, deep bool, extraMember ...bool) (*adaptiveTokenFixture, OrganizationMonthScope, FundingMonthCapturePlan, []TokenUsageWindowPair) {
	t.Helper()
	f := newAdaptiveTokenFixture(t)
	db, path, key, scope, plan := tokenCaptureFixture(t, f.root.SourceIdentity)
	if len(extraMember) != 0 && extraMember[0] {
		scope.Funding.UserIDs = []int64{201, 202}
		scope.MemberFingerprint = digest([]byte("201,202,"))[:32]
		key = digest([]byte("synthetic recovered multi-account month"))
		var err error
		plan, err = EnsureFundingMonthCapture(t.Context(), db, key, scope)
		if err != nil {
			t.Fatal(err)
		}
	}
	f.store, f.path, f.job = db, path, key
	if err := InitAdaptiveTokenCaptureStore(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if err := InitWalletMonthCaptureStore(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	fillCapturedSelectionFundsAndWallets(t, db, scope, plan, false)
	_, original, _ := tokenMonthFixture()
	for day := 1; day < 30; day++ {
		for phase, o := range []TokenUsageWindowObservation{original[day].Older, original[day].Newer} {
			o.Request.SourceIdentity = f.root.SourceIdentity
			balance, used := int64(0), int64(0)
			o.Wallet = WalletUser{UserID: o.Request.UserID, Exists: true, BalanceQuota: &balance, UsedQuota: &used}
			if phase == 0 {
				original[day].Older = o
			} else {
				original[day].Newer = o
			}
			syntheticRecoveredSlot(t, db, plan, (phase*30+day)*len(scope.Funding.UserIDs), o)
		}
	}
	if len(scope.Funding.UserIDs) == 2 {
		_, other, _ := tokenMonthFixture()
		for day, pair := range other {
			for phase, o := range []TokenUsageWindowObservation{pair.Older, pair.Newer} {
				o.Request.UserID, o.Request.SourceIdentity = 202, f.root.SourceIdentity
				balance, used := int64(3), int64(5)
				o.Wallet = WalletUser{UserID: 202, Exists: true, BalanceQuota: &balance, UsedQuota: &used}
				change := func(r *UsageWindowRow) {
					r.UserID = 202
					r.Quota, r.MinQuota, r.MaxQuota = r.Quota*2, r.MinQuota*2, r.MaxQuota*2
					r.MinID, r.MaxID = r.MinID+1000000, r.MaxID+1000000
				}
				for i := range o.Totals {
					change(&o.Totals[i])
				}
				for i := range o.Rows {
					change(&o.Rows[i].UsageWindowRow)
					if o.Rows[i].TokenID > 0 {
						o.Rows[i].TokenID += 100
					}
				}
				syntheticRecoveredSlot(t, db, plan, (phase*30+day)*2+1, o)
			}
		}
	}
	if parentFirst {
		o, err := f.ops.capture(t.Context(), f.source, f.root)
		if err != nil {
			t.Fatal(err)
		}
		syntheticRecoveredSlot(t, db, plan, 0, o)
	}
	f.fail = func(r UsageWindowRequest) error {
		limit := int64(43200)
		if deep {
			limit = 5400
		}
		if r.To-r.From > limit {
			return &mysql.MySQLError{Number: 3024, Message: "synthetic indexed aggregate deadline"}
		}
		return nil
	}
	finishAdaptiveFixture(t, f)
	pairs, err := LoadAdaptiveTokenWindowPairs(t.Context(), db, key, f.root)
	if err != nil {
		t.Fatal(err)
	}
	return f, scope, plan, append(pairs, original[1:]...)
}

func TestRecoveredTokenMonthReusesOriginalsAndSealsExactChildPairs(t *testing.T) {
	for _, parentFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "first_sweep_failed", true: "second_sweep_failed"}[parentFirst], func(t *testing.T) {
			f, scope, _, expected := recoveredMonthFixture(t, parentFirst, true)
			before, err := os.ReadFile(f.path)
			if err != nil {
				t.Fatal(err)
			}
			queries := f.queries()
			if old, err := BuildCapturedMonthSelection(t.Context(), f.store, f.job, scope); err == nil || len(old.Selection) != 0 {
				t.Fatal("legacy reader accepted sparse originals", err)
			}
			pairs, report, err := LoadRecoveredTokenMonth(t.Context(), f.store, f.job, scope, f.root.UserID)
			want, wantErr := SummarizeTokenUsageMonth(t.Context(), scope.Funding, expected, time.Now())
			if err != nil || wantErr != nil || !reflect.DeepEqual(pairs, expected) || !reflect.DeepEqual(report, want) || report.ConsumeQuota != 511609 || report.RefundQuota != 2 {
				t.Fatal("month changed charges/refunds or original records", report, want, err, wantErr)
			}
			assembled, err := BuildRecoveredCapturedMonthSelection(t.Context(), f.store, f.job, scope)
			if err != nil {
				t.Fatal(err)
			}
			var selection CapturedMonthSelection
			if json.Unmarshal(assembled.Selection, &selection) != nil || selection.Version != 2 || selection.Mode != RecoveredCapturedMonthSelectionMode || len(selection.Tokens[0].Usage) != 45 || len(selection.Tokens[0].Parents) != 90 {
				t.Fatal("missing explicit bounded child representation", selection)
			}
			archive, got, err := DecodeTokenUsageMonthArchive(t.Context(), assembled.Tokens[0].Raw, time.Now())
			if err != nil || !reflect.DeepEqual(archive.Pairs, expected) || !reflect.DeepEqual(got, want) || archive.SelectionSHA256 != digest(assembled.Selection) {
				t.Fatal("archive no longer contains exact originals", err)
			}
			destination, path := emptyCapturedCopy(t)
			copied, err := CopyRecoveredCapturedMonthObservationsBounded(t.Context(), f.store, destination, f.job, scope, 64<<20)
			if err != nil || !reflect.DeepEqual(copied, assembled) {
				t.Fatal("recovered handoff copy failed", err)
			}
			var controls, count int
			if err := destination.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name LIKE 'monthly_adaptive_%' OR name='portal_monthly_token_capture_slots'`).Scan(&controls); err != nil || controls != 0 {
				t.Fatal("scheduler/private state leaked into sealed source", controls, err)
			}
			if err := destination.QueryRow(`SELECT count(*) FROM portal_monthly_usage_observations`).Scan(&count); err != nil || count != 90 {
				t.Fatal("selected discarded parent or lost a child", count, err)
			}
			if err := destination.Close(); err != nil {
				t.Fatal(err)
			}
			ro, err := sql.Open("sqlite", "file:"+path+"?mode=ro&immutable=1&_pragma=query_only(1)")
			if err != nil {
				t.Fatal(err)
			}
			defer ro.Close()
			ro.SetMaxOpenConns(1)
			if err := VerifyCapturedObservationSelection(t.Context(), ro, assembled.Selection); err != nil {
				t.Fatal("sealed recovered selection did not survive reopen", err)
			}
			selection.Version, selection.Mode = 1, CapturedMonthSelectionMode
			downgraded, _ := json.Marshal(selection)
			if err := VerifyCapturedObservationSelection(t.Context(), ro, downgraded); err == nil {
				t.Fatal("child windows bypassed legacy 31-window limit")
			}
			if err := f.store.Close(); err != nil {
				t.Fatal(err)
			}
			captureRO, err := sql.Open("sqlite", "file:"+f.path+"?mode=ro&immutable=1&_pragma=query_only(1)")
			if err != nil {
				t.Fatal(err)
			}
			defer captureRO.Close()
			captureRO.SetMaxOpenConns(1)
			reopened, err := BuildRecoveredCapturedMonthSelection(t.Context(), captureRO, f.job, scope)
			if err != nil || !reflect.DeepEqual(reopened, assembled) {
				t.Fatal("restart/read-only capture changed the selection", err)
			}
			after, err := os.ReadFile(f.path)
			if err != nil || !reflect.DeepEqual(before, after) || f.queries() != queries {
				t.Fatal("assembly/copy queried source or modified original capture store", err)
			}
		})
	}
}

func TestRecoveredWindowShapeCalendarAndMaximumLeafBudget(t *testing.T) {
	for _, month := range []string{"2026-02", "2024-02", "2026-09", "2026-05"} {
		t.Run(month, func(t *testing.T) {
			db, _ := walletTestStore(t)
			if err := InitUsageWindowStore(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			r, original, _ := tokenMonthFixture()
			r.Month = month
			start, err := time.ParseInLocation("2006-01", month, fundingMonthZone)
			if err != nil {
				t.Fatal(err)
			}
			versions := []UsageWindowVersion{}
			for day := start.Unix(); day < start.AddDate(0, 1, 0).Unix(); day += 86400 {
				// Full sixteen-leaf days, including explicitly empty leaves.
				for from := day; from < day+86400; from += 5400 {
					var v UsageWindowVersion
					for phase, o := range []TokenUsageWindowObservation{original[10].Older, original[10].Newer} {
						o.Request.From, o.Request.To = from, from+5400
						u, err := UsageObservationFromToken(o)
						if err != nil {
							t.Fatal(err)
						}
						id, err := SaveUsageWindowObservation(t.Context(), db, u)
						if err != nil {
							t.Fatal(err)
						}
						if phase == 0 {
							v.OlderID = id
						} else {
							v.NewerID = id
						}
					}
					versions = append(versions, v)
				}
			}
			if err := verifyRecoveredWindowShape(t.Context(), db, r, versions); err != nil {
				t.Fatal("valid 28/29/30/31-day tree rejected", err)
			}
			if err := verifyRecoveredWindowShape(t.Context(), db, r, append(versions, versions[0])); err == nil {
				t.Fatal("overlapping/excess leaf accepted")
			}
		})
	}
}

func TestRecoveredTokenMonthLegacyOnlyHasIdenticalBytesAndNoSchemaWrites(t *testing.T) {
	db, path, key, scope := capturedSelectionFixture(t, false)
	want, err := BuildCapturedMonthSelection(t.Context(), db, key, scope)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := BuildRecoveredCapturedMonthSelection(t.Context(), db, key, scope)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("old source representation changed", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("read-only fallback initialized a schema", err)
	}
}

func TestRecoveredTokenMonthCannotRelaxLegacyWholeSweepSeparation(t *testing.T) {
	db, _, key, scope := capturedSelectionFixture(t, false)
	plan, err := loadFundingMonthCapturePlan(t.Context(), db, key, scope, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	newer := []TokenUsageWindowObservation{}
	for ordinal := 30; ordinal < 60; ordinal++ {
		o, err := loadTokenCaptureSlot(t.Context(), db, plan, ordinal)
		if err != nil {
			t.Fatal(err)
		}
		o.StartedAtMS -= 2000
		o.CompletedAtMS -= 2000
		newer = append(newer, o)
	}
	// Synthetic records remain individually >five minutes apart, but the
	// second whole sweep starts <five minutes after the first whole sweep.
	if _, err := db.Exec(`DROP TRIGGER monthly_token_capture_no_delete; DELETE FROM portal_monthly_token_capture_slots WHERE ordinal>=30`); err != nil {
		t.Fatal(err)
	}
	for i, o := range newer {
		syntheticRecoveredSlot(t, db, plan, 30+i, o)
	}
	if _, _, err := LoadCapturedTokenMonth(t.Context(), db, key, scope, 201); err != nil {
		t.Fatal("test does not isolate the whole-sweep guard", err)
	}
	if _, err := BuildCapturedMonthSelection(t.Context(), db, key, scope); err == nil {
		t.Fatal("invalid legacy fixture accepted")
	}
	if got, err := BuildRecoveredCapturedMonthSelection(t.Context(), db, key, scope); err == nil || len(got.Selection) != 0 {
		t.Fatal("new fallback bypassed legacy whole-sweep separation", err)
	}
}

func TestRecoveredTokenMonthMultipleAccountsRemainIndependent(t *testing.T) {
	f, scope, _, _ := recoveredMonthFixture(t, true, false, true)
	assembled, err := BuildRecoveredCapturedMonthSelection(t.Context(), f.store, f.job, scope)
	if err != nil || len(assembled.Tokens) != 2 {
		t.Fatal("missing complete member or split member", err)
	}
	for i, token := range assembled.Tokens {
		archive, report, err := DecodeTokenUsageMonthArchive(t.Context(), token.Raw, time.Now())
		consume, refund := int64(511609), int64(2)
		if i == 1 {
			consume, refund = 2000012, 4
		}
		if err != nil || archive.SelectionSHA256 != digest(assembled.Selection) || report.UserID != scope.Funding.UserIDs[i] || report.ConsumeQuota != consume || report.RefundQuota != refund {
			t.Fatal("accounts netted or mixed", report, err)
		}
	}
	destination, _ := emptyCapturedCopy(t)
	if _, err := CopyRecoveredCapturedMonthObservationsBounded(t.Context(), f.store, destination, f.job, scope, 64<<20); err != nil {
		t.Fatal("mixed old/child accounts did not survive isolated copy", err)
	}
}

func TestRecoveredTokenMonthRejectsMissingDamagedConflictingAndForeignData(t *testing.T) {
	for _, scenario := range []string{"missing_other_day", "old_projection", "child_projection", "incomplete_child", "second_without_first", "parent_amount_changed", "already_complete_parent", "other_job", "foreign_account", "scope_changed", "ordinal_outside_plan", "canceled", "shared_budget"} {
		t.Run(scenario, func(t *testing.T) {
			f, scope, plan, _ := recoveredMonthFixture(t, true, false)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			exec := func(q string, args ...any) {
				if _, err := f.store.Exec(q, args...); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "missing_other_day":
				exec(`DROP TRIGGER monthly_token_capture_no_delete; DELETE FROM portal_monthly_token_capture_slots WHERE ordinal=10`)
			case "old_projection":
				// A wrapped missing projection MUST NOT look like a missing slot.
				exec(`DROP TRIGGER monthly_usage_no_delete; DELETE FROM portal_monthly_usage_observations WHERE id=(SELECT usage_id FROM portal_monthly_token_capture_slots WHERE ordinal=0)`)
			case "child_projection":
				exec(`DROP TRIGGER monthly_usage_no_delete; DELETE FROM portal_monthly_usage_observations WHERE id=(SELECT usage_id FROM monthly_adaptive_token_observations LIMIT 1)`)
			case "incomplete_child":
				exec(`DROP TRIGGER adaptive_token_observations_no_DELETE; DELETE FROM monthly_adaptive_token_observations WHERE rowid=(SELECT rowid FROM monthly_adaptive_token_observations WHERE phase=1 LIMIT 1)`)
			case "second_without_first":
				a, err := loadTokenCaptureSlot(ctx, f.store, plan, 0)
				if err != nil {
					t.Fatal(err)
				}
				a.StartedAtMS += 300001
				a.CompletedAtMS += 300001
				syntheticRecoveredSlot(t, f.store, plan, 30, a)
				exec(`DROP TRIGGER monthly_token_capture_no_delete; DELETE FROM portal_monthly_token_capture_slots WHERE ordinal=0`)
			case "parent_amount_changed", "already_complete_parent":
				a, err := loadTokenCaptureSlot(ctx, f.store, plan, 0)
				if err != nil {
					t.Fatal(err)
				}
				if scenario == "parent_amount_changed" {
					a.Rows[0].Quota++
					a.Totals[0].Quota++
					exec(`DROP TRIGGER monthly_token_capture_no_delete; DELETE FROM portal_monthly_token_capture_slots WHERE ordinal=0`)
					syntheticRecoveredSlot(t, f.store, plan, 0, a)
				} else {
					a.StartedAtMS += 300001
					a.CompletedAtMS += 300001
					syntheticRecoveredSlot(t, f.store, plan, 30, a)
				}
			case "other_job":
				f.job = digest([]byte("another authority-bound task"))
			case "foreign_account":
				scope.Funding.UserIDs = []int64{999}
			case "scope_changed":
				scope.AuthVersion++
			case "ordinal_outside_plan":
				exec(`INSERT INTO portal_monthly_token_capture_slots SELECT job_key,100,user_id,sha256,usage_id,started_at_ms,completed_at_ms,payload FROM portal_monthly_token_capture_slots WHERE ordinal=1`)
			case "canceled":
				cancel()
			case "shared_budget":
				// Count abandoned bytes too, before attempting to parse/select.
				for i := 0; i < 32; i++ {
					exec(`INSERT INTO monthly_adaptive_token_observations(root_id,window_from,window_to,phase,sha,usage_id,started_at_ms,completed_at_ms,payload,projection) VALUES(?,?,?,0,?,?,1,2,zeroblob(2097152),x'00')`, digest([]byte("unrelated retained bytes")), i, i+1, digest([]byte("raw")), digest([]byte("projection")))
				}
			}
			queries := f.queries()
			out, err := BuildRecoveredCapturedMonthSelection(ctx, f.store, f.job, scope)
			if err == nil || len(out.Selection) != 0 || len(out.Tokens) != 0 || queries != f.queries() {
				t.Fatal("invalid data returned usable month or queried upstream", err)
			}
			if scenario == "canceled" && !errors.Is(err, context.Canceled) || scenario == "shared_budget" && !errors.Is(err, ErrTokenMonthCaptureBudget) {
				t.Fatal("lost bounded/cancellation reason", err)
			}
		})
	}
}

func TestRecoveredTokenPartitionRejectsNonBinaryGapsAndTinyWindows(t *testing.T) {
	for _, tc := range []struct {
		ends []int64
		want bool
	}{
		{[]int64{0, 86400}, true}, {[]int64{0, 43200, 86400}, true},
		{[]int64{0, 21600, 43200, 86400}, true}, {[]int64{0, 20000, 86400}, false},
		{[]int64{0, 43200, 43201, 86400}, false}, {[]int64{0, 43200}, false}, {[]int64{1, 86400}, false},
	} {
		ends := tc.ends
		leaves := []UsageWindowRequest{}
		for i := 0; i+1 < len(ends); i++ {
			leaves = append(leaves, UsageWindowRequest{From: ends[i], To: ends[i+1]})
		}
		if got := isRecoveredTokenPartition(0, 86400, leaves); got != tc.want {
			t.Fatal("partition shape", ends, got, tc.want)
		}
	}
}

func TestRecoveredSelectionVerifierRejectsInvalidShapeVersionAndLostProjection(t *testing.T) {
	for _, scenario := range []string{"non_binary", "missing_projection", "wrong_mode", "unknown_version", "cross_day", "incomplete_day"} {
		t.Run(scenario, func(t *testing.T) {
			f, scope, _, _ := recoveredMonthFixture(t, false, false)
			destination, _ := emptyCapturedCopy(t)
			assembled, err := CopyRecoveredCapturedMonthObservationsBounded(t.Context(), f.store, destination, f.job, scope, 64<<20)
			if err != nil {
				t.Fatal(err)
			}
			var selection CapturedMonthSelection
			if err := json.Unmarshal(assembled.Selection, &selection); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "wrong_mode":
				selection.Mode = CapturedMonthSelectionMode
			case "unknown_version":
				selection.Version = 3
			case "missing_projection":
				if _, err := destination.Exec(`DROP TRIGGER monthly_usage_no_delete; DELETE FROM portal_monthly_usage_observations WHERE id=?`, selection.Tokens[0].Usage[0].OlderID); err != nil {
					t.Fatal(err)
				}
			case "incomplete_day":
				selection.Tokens[0].Usage = selection.Tokens[0].Usage[1:]
				selection.Tokens[0].Parents = selection.Tokens[0].Parents[2:]
			case "non_binary", "cross_day":
				// An explicitly observed empty day split at a non-midpoint can
				// still pass the general money/coverage audit. V2 must refuse
				// it rather than silently widening the recovery algorithm.
				w := selection.Tokens[0].Usage[11] // closed, actually empty day
				old, err := LoadUsageWindowObservation(t.Context(), destination, w.OlderID)
				if err != nil || len(old.Rows) != 0 {
					t.Fatal("invalid empty fixture", err)
				}
				newer, err := LoadUsageWindowObservation(t.Context(), destination, w.NewerID)
				if err != nil {
					t.Fatal(err)
				}
				mid := old.Request.From + 20000
				if scenario == "cross_day" {
					mid = old.Request.To + 3600
				}
				var replacements []UsageWindowVersion
				for _, bounds := range [][2]int64{{old.Request.From, mid}, {mid, old.Request.To}} {
					if bounds[1] <= bounds[0] {
						continue
					}
					a, b := old, newer
					a.Request.From, a.Request.To = bounds[0], bounds[1]
					b.Request = a.Request
					// Cross-day case remains <=one day but crosses CST midnight.
					if scenario == "cross_day" {
						a.Request.From += 7200
						b.Request.From = a.Request.From
					}
					aid, err := SaveUsageWindowObservation(t.Context(), destination, a)
					if err != nil {
						t.Fatal(err)
					}
					bid, err := SaveUsageWindowObservation(t.Context(), destination, b)
					if err != nil {
						t.Fatal(err)
					}
					replacements = append(replacements, UsageWindowVersion{OlderID: aid, NewerID: bid})
				}
				usage := append([]UsageWindowVersion{}, selection.Tokens[0].Usage[:11]...)
				usage = append(usage, replacements...)
				usage = append(usage, selection.Tokens[0].Usage[12:]...)
				selection.Tokens[0].Usage = usage
				selection.Tokens[0].Parents = make([]string, 2*len(usage))
				for i := range selection.Tokens[0].Parents {
					selection.Tokens[0].Parents[i] = digest([]byte("placeholder only for adversarial shape test"))
				}
				if scenario == "non_binary" {
					if _, err := ReviewUsageMonth(t.Context(), destination, scope.Funding, usage); err != nil {
						t.Fatal("shape fixture does not reach the specific shape guard", err)
					}
				}
			}
			raw, _ := json.Marshal(selection)
			if err := VerifyCapturedObservationSelection(t.Context(), destination, raw); err == nil {
				t.Fatal("invalid v2 handoff accepted", scenario)
			}
		})
	}
}

func TestRecoveredCopyFailureNeverReturnsPartialHandoff(t *testing.T) {
	f, scope, _, _ := recoveredMonthFixture(t, true, false)
	for _, scenario := range []string{"too_small", "over_limit", "nonempty", "same_file_alias", "readonly"} {
		t.Run(scenario, func(t *testing.T) {
			destination, _ := emptyCapturedCopy(t)
			limit := int64(64 << 20)
			switch scenario {
			case "too_small":
				limit = 4096
			case "over_limit":
				limit++
			case "nonempty":
				if _, err := destination.Exec(`CREATE TABLE occupied(value)`); err != nil {
					t.Fatal(err)
				}
			case "readonly":
				if _, err := destination.Exec(`PRAGMA query_only=ON`); err != nil {
					t.Fatal(err)
				}
			case "same_file_alias":
				alias, err := sql.Open("sqlite", f.path)
				if err != nil {
					t.Fatal(err)
				}
				defer alias.Close()
				destination = alias
			}
			out, err := CopyRecoveredCapturedMonthObservationsBounded(t.Context(), f.store, destination, f.job, scope, limit)
			if err == nil || len(out.Selection) != 0 || len(out.Tokens) != 0 {
				t.Fatal("partial copy returned usable handoff", err)
			}
		})
	}
}
