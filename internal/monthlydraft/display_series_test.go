package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

type displaySeriesFixture struct {
	g             *Generation
	auth          *gorm.DB
	plans         []GenerationPlan
	inputs        [][]byte
	requests      []monthlytasks.Request
	ranges        []AutomaticDisplaySeriesRange
	metadata      AutomaticContext
	fundingPath   string
	authorityPath string
}

// Three independent, synthetic source months with sealed daily funding scans,
// explicit exact-cutoff balances/refunds and retained wallet/directory scope.
// No customers or live databases; unknown historical token statistics stay
// unknown. The retained fixture's semantics are expressly test-attested.
func newDisplaySeriesFixture(t *testing.T) displaySeriesFixture {
	t.Helper()
	return newDisplaySeriesFixtureFrom(t, 7)
}

func newDisplaySeriesFixtureFrom(t *testing.T, firstMonth time.Month) displaySeriesFixture {
	t.Helper()
	if firstMonth < time.May || firstMonth > time.September {
		t.Fatal("synthetic source month outside this fixture's supported period")
	}
	ctx := context.Background()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "original-funding.db")
	source := generationDB(t, path, false)
	if err := monthlysource.InitFundingPageStore(ctx, source); err != nil {
		t.Fatal(err)
	}
	receiver := generationDB(t, filepath.Join(root, "receiver.db"), false)
	if err := InitGenerationReceiver(ctx, receiver); err != nil {
		t.Fatal(err)
	}
	if err := InitGenerationSelections(ctx, receiver); err != nil {
		t.Fatal(err)
	}
	auth, ro, authorityPath := usageAuthorityFixture(t)
	if err := auth.AutoMigrate(&monitor.PortalMonthlyBill{}); err != nil {
		t.Fatal(err)
	}
	owner, _, err := LoadUsageGenerationOwner(ctx, ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	origin, err := monthlysource.WalletSourceIdentity("synthetic-series", "synthetic-series")
	if err != nil {
		t.Fatal(err)
	}
	rules := monthlysource.FundingRules{Producer: monthlysource.FundingProducerRC4RC26, QuotaPerUnit: 500000}
	f := displaySeriesFixture{auth: auth, fundingPath: path, authorityPath: authorityPath, ranges: []AutomaticDisplaySeriesRange{{GroupID: 1, SourceIdentity: origin, From: fmt.Sprintf("2026-%02d", firstMonth), Through: "2026-09"}}}
	opening := []int64{1160872085, 2000000}
	used := []int64{10000000, 10000000}
	zone := time.FixedZone("Usage-UTC+8", 8*3600)
	for month := firstMonth; month <= time.September; month++ {
		start := time.Date(2026, month, 1, 0, 0, 0, 0, zone)
		end := start.AddDate(0, 1, 0)
		p := GenerationPlan{Version: 2, Owner: owner, Funding: monthlysource.FundingMonthRequest{Month: start.Format("2006-01"), SourceIdentity: origin, Rules: rules, UserIDs: []int64{201, 202}}, AutomaticScopeSourceID: "scope"}
		for at := start; at.Before(end); at = at.AddDate(0, 0, 1) {
			var scans []string
			for pass := int64(0); pass < 2; pass++ {
				scan, err := monthlysource.NewFundingDayScan(origin, rules, p.Funding.UserIDs, at.Unix(), at.AddDate(0, 0, 1).Unix())
				if err != nil {
					t.Fatal(err)
				}
				base := (end.Unix()+86400)*1000 + int64(at.Day())*1000 + pass*300000
				page := monthlysource.FundingPage{Version: 1, Scan: scan, FenceID: 100, EndReached: true, StartedAtMS: base, CompletedAtMS: base + 10, Rows: []monthlysource.FundingLog{}}
				if _, err := monthlysource.SaveFundingPage(ctx, source, page); err != nil {
					t.Fatal(err)
				}
				scans = append(scans, scan.ID)
			}
			window, err := monthlysource.ReviewFundingWindow(ctx, source, scans[0], scans[1])
			if err != nil {
				t.Fatal(err)
			}
			c := window.Comparison()
			p.Windows = append(p.Windows, monthlysource.FundingMonthWindowVersion{From: c.From, To: c.To, OlderScanID: c.OlderScanID, NewerScanID: c.NewerScanID, OlderHeadID: c.OlderHeadID, NewerHeadID: c.NewerHeadID})
		}
		input := monitor.MonthlyPreparationInput{Version: 1}
		scope := AutomaticScope{Group: monitor.MonthlyBundleGroup{GroupID: 1, Name: "synthetic org", Source: "proof#directory", Months: []string{p.Funding.Month}}}
		for i, id := range p.Funding.UserIDs {
			consume := []int64{2500, 10000}[i]
			input.Accounts = append(input.Accounts, monitor.MonthlyPreparationAccount{
				UserID: id, GroupID: 1, Username: fmt.Sprintf("synthetic-%d", id), Months: []string{p.Funding.Month}, CounterBasis: "net_consumption", BasisSource: "proof#counter",
				Movements:        []monitor.MonthlyPreparationMovement{},
				Baseline:         monitor.MonthlyPreparationBalance{UserID: id, At: start.Unix(), BalanceQuota: opening[i], UsedQuota: used[i], Source: "proof#opening", TimeBasis: "source_log_cutoff", CutoffSource: "proof#cutoff"},
				Snapshots:        []monitor.MonthlyPreparationBalance{{UserID: id, At: end.Unix(), BalanceQuota: opening[i] - consume, UsedQuota: used[i] + consume, Source: "proof#closing", TimeBasis: "source_log_cutoff", CutoffSource: "proof#cutoff"}},
				Spans:            []monitor.MonthlyPreparationSpan{{UserID: id, From: start.Unix(), To: end.Unix(), ConsumeQuota: consume, Source: "proof#usage"}},
				MovementCoverage: []monitor.MonthlyPreparationCoverage{{UserID: id, From: start.Unix(), To: end.Unix(), Source: "proof#funding"}},
				WalletMonths:     []monitor.MonthlyPreparationWalletMonth{{UserID: id, Month: p.Funding.Month, Source: "proof#refund"}},
			})
			scope.Group.Members = append(scope.Group.Members, monitor.MonthlyBundleMember{UserID: id, Username: fmt.Sprintf("synthetic-%d", id)})
			scope.Accounts = append(scope.Accounts, AutomaticWalletScope{UserID: id, WalletOnly: true, WalletSource: "proof#wallet"})
			opening[i] -= consume
			used[i] += consume
		}
		proof, _ := json.Marshal(input.Accounts)
		proofPath := filepath.Join(root, p.Funding.Month+"-original.json")
		if err := os.WriteFile(proofPath, proof, 0600); err != nil {
			t.Fatal(err)
		}
		scopeRaw, _ := json.Marshal(RetainedAutomaticScope{Version: 1, Owner: owner, Month: p.Funding.Month, SourceIdentity: origin, Scope: scope})
		scopePath := filepath.Join(root, p.Funding.Month+"-scope.json")
		if err := os.WriteFile(scopePath, scopeRaw, 0600); err != nil {
			t.Fatal(err)
		}
		input.Sources = []monitor.MonthlyPreparationSource{{ID: "proof", Path: proofPath, SHA256: generationHash(proof)}, {ID: "scope", Path: scopePath, SHA256: generationHash(scopeRaw)}}
		raw := draftJSON(t, input)
		p.PreparationSHA256 = generationHash(raw)
		canonicalPlan, canonical, _, err := canonicalGeneration(p, raw, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if err := generationWrite(ctx, receiver, func(ctx context.Context, tx *sql.Tx) error {
			return retainGenerationPlan(ctx, tx, canonical, raw, false)
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := SelectGenerationVersion(ctx, receiver, canonicalPlan, raw, ""); err != nil {
			t.Fatal(err)
		}
		f.plans = append(f.plans, canonicalPlan)
		f.inputs = append(f.inputs, raw)
		f.requests = append(f.requests, generationRequest(canonicalPlan, canonical))
	}
	selector, err := GenerationSelector(receiver)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := NewUsageGenerationGuard(ro, selector)
	if err != nil {
		t.Fatal(err)
	}
	f.g, err = NewGeneration(receiver, generationDB(t, path, true), root, guard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.g.Close() })
	load, err := NewRetainedAutomaticScopeLoader(f.g, "scope")
	if err != nil {
		t.Fatal(err)
	}
	f.metadata, err = NewObservedAutomaticContext(f.g, load)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func displaySeriesPublisher(db *gorm.DB) AutomaticDisplaySeriesPublisher {
	return func(ctx context.Context, req monthlytasks.Request, raw []byte, p *monitor.PreparedMonthlyDisplayPublication) error {
		_, err := monitor.ImportMonthlyBillWithDisplaySeries(ctx, db, raw, monitor.MonthlyBillExpectedAuthorization{GroupID: req.Owner.GroupID, AuthVersion: req.Owner.AuthVersion, MemberFingerprint: req.Owner.MemberFingerprint, MemberManifest: req.Owner.MemberManifest}, p, time.Now())
		return err
	}
}

func displaySeriesCount(t *testing.T, f displaySeriesFixture, want int64) {
	t.Helper()
	var count int64
	if err := f.auth.Model(&monitor.PortalMonthlyBill{}).Count(&count).Error; err != nil || count != want {
		t.Fatal("unexpected published bill count", count, want, err)
	}
}

func TestAutomaticDisplaySeriesBackgroundPreparationEveryMonthOrder(t *testing.T) {
	for _, order := range [][]int{{0, 1, 2}, {2, 1, 0}, {0, 2, 1}, {1, 2, 0}, {1, 0, 2}, {2, 0, 1}} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			f := newDisplaySeriesFixture(t)
			before, err := os.ReadFile(f.fundingPath)
			if err != nil {
				t.Fatal(err)
			}
			w, err := NewAutomaticDisplaySeriesLocalWorker(f.g, f.metadata, f.ranges, displaySeriesPublisher(f.auth))
			if err != nil {
				t.Fatal(err)
			}
			for i, index := range order {
				job, err := f.g.Submit(context.Background(), f.plans[index], f.inputs[index])
				if err != nil {
					t.Fatal(err)
				}
				if worked, err := w.step(context.Background()); err != nil || !worked || w.Status().Published != 1 {
					t.Fatal("background preparation/publication failed", index, worked, err, w.Status())
				}
				displaySeriesCount(t, f, int64(i+1))
				var taskCount int
				if err := f.g.receiver.QueryRow("SELECT COUNT(*) FROM portal_monthly_draft_tasks").Scan(&taskCount); err != nil || taskCount != i+1 {
					t.Fatal("preparation created unrequested customer tasks", taskCount, err)
				}
				var record monitor.PortalMonthlyBill
				if err := f.auth.First(&record, "group_id=1 AND month=?", job.Request.Month).Error; err != nil {
					t.Fatal(err)
				}
				var p struct {
					Display struct {
						Organization struct{ Opening, Closing string } `json:"organization"`
					} `json:"display_balances"`
					Members []struct {
						UserID  int64  `json:"user_id"`
						Closing string `json:"closing"`
					} `json:"members"`
				}
				if err := json.Unmarshal([]byte(record.Payload), &p); err != nil {
					t.Fatal(err)
				}
				// Independent integer-cent fixture expectation: organization starts
				// at 2325.74, each month rounds costs .005 + .02 = .03.
				wantOpen := []string{"2325.740000", "2325.710000", "2325.680000"}[index]
				wantClose := []string{"2325.710000", "2325.680000", "2325.650000"}[index]
				if p.Display.Organization.Opening != wantOpen || p.Display.Organization.Closing != wantClose {
					t.Fatal("order-dependent customer cents", index, p.Display, wantOpen, wantClose)
				}
				fresh, err := NewAutomaticDisplaySeriesLocalWorker(f.g, f.metadata, f.ranges, displaySeriesPublisher(f.auth))
				if err != nil {
					t.Fatal(err)
				}
				if worked, err := fresh.step(context.Background()); err != nil || worked {
					t.Fatal("restart reissued an existing bill", worked, err)
				}
				w = fresh
			}
			after, err := os.ReadFile(f.fundingPath)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("background preparation wrote original funding data", err)
			}
		})
	}
}

func TestAutomaticDisplaySeriesCacheReloadAuditsOriginalsAndOwnership(t *testing.T) {
	f := newDisplaySeriesFixture(t)
	request := f.requests[0]
	first, err := f.g.automaticDisplaySource(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := f.g.receiver.QueryRow("SELECT COUNT(*) FROM portal_monthly_draft_tasks").Scan(&count); err != nil || count != 0 {
		t.Fatal("source preparation submitted a customer task", err)
	}
	second, err := f.g.automaticDisplaySource(context.Background(), request)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatal("retained source changed on reload", err)
	}
	first.Names[201] = "mutated caller"
	first.Evidence[0] = 0
	third, err := f.g.automaticDisplaySource(context.Background(), request)
	if err != nil || !reflect.DeepEqual(second, third) {
		t.Fatal("cache aliases caller memory", err)
	}
	restarted, err := NewGeneration(f.g.receiver, f.g.funding, f.g.root.Name(), f.g.verifyCurrent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	fourth, err := restarted.automaticDisplaySource(context.Background(), request)
	if err != nil || !reflect.DeepEqual(second, fourth) {
		t.Fatal("reopened resource owner changed the retained source", err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(f.inputs[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input.Sources[0].Path, []byte("changed original"), 0600); err != nil {
		t.Fatal(err)
	}
	if source, err := f.g.automaticDisplaySource(context.Background(), request); err == nil || len(source.Evidence) != 0 {
		t.Fatal("cache bypassed source audit", err)
	}
	displaySeriesCount(t, f, 0)
}

func TestAutomaticDisplaySeriesCorruptCacheNeverReplacesOriginalMoney(t *testing.T) {
	for _, kind := range []string{"payload", "permissions", "symlink", "hardlink", "forged_evidence"} {
		t.Run(kind, func(t *testing.T) {
			f := newDisplaySeriesFixture(t)
			request := f.requests[0]
			if _, err := f.g.automaticDisplaySource(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			files, err := filepath.Glob(filepath.Join(f.g.root.Name(), "display-source-*.json"))
			if err != nil || len(files) != 1 {
				t.Fatal("cache not retained", err)
			}
			name := files[0]
			original, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "payload":
				if err := os.WriteFile(name, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "permissions":
				if err := os.Chmod(name, 0644); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Rename(name, name+".backup"); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(name+".backup", name); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(name, name+".second"); err != nil {
					t.Fatal(err)
				}
			case "forged_evidence":
				var cached automaticDisplayCache
				if err := json.Unmarshal(original, &cached); err != nil {
					t.Fatal(err)
				}
				var evidence monitor.MonthlyBillEvidence
				if err := json.Unmarshal(cached.Source.Evidence, &evidence); err != nil {
					t.Fatal(err)
				}
				evidence.Users[0].ConsumeQuota++
				cached.Source.Evidence, _ = json.Marshal(evidence)
				cached.SHA256, err = automaticDisplayCacheDigest(cached)
				if err != nil {
					t.Fatal(err)
				}
				raw, _ := json.Marshal(cached)
				if err := os.WriteFile(name, raw, 0600); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			if out, err := f.g.automaticDisplaySource(context.Background(), request); err == nil || len(out.Evidence) != 0 {
				t.Fatal("corrupt cache was used as authoritative money", err)
			}
			after, err := os.ReadFile(name)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("rejection silently rebuilt or overwrote the cache", err)
			}
			displaySeriesCount(t, f, 0)
		})
	}
}

func TestAutomaticDisplaySeriesCacheLockAndCapacity(t *testing.T) {
	f := newDisplaySeriesFixture(t)
	request := f.requests[0]
	raw, _ := json.Marshal(request)
	name := "display-source-" + generationHash(raw)
	lock, err := f.g.root.OpenFile(name+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if out, err := f.g.automaticDisplaySource(context.Background(), request); !errors.Is(err, ErrGenerationBusy) || len(out.Evidence) != 0 {
		t.Fatal("cache lock did not exclude another writer", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 256; i++ {
		if err := os.WriteFile(filepath.Join(f.g.root.Name(), fmt.Sprintf("display-source-%064x.json", i)), []byte("{}"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if out, err := f.g.automaticDisplaySource(context.Background(), request); !errors.Is(err, ErrGenerationCapacity) || len(out.Evidence) != 0 {
		t.Fatal("source cache grew beyond its fixed cap", err)
	}
	if _, err := os.Stat(filepath.Join(f.g.root.Name(), name+".json")); !os.IsNotExist(err) {
		t.Fatal("capacity failure left a completed source", err)
	}
	pending, err := filepath.Glob(filepath.Join(f.g.root.Name(), "display-source-*.pending-*"))
	if err != nil || len(pending) != 0 {
		t.Fatal("capacity failure left this call's incomplete temp file", pending, err)
	}
}

func TestAutomaticDisplaySeriesPreparationFailureBudgetAndReactivation(t *testing.T) {
	f := newDisplaySeriesFixture(t)
	job, err := f.g.Submit(context.Background(), f.plans[2], f.inputs[2])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.Run(context.Background(), job.Request.Owner, job.ID); err != nil {
		t.Fatal(err)
	}
	calls := 0
	metadata := func(ctx context.Context, r monthlytasks.Request) (AutomaticSourceContext, error) {
		calls++
		return f.metadata(ctx, r)
	}
	w, err := NewAutomaticDisplaySeriesLocalWorker(f.g, metadata, f.ranges, displaySeriesPublisher(f.auth))
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(f.inputs[0])
	if err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(input.Sources[0].Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input.Sources[0].Path, []byte("bad predecessor proof"), 0600); err != nil {
		t.Fatal(err)
	}
	for range 6 {
		if worked, err := w.step(context.Background()); err != nil || !worked {
			t.Fatal("bounded preparation failure", worked, err)
		}
		d := w.getDeferral(job.ID)
		d.until = time.Time{}
		w.putDeferral(job.ID, d)
	}
	if calls != 3 || w.getDeferral(job.ID).preparationFailures != 3 {
		t.Fatal("same failed dependency was recomputed without a bound", calls, w.getDeferral(job.ID))
	}
	if state, err := w.finalizationState(context.Background(), job.ID); err != nil || state != "needs_review" {
		t.Fatal("paused preparation was shown as permanently generating", state, err)
	}
	var count int
	if err := f.g.receiver.QueryRow("SELECT COUNT(*) FROM portal_monthly_publication_jobs").Scan(&count); err != nil || count != 0 {
		t.Fatal("source preparation spent publication retry budget", count, err)
	}
	if err := os.WriteFile(input.Sources[0].Path, good, 0600); err != nil {
		t.Fatal(err)
	}
	// Same source restored: no endless automatic retries. A deliberate worker
	// restart is an activation, while the publication journal is still untouched.
	fresh, err := NewAutomaticDisplaySeriesLocalWorker(f.g, f.metadata, f.ranges, displaySeriesPublisher(f.auth))
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := fresh.step(context.Background()); err != nil || !worked || fresh.Status().Published != 1 {
		t.Fatal("explicit recovery activation failed", worked, err, fresh.Status())
	}
	displaySeriesCount(t, f, 1)
}

func TestAutomaticDisplaySeriesMissingPredecessorDoesNotSpendPublishRetries(t *testing.T) {
	f := newDisplaySeriesFixture(t)
	selection, err := ReadGenerationSelection(context.Background(), f.g.receiver, 1, "2026-07")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WithdrawGenerationSelection(context.Background(), f.g.receiver, 1, "2026-07", selection.Head); err != nil {
		t.Fatal(err)
	}
	job, err := f.g.Submit(context.Background(), f.plans[2], f.inputs[2])
	if err != nil {
		t.Fatal(err)
	}
	w, err := NewAutomaticDisplaySeriesLocalWorker(f.g, f.metadata, f.ranges, displaySeriesPublisher(f.auth))
	if err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if worked, err := w.step(context.Background()); err != nil || !worked {
			t.Fatal("waiting source handling", worked, err)
		}
		d := w.deferred[job.ID]
		d.until = time.Time{}
		w.deferred[job.ID] = d
	}
	var count int
	if err := f.g.receiver.QueryRow("SELECT COUNT(*) FROM portal_monthly_publication_jobs").Scan(&count); err != nil || count != 0 {
		t.Fatal("waiting exhausted publication attempts", count, err)
	}
	displaySeriesCount(t, f, 0)
	// A NEW selected preparation is an activation, never reactivation of a
	// withdrawn input. Add a source-only marker to make a new canonical input.
	input, err := monitor.DecodeMonthlyPreparationInput(f.inputs[0])
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(f.g.root.Name(), "new-independent-proof.json")
	if err := os.WriteFile(path, []byte("synthetic new retained evidence version"), 0600); err != nil {
		t.Fatal(err)
	}
	input.Sources = append(input.Sources, monitor.MonthlyPreparationSource{ID: "new-proof", Path: path, SHA256: generationHash([]byte("synthetic new retained evidence version"))})
	raw := draftJSON(t, input)
	p := f.plans[0]
	p.PreparationSHA256 = generationHash(raw)
	p, canonical, _, err := canonicalGeneration(p, raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := generationWrite(context.Background(), f.g.receiver, func(ctx context.Context, tx *sql.Tx) error {
		return retainGenerationPlan(ctx, tx, canonical, raw, false)
	}); err != nil {
		t.Fatal(err)
	}
	current, err := ReadGenerationSelection(context.Background(), f.g.receiver, 1, "2026-07")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SelectGenerationVersion(context.Background(), f.g.receiver, p, raw, current.Head); err != nil {
		t.Fatal(err)
	}
	if worked, err := w.step(context.Background()); err != nil || !worked || w.Status().Published != 1 {
		t.Fatal("new source did not activate waiting bill", worked, err, w.Status())
	}
	displaySeriesCount(t, f, 1)
}

func TestAutomaticDisplaySeriesRejectsInvalidRanges(t *testing.T) {
	f := newDisplaySeriesFixture(t)
	for _, kind := range []string{"missing", "duplicate", "origin", "reversed", "future", "too_long"} {
		t.Run(kind, func(t *testing.T) {
			ranges := append([]AutomaticDisplaySeriesRange(nil), f.ranges...)
			switch kind {
			case "missing":
				ranges = nil
			case "duplicate":
				ranges = append(ranges, ranges[0])
			case "origin":
				ranges[0].SourceIdentity = "unknown"
			case "reversed":
				ranges[0].From = "2026-10"
			case "future":
				ranges[0].Through = "2099-12"
			case "too_long":
				ranges[0].From = "2020-01"
				ranges[0].Through = "2030-01"
			}
			if prepare, err := NewSelectedAutomaticDisplaySeries(f.g, ranges); err == nil || prepare != nil {
				t.Fatal("invalid supported range accepted", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if source, err := f.g.automaticDisplaySource(ctx, f.requests[0]); !errors.Is(err, context.Canceled) || len(source.Evidence) != 0 {
		t.Fatal("cancelled preparation returned source", err)
	}
	for _, pattern := range []string{"display-source-*.json", "display-source-*.pending-*"} {
		files, err := filepath.Glob(filepath.Join(f.g.root.Name(), pattern))
		if err != nil || len(files) != 0 {
			t.Fatal("cancelled preparation left usable or pending source", files, err)
		}
	}
}

func TestAutomaticDisplaySeriesCurrentAuthorizationKeepsHistoricalSourceRevision(t *testing.T) {
	f := newDisplaySeriesFixture(t)
	w, err := NewAutomaticDisplaySeriesLocalWorker(f.g, f.metadata, f.ranges, displaySeriesPublisher(f.auth))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.Submit(context.Background(), f.plans[0], f.inputs[0]); err != nil {
		t.Fatal(err)
	}
	if worked, err := w.step(context.Background()); err != nil || !worked || w.Status().Published != 1 {
		t.Fatal("initial historical publication failed", worked, err, w.Status())
	}
	var first monitor.PortalMonthlyBill
	if err := f.auth.First(&first, "group_id=1 AND month='2026-07'").Error; err != nil {
		t.Fatal(err)
	}
	if err := f.auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
		t.Fatal(err)
	}
	owner, _, err := LoadUsageGenerationOwner(context.Background(), f.auth, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(f.inputs[2])
	if err != nil {
		t.Fatal(err)
	}
	for i := range input.Sources {
		if input.Sources[i].ID != "scope" {
			continue
		}
		raw, err := os.ReadFile(input.Sources[i].Path)
		if err != nil {
			t.Fatal(err)
		}
		var scope RetainedAutomaticScope
		if err := json.Unmarshal(raw, &scope); err != nil {
			t.Fatal(err)
		}
		scope.Owner = owner
		raw, err = json.Marshal(scope)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(f.g.root.Name(), "2026-09-scope-revision2.json")
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		input.Sources[i].Path, input.Sources[i].SHA256 = path, generationHash(raw)
	}
	raw := draftJSON(t, input)
	p := f.plans[2]
	p.Owner = owner
	p.PreparationSHA256 = generationHash(raw)
	p, canonical, _, err := canonicalGeneration(p, raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := generationWrite(context.Background(), f.g.receiver, func(ctx context.Context, tx *sql.Tx) error {
		return retainGenerationPlan(ctx, tx, canonical, raw, false)
	}); err != nil {
		t.Fatal(err)
	}
	selection, err := ReadGenerationSelection(context.Background(), f.g.receiver, 1, "2026-09")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SelectGenerationVersion(context.Background(), f.g.receiver, p, raw, selection.Head); err != nil {
		t.Fatal(err)
	}
	job, err := f.g.Submit(context.Background(), p, raw)
	if err != nil {
		t.Fatal(err)
	}
	if job.Request.Owner.AuthVersion != 2 {
		t.Fatal("test did not select live authorization")
	}
	if worked, err := w.step(context.Background()); err != nil || !worked || w.Status().Published != 2 {
		t.Fatal("live authorization update invalidated immutable historical sources", worked, err, w.Status())
	}
	var after monitor.PortalMonthlyBill
	if err := f.auth.First(&after, "group_id=1 AND month='2026-07'").Error; err != nil || after != first {
		t.Fatal("new authority rewrote the historical bill", err)
	}
	displaySeriesCount(t, f, 2)
}

func TestAutomaticDisplaySeriesNewZeroOpeningMemberDoesNotRebaseHistory(t *testing.T) {
	f := newDisplaySeriesFixture(t)
	w, err := NewAutomaticDisplaySeriesLocalWorker(f.g, f.metadata, f.ranges, displaySeriesPublisher(f.auth))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.Submit(context.Background(), f.plans[0], f.inputs[0]); err != nil {
		t.Fatal(err)
	}
	if worked, err := w.step(context.Background()); err != nil || !worked || w.Status().Published != 1 {
		t.Fatal("initial issue failed", worked, err, w.Status())
	}
	var old monitor.PortalMonthlyBill
	if err := f.auth.First(&old, "group_id=1 AND month='2026-07'").Error; err != nil {
		t.Fatal(err)
	}
	if err := f.auth.Create(&monitor.TrackedUser{UserID: 303, GroupID: 1, Username: "synthetic-303"}).Error; err != nil {
		t.Fatal(err)
	}
	owner, _, err := LoadUsageGenerationOwner(context.Background(), f.auth, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	input, err := monitor.DecodeMonthlyPreparationInput(f.inputs[2])
	if err != nil {
		t.Fatal(err)
	}
	zone := time.FixedZone("Usage-UTC+8", 8*3600)
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, zone)
	end := start.AddDate(0, 1, 0)
	input.Accounts = append(input.Accounts, monitor.MonthlyPreparationAccount{UserID: 303, GroupID: 1, Username: "synthetic-303", Months: []string{"2026-09"}, CounterBasis: "net_consumption", BasisSource: "proof#new-counter",
		Baseline:  monitor.MonthlyPreparationBalance{UserID: 303, At: start.Unix(), Source: "proof#new-opening", TimeBasis: "source_log_cutoff", CutoffSource: "proof#new-cutoff"},
		Snapshots: []monitor.MonthlyPreparationBalance{{UserID: 303, At: end.Unix(), Source: "proof#new-closing", TimeBasis: "source_log_cutoff", CutoffSource: "proof#new-cutoff"}},
		Spans:     []monitor.MonthlyPreparationSpan{{UserID: 303, From: start.Unix(), To: end.Unix(), Source: "proof#new-usage"}}, Movements: []monitor.MonthlyPreparationMovement{},
		MovementCoverage: []monitor.MonthlyPreparationCoverage{{UserID: 303, From: start.Unix(), To: end.Unix(), Source: "proof#new-funding"}},
		WalletMonths:     []monitor.MonthlyPreparationWalletMonth{{UserID: 303, Month: "2026-09", Source: "proof#new-refund"}},
	})
	for i := range input.Sources {
		source := &input.Sources[i]
		if source.ID == "proof" {
			raw, _ := json.Marshal(input.Accounts)
			path := filepath.Join(f.g.root.Name(), "2026-09-three-member-proof.json")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			source.Path, source.SHA256 = path, generationHash(raw)
		}
		if source.ID == "scope" {
			raw, err := os.ReadFile(source.Path)
			if err != nil {
				t.Fatal(err)
			}
			var scope RetainedAutomaticScope
			if err := json.Unmarshal(raw, &scope); err != nil {
				t.Fatal(err)
			}
			scope.Owner = owner
			scope.Scope.Group.Members = append(scope.Scope.Group.Members, monitor.MonthlyBundleMember{UserID: 303, Username: "synthetic-303"})
			scope.Scope.Accounts = append(scope.Scope.Accounts, AutomaticWalletScope{UserID: 303, WalletOnly: true, WalletSource: "proof#new-wallet"})
			raw, _ = json.Marshal(scope)
			path := filepath.Join(f.g.root.Name(), "2026-09-three-member-scope.json")
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			source.Path, source.SHA256 = path, generationHash(raw)
		}
	}
	p := f.plans[2]
	p.Owner = owner
	p.Funding.UserIDs = []int64{201, 202, 303}
	p.Windows = nil
	db := generationDB(t, f.fundingPath, false)
	for at := start; at.Before(end); at = at.AddDate(0, 0, 1) {
		var ids []string
		for pass := int64(0); pass < 2; pass++ {
			scan, err := monthlysource.NewFundingDayScan(p.Funding.SourceIdentity, p.Funding.Rules, p.Funding.UserIDs, at.Unix(), at.AddDate(0, 0, 1).Unix())
			if err != nil {
				t.Fatal(err)
			}
			base := (end.Unix()+86400)*1000 + int64(at.Day())*1000 + pass*300000
			page := monthlysource.FundingPage{Version: 1, Scan: scan, FenceID: 100, EndReached: true, StartedAtMS: base, CompletedAtMS: base + 10, Rows: []monthlysource.FundingLog{}}
			if _, err := monthlysource.SaveFundingPage(context.Background(), db, page); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, scan.ID)
		}
		window, err := monthlysource.ReviewFundingWindow(context.Background(), db, ids[0], ids[1])
		if err != nil {
			t.Fatal(err)
		}
		c := window.Comparison()
		p.Windows = append(p.Windows, monthlysource.FundingMonthWindowVersion{From: c.From, To: c.To, OlderScanID: c.OlderScanID, NewerScanID: c.NewerScanID, OlderHeadID: c.OlderHeadID, NewerHeadID: c.NewerHeadID})
	}
	raw := draftJSON(t, input)
	p.PreparationSHA256 = generationHash(raw)
	p, canonical, _, err := canonicalGeneration(p, raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := generationWrite(context.Background(), f.g.receiver, func(ctx context.Context, tx *sql.Tx) error {
		return retainGenerationPlan(ctx, tx, canonical, raw, false)
	}); err != nil {
		t.Fatal(err)
	}
	selection, err := ReadGenerationSelection(context.Background(), f.g.receiver, 1, "2026-09")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SelectGenerationVersion(context.Background(), f.g.receiver, p, raw, selection.Head); err != nil {
		t.Fatal(err)
	}
	if _, err := f.g.Submit(context.Background(), p, raw); err != nil {
		t.Fatal(err)
	}
	if worked, err := w.step(context.Background()); err != nil || !worked || w.Status().Published != 2 {
		t.Fatal("new member changed historical anchors or stopped the new bill", worked, err, w.Status())
	}
	var first, last monitor.PortalMonthlyBill
	if err := f.auth.First(&first, "group_id=1 AND month='2026-07'").Error; err != nil || first != old {
		t.Fatal("member change rewrote history", err)
	}
	if err := f.auth.First(&last, "group_id=1 AND month='2026-09'").Error; err != nil {
		t.Fatal(err)
	}
	var body struct {
		Members []struct {
			UserID  int64  `json:"user_id"`
			Opening string `json:"opening"`
			Closing string `json:"closing"`
		} `json:"members"`
	}
	if err := json.Unmarshal([]byte(last.Payload), &body); err != nil || len(body.Members) != 3 || body.Members[2].UserID != 303 || body.Members[2].Opening != "0.000000" || body.Members[2].Closing != "0.000000" {
		t.Fatal("new member omitted or assigned old money", body, err)
	}
}
