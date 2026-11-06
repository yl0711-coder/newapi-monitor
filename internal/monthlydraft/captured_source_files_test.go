package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

// Synthetic original records only. Never load private retained data, source
// credentials or production. The test deliberately does NOT assert historical
// month balances: none can be obtained from these current wallet observations.
func capturedFilesFixture(t *testing.T, tokenEdits ...func(*monthlysource.TokenUsageWindowObservation)) (generationFixture, *gorm.DB, *WalletSourcePreparation, SourcePreparationRequest, *sourcePreparationDriver) {
	t.Helper()
	f, auth, _, p, driver := walletPreparationFixture(t)
	ctx := t.Context()
	req := SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}
	scope, err := p.funding.scope(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := monthlysource.InitTokenMonthCaptureStore(ctx, p.funding.capture); err != nil {
		t.Fatal(err)
	}
	plan, err := monthlysource.EnsureFundingMonthCapture(ctx, p.funding.capture, req.Key(), scope)
	if err != nil {
		t.Fatal(err)
	}
	for i, scan := range plan.Scans {
		at := time.Now().Add(-time.Hour).UnixMilli() + int64(i)*1000
		page := monthlysource.FundingPage{Version: 1, Scan: scan, FenceID: 100000000, EndReached: true, Rows: []monthlysource.FundingLog{}, StartedAtMS: at, CompletedAtMS: at + 1}
		if _, err := monthlysource.SaveFundingPage(ctx, p.funding.capture, page); err != nil {
			t.Fatal(err)
		}
	}
	f.plan.Funding.SourceIdentity = scope.Funding.SourceIdentity
	addTokenArchives(t, &f)
	archives := map[int64]monthlysource.TokenUsageMonthArchive{}
	for _, id := range scope.Funding.UserIDs {
		raw, err := os.ReadFile(filepath.Join(f.root, "tokens-"+stringID(id)+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var archive monthlysource.TokenUsageMonthArchive
		if json.Unmarshal(raw, &archive) != nil {
			t.Fatal("invalid fixture")
		}
		archives[id] = archive
	}
	ordinal := 0
	for phase := 0; phase < 2; phase++ {
		for day := 0; day < len(plan.Scans)/2; day++ {
			for _, id := range scope.Funding.UserIDs {
				o := archives[id].Pairs[day].Older
				if phase == 1 {
					o = archives[id].Pairs[day].Newer
					o.StartedAtMS += 60000
					o.CompletedAtMS += 60000
				}
				for _, edit := range tokenEdits {
					edit(&o) // Synthetic original creation only; never update retained slots.
				}
				u, err := monthlysource.UsageObservationFromToken(o)
				if err != nil {
					t.Fatal(err)
				}
				uid, err := monthlysource.SaveUsageWindowObservation(ctx, p.funding.capture, u)
				if err != nil {
					t.Fatal(err)
				}
				raw := materialPreparationJSON(t, o)
				if _, err := p.funding.capture.Exec(`INSERT INTO portal_monthly_token_capture_slots(job_key,ordinal,user_id,sha256,usage_id,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?,?,?,?)`, req.Key(), ordinal, id, generationHash(raw), uid, o.StartedAtMS, o.CompletedAtMS, raw); err != nil {
					t.Fatal(err)
				}
				ordinal++
			}
		}
	}
	for i := 0; i < 2; i++ {
		at := time.Now().Add(-time.Duration(10-i*6) * time.Minute).UnixMilli()
		o := monthlysource.WalletObservation{Version: 1, Basis: "database_observation_not_month_boundary", SourceIdentity: scope.Funding.SourceIdentity, StartedAtMS: at, CompletedAtMS: at + 1, Users: []monthlysource.WalletUser{}}
		for _, id := range scope.Funding.UserIDs {
			balance, used := int64(123456789+i), int64(9007199254740993)+int64(i)
			o.Users = append(o.Users, monthlysource.WalletUser{UserID: id, Exists: true, BalanceQuota: &balance, UsedQuota: &used})
		}
		id, err := monthlysource.SaveWalletObservation(ctx, p.funding.capture, o)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.funding.capture.Exec(`INSERT INTO portal_monthly_wallet_capture_slots(job_key,ordinal,observation_id) VALUES(?,?,?)`, req.Key(), i, id); err != nil {
			t.Fatal(err)
		}
	}
	return f, auth, p, req, driver
}

func stringID(id int64) string { return strconv.FormatInt(id, 10) }

func privateCapturedOutput(t *testing.T) string {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func TestCapturedSourceFilesSealReuseExactMoneyAndOldReader(t *testing.T) {
	f, _, p, req, driver := capturedFilesFixture(t)
	directory := privateCapturedOutput(t)
	before, err := p.AssembleCapturedSources(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := p.SealCapturedSources(t.Context(), req, directory)
	if err != nil || sealed.Mode != capturedSourceFilesMode {
		t.Fatal(sealed, err)
	}
	again, err := p.SealCapturedSources(t.Context(), req, directory)
	if err != nil || !reflect.DeepEqual(sealed, again) {
		t.Fatal("restart changed sealed source", err)
	}
	after, err := p.AssembleCapturedSources(t.Context(), req)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("original captures changed", err)
	}
	materialRaw, err := os.ReadFile(sealed.Material.Path)
	if err != nil || generationHash(materialRaw) != sealed.Material.SHA256 {
		t.Fatal(err)
	}
	var m monthlysource.MonthlyObservationMaterial
	if json.Unmarshal(materialRaw, &m) != nil || m.Mode != monthlysource.MonthlyObservationMaterialMode || m.FundingVersion != 2 || len(m.Tokens) != 2 || len(m.UsageWindows) != 60 {
		t.Fatal("incompatible material", m)
	}
	for _, token := range m.Tokens {
		raw, err := os.ReadFile(token.File.Path)
		if err != nil {
			t.Fatal(err)
		}
		archive, report, err := monthlysource.DecodeTokenUsageMonthArchive(t.Context(), raw, time.Now())
		if err != nil || report.ConsumeQuota != 1500000 || report.RefundQuota != 500000 || len(report.Rows) != 3 || archive.SelectionSHA256 != sealed.Selection.SHA256 {
			t.Fatal("account money/IDs changed", report, err)
		}
	}
	// Existing consumer can bind the copy without migrating it or falling
	// back to its default DB. This is NOT an approval of historical balances.
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	g := *f.g
	g.root = root
	input := monitor.MonthlyPreparationInput{Sources: []monitor.MonthlyPreparationSource{{ID: "copy", Path: m.Database.Path, SHA256: m.Database.SHA256}}}
	plan := GenerationPlan{ObservationStoreSourceID: "copy", WalletObservations: &WalletObservationSelection{SourceID: "copy", IDs: m.WalletIDs}}
	scoped, release, err := g.bindObservationStore(t.Context(), plan, input)
	if err != nil {
		t.Fatal("old sealed-source reader rejected copy", err)
	}
	release()
	if scoped == &g || driver.queries.Load() != 0 || driver.writes.Load() != 0 || driver.pings.Load() != 0 {
		t.Fatal("seal queried source or used default DB")
	}
	generationAdmissionCounts(t, f.receiver, 0, 0)
}

func TestCapturedSourceFilesFailureRetainedNeverPublished(t *testing.T) {
	for _, scenario := range []string{"disk", "token_write", "revoke", "directory_moved", "token_link", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			f, auth, p, req, _ := capturedFilesFixture(t)
			directory := privateCapturedOutput(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fired := false
			write := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
				if err := writeArchiveFile(ctx, root, name, raw); err != nil {
					return err
				}
				when := "selection.json"
				if scenario == "token_write" {
					when = "tokens-201.json"
				}
				if fired || name != when {
					return nil
				}
				fired = true
				switch scenario {
				case "disk", "token_write":
					return errors.New("synthetic disk failure")
				case "revoke":
					return auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error
				case "directory_moved":
					if err := os.Rename(directory, directory+"-retained"); err != nil {
						return err
					}
					return os.Mkdir(directory, 0700)
				case "token_link":
					return os.Symlink(filepath.Join(f.root, "tokens-201.json"), filepath.Join(directory, "tokens-201.json"))
				case "cancel":
					cancel()
				}
				return nil
			}
			got, err := p.sealCapturedSources(ctx, req, directory, write)
			if err == nil || got.Material.SHA256 != "" {
				t.Fatal("failed seal accepted", err)
			}
			if scenario == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			for _, dir := range []string{directory, directory + "-retained"} {
				if _, err := os.Lstat(filepath.Join(dir, "materials.json")); !os.IsNotExist(err) {
					t.Fatal("failed package completed", err)
				}
			}
			if scenario == "disk" {
				original, err := os.ReadFile(filepath.Join(directory, "selection.json"))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := p.SealCapturedSources(t.Context(), req, directory); err == nil {
					t.Fatal("partial directory overwritten")
				}
				retained, err := os.ReadFile(filepath.Join(directory, "selection.json"))
				if err != nil || !bytes.Equal(original, retained) {
					t.Fatal("partial evidence lost", err)
				}
				if _, err := p.SealCapturedSources(t.Context(), req, privateCapturedOutput(t)); err != nil {
					t.Fatal("fresh retry failed", err)
				}
			}
			generationAdmissionCounts(t, f.receiver, 0, 0)
		})
	}
}

func TestCapturedSourceFilesRejectChangedCompleteAndOutputAliases(t *testing.T) {
	_, _, p, req, _ := capturedFilesFixture(t)
	dir := privateCapturedOutput(t)
	sealed, err := p.SealCapturedSources(t.Context(), req, dir)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(sealed.Material.Path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sealed.Material.Path, append(raw, ' '), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := p.SealCapturedSources(t.Context(), req, dir); err == nil {
		t.Fatal("changed complete manifest repaired/accepted")
	}
	alias := filepath.Join(privateCapturedOutput(t), "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	if _, err := p.SealCapturedSources(t.Context(), req, alias); err == nil {
		t.Fatal("symlink output accepted")
	}
	root, lock, err := openCapturedOutput(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	defer lock.Close()
	if _, err := p.SealCapturedSources(t.Context(), req, dir); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("simultaneous writer accepted", err)
	}
}

func TestCapturedSourceFilesRefusesLostOrAliasedEvidence(t *testing.T) {
	_, _, p, req, _ := capturedFilesFixture(t)
	for _, scenario := range []string{"token_missing", "token_changed", "database_hardlink", "sidecar", "public_permissions", "unexpected_file"} {
		t.Run(scenario, func(t *testing.T) {
			dir := privateCapturedOutput(t)
			sealed, err := p.SealCapturedSources(t.Context(), req, dir)
			if err != nil {
				t.Fatal(err)
			}
			var m monthlysource.MonthlyObservationMaterial
			raw, err := os.ReadFile(sealed.Material.Path)
			if err != nil || json.Unmarshal(raw, &m) != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "token_missing":
				err = os.Remove(m.Tokens[0].File.Path)
			case "token_changed":
				err = os.WriteFile(m.Tokens[0].File.Path, []byte("{}"), 0600)
			case "database_hardlink":
				err = os.Link(m.Database.Path, filepath.Join(privateCapturedOutput(t), "alias.sqlite"))
			case "sidecar":
				err = os.WriteFile(m.Database.Path+"-wal", nil, 0600)
			case "public_permissions":
				err = os.Chmod(m.Database.Path, 0644)
			case "unexpected_file":
				err = os.WriteFile(filepath.Join(dir, "unexpected.json"), []byte("{}"), 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if got, err := p.SealCapturedSources(t.Context(), req, dir); err == nil || got.Material.SHA256 != "" {
				t.Fatal("damaged output accepted or repaired", err)
			}
		})
	}
}

func TestCapturedSourceFilesCompletedBeforeCancellationCanBeRevalidated(t *testing.T) {
	_, _, p, req, _ := capturedFilesFixture(t)
	dir := privateCapturedOutput(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	write := func(ctx context.Context, root *os.Root, name string, raw []byte) error {
		if err := writeArchiveFile(ctx, root, name, raw); err != nil {
			return err
		}
		if name == "materials.json" {
			cancel()
		}
		return nil
	}
	if got, err := p.sealCapturedSources(ctx, req, dir, write); !errors.Is(err, context.Canceled) || got.Material.SHA256 != "" {
		t.Fatal("canceled operation claimed success", err)
	}
	// Only an actually complete, unchanged set may be recovered. No source
	// reread, incomplete-file overwrite or customer publication is performed.
	if got, err := p.SealCapturedSources(t.Context(), req, dir); err != nil || got.Material.SHA256 == "" {
		t.Fatal("complete restart could not recover", err)
	}
}
