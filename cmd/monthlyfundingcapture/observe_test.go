package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func observationFixture(t *testing.T, kind string) (string, observationRequest, []byte) {
	t.Helper()
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	r := observationRequest{Version: 1, Kind: kind, SourceIdentity: strings.Repeat("a", 64), UserIDs: []int64{201}}
	started := time.Now().UnixMilli() - 10
	q, used := int64(-1), int64(9007199254740993)
	wallet := monthlysource.WalletUser{UserID: 201, Exists: true, BalanceQuota: &q, UsedQuota: &used}
	var data any
	if kind == "observe-usage" {
		r.From, r.To = 1788192000, 1788195600
		data = monthlysource.UsageWindowObservation{Version: 1, Basis: "database_view_wallet_and_usage_not_month_boundary",
			Request: usageObservationRequest(r), StartedAtMS: started, CompletedAtMS: started + 1, Wallet: wallet,
			Rows: []monthlysource.UsageWindowRow{{UserID: 201, Type: 2, BillingSource: "wallet", Records: 1,
				Quota: used, MinQuota: used, MaxQuota: used, MinID: 11, MaxID: 11, FirstAt: r.From + 10, LastAt: r.From + 10},
				{UserID: 201, Type: 6, BillingSource: "wallet", Records: 1, Quota: 500000, MinQuota: 500000, MaxQuota: 500000,
					MinID: 12, MaxID: 12, FirstAt: r.From + 20, LastAt: r.From + 20}}}
	} else {
		data = monthlysource.WalletObservation{Version: 1, Basis: "database_observation_not_month_boundary", SourceIdentity: r.SourceIdentity,
			StartedAtMS: started, CompletedAtMS: started + 1, Users: []monthlysource.WalletUser{wallet}}
	}
	raw, err := json.Marshal(data)
	if err != nil || validateObservation(raw, r) != nil {
		t.Fatal("invalid synthetic observation", err)
	}
	return workspace, r, raw
}

func TestObservationCommandDefaultAndPrivateCapture(t *testing.T) {
	for _, kind := range []string{"observe-usage", "observe-wallet"} {
		t.Run(kind, func(t *testing.T) {
			workspace, r, raw := observationFixture(t, kind)
			calls := 0
			collect := func(ctx context.Context, connection string, got observationRequest) ([]byte, error) {
				calls++
				if _, ok := ctx.Deadline(); !ok || connection != "/never-read-by-fixture.json" {
					t.Fatal("unbounded capture or connection changed")
				}
				b, _ := json.Marshal(got)
				want, _ := json.Marshal(r)
				if !bytes.Equal(b, want) {
					t.Fatal("capture scope changed")
				}
				return raw, nil
			}
			for range 2 {
				got, err := observeOne(context.Background(), workspace, "one", "/never-read-by-fixture.json", r, false, collect)
				if err != nil || got.Complete || got.Directory != "" || calls != 0 || got.Mode != "checked_local_output_no_source_connection" {
					t.Fatal("default had side effects", got, err)
				}
			}
			entries, err := os.ReadDir(workspace)
			if err != nil || len(entries) != 0 {
				t.Fatal("default created files or lock", err)
			}
			got, err := observeOne(context.Background(), workspace, "one", "/never-read-by-fixture.json", r, true, collect)
			if err != nil || !got.Complete || calls != 1 || got.ObservationSHA256 != hash(raw) {
				t.Fatal("capture failed", got, err)
			}
			for _, name := range []string{"request.json", "observation.json", "manifest.pending.json", "manifest.json"} {
				info, err := os.Lstat(filepath.Join(got.Directory, name))
				if err != nil || info.Mode().Perm() != 0600 || !info.Mode().IsRegular() {
					t.Fatal("nonprivate output", name, err)
				}
			}
			manifest, err := os.ReadFile(filepath.Join(got.Directory, "manifest.json"))
			var stored observationReceipt
			if err != nil || json.Unmarshal(manifest, &stored) != nil || stored != got {
				t.Fatal("completion marker differs from receipt", err)
			}
			b, err := os.ReadFile(filepath.Join(got.Directory, "observation.json"))
			if err != nil || !bytes.Equal(b, raw) {
				t.Fatal("raw integer amounts or refund changed", err)
			}
			// The files preserve the original typed format. Exercise the real
			// append-only SQLite consumer, not a JSON-only test replacement.
			observationRoundTripStore(t, kind, b)
			if _, err := observeOne(context.Background(), workspace, "one", "unused", r, true, collect); err == nil || calls != 1 {
				t.Fatal("existing output overwritten or recaptured")
			}
		})
	}
}

func observationRoundTripStore(t *testing.T, kind string, raw []byte) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "observations.sqlite")
	if err := exclusiveFile(path, nil); err != nil {
		t.Fatal(err)
	}
	db, err := openReceiver(context.Background(), path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if kind == "observe-usage" {
		var o monthlysource.UsageWindowObservation
		if err := json.Unmarshal(raw, &o); err != nil {
			t.Fatal(err)
		}
		if err := monthlysource.InitUsageWindowStore(ctx, db); err != nil {
			t.Fatal(err)
		}
		id, err := monthlysource.SaveUsageWindowObservation(ctx, db, o)
		if err != nil || id != hash(raw) {
			t.Fatal("typed usage write", err)
		}
		got, err := monthlysource.LoadUsageWindowObservation(ctx, db, id)
		b, _ := json.Marshal(got)
		if err != nil || !bytes.Equal(raw, b) {
			t.Fatal("typed usage changed", err)
		}
	} else {
		var o monthlysource.WalletObservation
		if err := json.Unmarshal(raw, &o); err != nil {
			t.Fatal(err)
		}
		if err := monthlysource.InitWalletObservationStore(ctx, db); err != nil {
			t.Fatal(err)
		}
		id, err := monthlysource.SaveWalletObservation(ctx, db, o)
		if err != nil || id != hash(raw) {
			t.Fatal("typed wallet write", err)
		}
		got, err := monthlysource.LoadWalletObservation(ctx, db, id)
		b, _ := json.Marshal(got)
		if err != nil || !bytes.Equal(raw, b) {
			t.Fatal("typed wallet changed", err)
		}
	}
}

func TestObservationCommandRejectsInvalidAndChangedCapture(t *testing.T) {
	for _, scenario := range []string{"source", "duplicate", "usage_users", "usage_range", "future", "wallet_range", "canceled", "capture_error", "wrong_capture", "wrong_source", "duplicate_json", "unknown_json", "noncanonical", "capture_canceled", "busy", "public_root", "symlink_root"} {
		t.Run(scenario, func(t *testing.T) {
			workspace, r, raw := observationFixture(t, "observe-usage")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			collect := func(context.Context, string, observationRequest) ([]byte, error) {
				calls++
				if scenario == "capture_error" {
					return nil, errors.New("synthetic source failure")
				}
				if scenario == "capture_canceled" {
					cancel()
				}
				return raw, nil
			}
			switch scenario {
			case "source":
				r.SourceIdentity = "bad"
			case "duplicate":
				r.UserIDs = []int64{201, 201}
			case "usage_users":
				r.UserIDs = []int64{201, 202}
			case "usage_range":
				r.To = r.From + 86401
			case "future":
				r.From, r.To = time.Now().Unix()-2, time.Now().Unix()-1
			case "wallet_range":
				r.Kind = "observe-wallet"
			case "canceled":
				cancel()
			case "wrong_capture":
				raw = bytes.Replace(raw, []byte(`"user_id":201`), []byte(`"user_id":202`), 1)
			case "wrong_source":
				raw = bytes.ReplaceAll(raw, []byte(r.SourceIdentity), []byte(strings.Repeat("b", 64)))
			case "duplicate_json":
				raw = bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
			case "unknown_json":
				raw = bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"complete":true`), 1)
			case "noncanonical":
				raw = append(raw, '\n')
			case "busy":
				lock, err := workspaceLock(workspace, true, true)
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Close()
			case "public_root":
				if err := os.Chmod(workspace, 0755); err != nil {
					t.Fatal(err)
				}
			case "symlink_root":
				alias := filepath.Join(t.TempDir(), "alias")
				if err := os.Symlink(workspace, alias); err != nil {
					t.Fatal(err)
				}
				workspace = alias
			}
			got, err := observeOne(ctx, workspace, "one", "unused", r, true, collect)
			if err == nil || got.Complete || got.Directory != "" {
				t.Fatal("bad capture accepted", got, err)
			}
			if scenario != "capture_error" && scenario != "wrong_capture" && scenario != "wrong_source" && scenario != "duplicate_json" && scenario != "unknown_json" && scenario != "noncanonical" && scenario != "capture_canceled" && calls != 0 {
				t.Fatal("invalid request connected to source")
			}
			if _, err := os.Lstat(filepath.Join(workspace, "one")); !os.IsNotExist(err) {
				t.Fatal("invalid capture retained output", err)
			}
		})
	}
}

func TestObservationCommandFailedOutputDoesNotSeal(t *testing.T) {
	for _, scenario := range []string{"disk", "late_corruption", "foreign_marker", "directory_replaced", "hardlink", "symlink", "public_file", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			workspace, r, raw := observationFixture(t, "observe-wallet")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			collect := func(context.Context, string, observationRequest) ([]byte, error) { return raw, nil }
			write := func(dir *os.Root, name string, b []byte) error {
				if scenario == "disk" && name == "observation.json" {
					return errors.New("synthetic disk full")
				}
				if err := writeObservationFile(dir, name, b); err != nil {
					return err
				}
				if name == "manifest.pending.json" {
					switch scenario {
					case "late_corruption":
						f, err := dir.OpenFile("request.json", os.O_TRUNC|os.O_WRONLY, 0600)
						if err != nil {
							return err
						}
						_, err = f.Write([]byte("damaged"))
						return errors.Join(err, f.Close())
					case "foreign_marker":
						return writeObservationFile(dir, "manifest.json", []byte("foreign"))
					case "directory_replaced":
						return os.Rename(filepath.Join(workspace, "one"), filepath.Join(workspace, "moved"))
					case "hardlink":
						return dir.Link("observation.json", "alias.json")
					case "symlink":
						if err := dir.Remove("observation.json"); err != nil {
							return err
						}
						return dir.Symlink("request.json", "observation.json")
					case "public_file":
						return dir.Chmod("observation.json", 0644)
					case "canceled":
						cancel()
					}
				}
				return nil
			}
			got, err := observeOneWithWrite(ctx, workspace, "one", "unused", r, true, collect, write)
			if err == nil || got.Complete || got.Directory == "" || got.Mode != "incomplete_private_observation_do_not_use" {
				t.Fatal("failed output appeared complete", got, err)
			}
			if scenario == "foreign_marker" {
				b, err := os.ReadFile(filepath.Join(workspace, "one", "manifest.json"))
				if err != nil || string(b) != "foreign" {
					t.Fatal("foreign file overwritten", err)
				}
			} else if _, err := os.Lstat(filepath.Join(workspace, "one", "manifest.json")); !os.IsNotExist(err) {
				t.Fatal("failure left valid completion marker", err)
			}
		})
	}
}

func TestObservationCLIControlsAndDefaultNoCredentials(t *testing.T) {
	workspace, _, _ := observationFixture(t, "observe-wallet")
	args := []string{"observe-wallet", "-workspace", workspace, "-output", "one", "-source-identity", strings.Repeat("a", 64), "-user-ids", "201", "-connection-file", "/nonexistent/secret.json"}
	var out bytes.Buffer
	if err := run(context.Background(), args, &out); err != nil {
		t.Fatal("default read credentials", err)
	}
	var r observationReceipt
	if json.Unmarshal(out.Bytes(), &r) != nil || r.Complete || r.Mode != "checked_local_output_no_source_connection" {
		t.Fatal("bad CLI receipt")
	}
	for _, more := range [][]string{{"-unknown-secret", "not-to-echo"}, {"-from", "1"}, {"-output", "../outside"}, {"-user-ids", "201,201"}, {"-capture"}} {
		out.Reset()
		if err := run(context.Background(), append(args, more...), &out); err == nil || strings.Contains(out.String(), "secret") {
			t.Fatal("invalid flags or failed capture leaked configuration", err)
		}
	}
	if _, err := os.Stat(filepath.Join(workspace, "one")); !os.IsNotExist(err) {
		t.Fatal("CLI failure created output", err)
	}
}

func TestObservationWalletMissingAccountStaysUnknown(t *testing.T) {
	_, r, raw := observationFixture(t, "observe-wallet")
	var o monthlysource.WalletObservation
	if err := json.Unmarshal(raw, &o); err != nil {
		t.Fatal(err)
	}
	r.UserIDs = []int64{202, 201}
	o.Users = append(o.Users, monthlysource.WalletUser{UserID: 202, Exists: false})
	raw, err := json.Marshal(o)
	if err != nil || validateObservation(raw, r) != nil || o.Users[1].BalanceQuota != nil || o.Users[1].UsedQuota != nil {
		t.Fatal("missing account changed into a zero balance", err)
	}
	o.Users[1].UserID = 203
	raw, _ = json.Marshal(o)
	if validateObservation(raw, r) == nil {
		t.Fatal("different account substituted")
	}
}

func TestObservationCLIRealBinaryDefaults(t *testing.T) {
	binary := os.Getenv("MONTHLY_CAPTURE_TEST_BINARY")
	if binary == "" {
		t.Skip("explicit actual capture binary required")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("absolute binary required")
	}
	t.Run("token_wallet_reuse", func(t *testing.T) { testTokenWalletReuseActualCLI(t, binary) })
	for _, kind := range []string{"observe-usage", "observe-wallet"} {
		workspace, r, _ := observationFixture(t, kind)
		args := []string{kind, "-workspace", workspace, "-output", "one", "-source-identity", r.SourceIdentity, "-user-ids", "201", "-connection-file", "/nonexistent/private-observation-config.json"}
		if kind == "observe-usage" {
			args = append(args, "-from", "1788192000", "-to", "1788195600")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var stdout, stderr bytes.Buffer
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		cancel()
		var receipt observationReceipt
		if err != nil || stderr.Len() != 0 || json.Unmarshal(stdout.Bytes(), &receipt) != nil || receipt.Complete || receipt.Mode != "checked_local_output_no_source_connection" {
			t.Fatal("actual default command failed", kind, err)
		}
		entries, err := os.ReadDir(workspace)
		if err != nil || len(entries) != 0 {
			t.Fatal("actual default created files", err)
		}
	}
}
