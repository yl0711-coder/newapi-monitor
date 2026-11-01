package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func tokenObservationFixture(t *testing.T) (string, observationRequest, []byte) {
	t.Helper()
	workspace, r, raw := observationFixture(t, "observe-usage")
	var old monthlysource.UsageWindowObservation
	if err := json.Unmarshal(raw, &old); err != nil {
		t.Fatal(err)
	}
	r.Kind = "observe-token-usage"
	o := monthlysource.TokenUsageWindowObservation{Version: 1, Basis: "database_view_wallet_usage_and_token_groups_not_month_boundary", Request: old.Request, StartedAtMS: old.StartedAtMS, CompletedAtMS: old.CompletedAtMS, Wallet: old.Wallet, Totals: old.Rows, Rows: []monthlysource.TokenUsageWindowRow{}}
	for _, row := range old.Rows {
		o.Rows = append(o.Rows, monthlysource.TokenUsageWindowRow{UsageWindowRow: row, TokenID: 12, Name: "same token"})
	}
	raw, err := json.Marshal(o)
	if err != nil || validateObservation(raw, r) != nil {
		t.Fatal("invalid synthetic token observation", err)
	}
	return workspace, r, raw
}

func TestTokenCaptureSafeDiagnosticsPreserveCancellationWithoutValues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, raw := tokenObservationFixture(t)
	var o monthlysource.TokenUsageWindowObservation
	if json.Unmarshal(raw, &o) != nil {
		t.Fatal("fixture")
	}
	db, err := sql.Open("mysql", "readonly@tcp(127.0.0.1:1)/not_used")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close() // No Ping/query: canceled context must fail before source IO.
	_, err = monthlysource.CaptureTokenUsageWindow(ctx, db, o.Request)
	msg := safeError(err)
	if !errors.Is(err, context.Canceled) || !strings.Contains(msg, "stage=input") || !strings.Contains(msg, "class=canceled") || !strings.Contains(msg, "未返回部分结果") {
		t.Fatal("safe stage missing", msg)
	}
	for _, secret := range []string{"monitor_ro:secret@tcp", "SELECT private_column", "private diagnostic"} {
		if strings.Contains(safeError(errors.Join(err, errors.New(secret))), secret) {
			t.Fatal("driver/input diagnostic leaked")
		}
	}
}

func TestTokenCaptureSafeDiagnosticsIncludeOnlyFixedValidationReason(t *testing.T) {
	_, _, raw := tokenObservationFixture(t)
	var o monthlysource.TokenUsageWindowObservation
	_ = json.Unmarshal(raw, &o)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db, err := sql.Open("mysql", "readonly@tcp(127.0.0.1:1)/not_used")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, captureErr := monthlysource.CaptureTokenUsageWindow(ctx, db, o.Request)
	o.CompletedAtMS = o.StartedAtMS + 5001
	validationErr := monthlysource.ValidateTokenUsageWindowObservation(o)
	msg := safeError(errors.Join(captureErr, validationErr, errors.New("secret SQL and token values")))
	if !strings.Contains(msg, "reason=observation_timing") || !strings.Contains(msg, "class=canceled") || strings.Contains(msg, "secret") || strings.Contains(msg, "5001") {
		t.Fatal("unsafe or ambiguous diagnostic", msg)
	}
}

func TestTokenObservationDefaultCaptureRetainedReadAndNoOverwrite(t *testing.T) {
	workspace, r, raw := tokenObservationFixture(t)
	calls := 0
	collect := func(ctx context.Context, connection string, got observationRequest) ([]byte, error) {
		calls++
		if _, ok := ctx.Deadline(); !ok || got.Kind != r.Kind || connection != "/synthetic-never-read.json" {
			t.Fatal("invalid bounded capture")
		}
		return raw, nil
	}
	if receipt, err := observeOne(context.Background(), workspace, "tokens", "/synthetic-never-read.json", r, false, collect); err != nil || receipt.Complete || calls != 0 {
		t.Fatal("default captured source", receipt, err)
	}
	entries, err := os.ReadDir(workspace)
	if err != nil || len(entries) != 0 {
		t.Fatal("default wrote output", err)
	}
	receipt, err := observeOne(context.Background(), workspace, "tokens", "/synthetic-never-read.json", r, true, collect)
	if err != nil || !receipt.Complete || calls != 1 || receipt.ObservationSHA256 != hash(raw) {
		t.Fatal("token capture not sealed", receipt, err)
	}
	manifest, err := os.ReadFile(filepath.Join(receipt.Directory, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	ref := assemblyCapture{Directory: receipt.Directory, ManifestSHA256: hash(manifest)}
	loaded, err := readAssemblyCapture(ref, "observe-token-usage")
	if err != nil || !bytes.Equal(loaded, raw) {
		t.Fatal("sealed original did not roundtrip", err)
	}
	if _, err := readAssemblyCapture(ref, "observe-usage"); err == nil {
		t.Fatal("token capture reinterpreted as old usage capture")
	}
	if _, err := observeOne(context.Background(), workspace, "tokens", "/synthetic-never-read.json", r, true, collect); err == nil || calls != 1 {
		t.Fatal("existing token capture overwritten")
	}
	if err := os.WriteFile(filepath.Join(receipt.Directory, "observation.json"), bytes.Replace(raw, []byte("same token"), []byte("tampered!!"), 1), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readAssemblyCapture(ref, "observe-token-usage"); err == nil {
		t.Fatal("modified source accepted")
	}
}

func TestTokenObservationFailuresCannotSealOrWidenScope(t *testing.T) {
	for _, scenario := range []string{"transport", "token_total_changed", "missing_id", "unknown_field", "partial_write", "many_users", "too_long"} {
		t.Run(scenario, func(t *testing.T) {
			workspace, r, raw := tokenObservationFixture(t)
			if scenario == "many_users" {
				r.UserIDs = append(r.UserIDs, 202)
			}
			if scenario == "too_long" {
				r.To = r.From + 86401
			}
			collect := func(context.Context, string, observationRequest) ([]byte, error) {
				switch scenario {
				case "transport":
					return nil, errors.New("synthetic transport failure")
				case "token_total_changed":
					var o monthlysource.TokenUsageWindowObservation
					_ = json.Unmarshal(raw, &o)
					o.Rows[0].Quota++
					return json.Marshal(o)
				case "missing_id":
					return bytes.Replace(raw, []byte(`,"token_id":12`), nil, 1), nil
				case "unknown_field":
					return append(raw[:len(raw)-1], []byte(`,"reviewed":true}`)...), nil
				default:
					return raw, nil
				}
			}
			write := writeObservationFile
			if scenario == "partial_write" {
				write = func(root *os.Root, name string, b []byte) error {
					if name == "manifest.pending.json" {
						return errors.New("synthetic disk failure")
					}
					return writeObservationFile(root, name, b)
				}
			}
			receipt, err := observeOneWithWrite(context.Background(), workspace, "failed", "synthetic", r, true, collect, write)
			if err == nil || receipt.Complete {
				t.Fatal("bad token capture sealed", err)
			}
			if _, err := os.Stat(filepath.Join(workspace, "failed", "manifest.json")); !os.IsNotExist(err) {
				t.Fatal("completion marker exists after failure")
			}
		})
	}
	workspace, r, _ := tokenObservationFixture(t)
	var out bytes.Buffer
	err := run(context.Background(), []string{"observe-token-usage", "-workspace", workspace, "-output", "dry", "-source-identity", r.SourceIdentity, "-user-ids", "201", "-from", "1788192000", "-to", "1788195600"}, &out)
	if err != nil || !strings.Contains(out.String(), "checked_local_output_no_source_connection") {
		t.Fatal("new CLI dispatch failed", err, out.String())
	}
}

func TestTokenObservationLargePayloadRetainsOriginalLimitIsolation(t *testing.T) {
	workspace, r, raw := tokenObservationFixture(t)
	var o monthlysource.TokenUsageWindowObservation
	if err := json.Unmarshal(raw, &o); err != nil {
		t.Fatal(err)
	}
	base := o.Totals[0]
	base.Records, base.Quota, base.MinQuota, base.MaxQuota, base.MinID, base.MaxID = 200, 200, 1, 1, 100, 299
	o.Totals[0] = base
	o.Rows = nil
	for i := 0; i < 200; i++ {
		row := base
		row.Records, row.Quota, row.MinID, row.MaxID = 1, 1, int64(100+i), int64(100+i)
		o.Rows = append(o.Rows, monthlysource.TokenUsageWindowRow{UsageWindowRow: row, TokenID: int64(i + 1), Name: strings.Repeat("x", 512)})
	}
	o.Rows = append(o.Rows, monthlysource.TokenUsageWindowRow{UsageWindowRow: o.Totals[1], TokenID: 201, Name: "refund"})
	large, err := json.Marshal(o)
	if err != nil || len(large) <= 65536 || validateObservation(large, r) != nil {
		t.Fatal("large valid token fixture rejected", err)
	}
	receipt, err := observeOne(context.Background(), workspace, "large", "synthetic", r, true, func(context.Context, string, observationRequest) ([]byte, error) { return large, nil })
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile(filepath.Join(receipt.Directory, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := readAssemblyCapture(assemblyCapture{Directory: receipt.Directory, ManifestSHA256: hash(manifest)}, r.Kind)
	if err != nil || !bytes.Equal(loaded, large) {
		t.Fatal("large token original not retained", err)
	}
	legacy := r
	legacy.Kind = "observe-usage"
	if err := validateObservation(large, legacy); err == nil {
		t.Fatal("old observation limit/schema was relaxed")
	}
}
