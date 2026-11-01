package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func walletReuseAssemblyFixture(t *testing.T) (string, monthlysource.MonthlyObservationFile, monthlysource.MonthlyObservationMaterial) {
	t.Helper()
	root, request := observationMaterialFixture(t)
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := assembleObservationMonth(t.Context(), root, "source-material", raw, true)
	if err != nil {
		t.Fatal(err)
	}
	m := readObservationMaterial(t, root, "source-material", receipt)
	return root, monthlysource.MonthlyObservationFile{Path: filepath.Join(root, "source-material", "materials.json"), SHA256: receipt.MaterialSHA256}, m
}

func TestWalletReuseAssemblyPreservesOriginalAndReauditsGeneration(t *testing.T) {
	root, original, m := walletReuseAssemblyFixture(t)
	before := map[string]string{}
	for _, path := range []string{original.Path, m.Database.Path, m.Tokens[0].File.Path, m.Tokens[1].File.Path} {
		before[path] = fileHash(t, path)
	}
	preview, err := assembleWalletReuse(t.Context(), root, "reused", original, false, writeObservationFile)
	if err != nil || preview.Complete || preview.Directory != "" || preview.Accounts != 2 {
		t.Fatal("reuse preflight failed", err)
	}
	if _, err := os.Stat(filepath.Join(root, "reused")); !os.IsNotExist(err) {
		t.Fatal("preflight wrote output", err)
	}
	receipt, err := assembleWalletReuse(t.Context(), root, "reused", original, true, writeObservationFile)
	if err != nil || !receipt.Complete || receipt.ManifestSHA256 != preview.ManifestSHA256 || receipt.MaterialSHA256 != preview.MaterialSHA256 {
		t.Fatal("reuse metadata did not match inspection", err)
	}
	raw, err := privateRead(filepath.Join(root, "reused", "materials.json"), 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	var reused monthlysource.MonthlyObservationMaterial
	if json.Unmarshal(raw, &reused) != nil || hash(raw) != receipt.MaterialSHA256 || reused.WalletReuse == nil || len(reused.WalletIDs) != 0 {
		t.Fatal("invalid new handoff")
	}
	metaRaw, err := privateRead(reused.WalletReuse.Path, monthlysource.WalletReuseMaterialLimit)
	if err != nil || hash(metaRaw) != receipt.ManifestSHA256 {
		t.Fatal("metadata seal not private/single-linked", err)
	}
	var meta monthlysource.WalletReuseMaterial
	if json.Unmarshal(metaRaw, &meta) != nil || len(meta.Accounts) != 2 || meta.Accounts[0].Selected[0].StartedAtMS == meta.Accounts[1].Selected[0].StartedAtMS {
		t.Fatal("independent account clocks were merged")
	}
	for p, sha := range before {
		if fileHash(t, p) != sha {
			t.Fatal("old source or database mutated")
		}
	}
	if _, err := assembleWalletReuse(t.Context(), root, "reused", original, true, writeObservationFile); err == nil {
		t.Fatal("existing output overwritten")
	}
	// This exercises the PRODUCT wallet-source reader and original exact-money
	// normalizers/generator against the actual newly written metadata files.
	testObservationMaterialGeneration(t, root, reused)
	if binary := os.Getenv("MONTHLY_CAPTURE_TEST_BINARY"); binary != "" {
		args := []string{"assemble-wallet-reuse", "-workspace", root, "-output", "actual-cli", "-material", original.Path, "-expected-material", original.SHA256}
		for _, create := range []bool{false, true} {
			current := append([]string{}, args...)
			if create {
				current = append(current, "-create-output")
			}
			raw, err := exec.CommandContext(t.Context(), binary, current...).CombinedOutput()
			var actual walletReuseReceipt
			if err != nil || json.Unmarshal(raw, &actual) != nil || actual.Complete != create || actual.ManifestSHA256 != receipt.ManifestSHA256 {
				t.Fatal("actual CLI handoff failed", err)
			}
		}
	}
}

func TestWalletReuseAssemblyInterruptedSourcesAndDiskNeverComplete(t *testing.T) {
	for _, kind := range []string{"source", "disk", "cancel", "corrupt", "directory"} {
		t.Run(kind, func(t *testing.T) {
			root, original, m := walletReuseAssemblyFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			write := func(dir *os.Root, name string, raw []byte) error {
				if kind == "disk" {
					return errors.New("synthetic disk full")
				}
				if err := writeObservationFile(dir, name, raw); err != nil {
					return err
				}
				switch kind {
				case "source":
					return os.WriteFile(m.Tokens[0].File.Path, []byte("changed"), 0600)
				case "cancel":
					cancel()
				case "corrupt":
					return os.WriteFile(filepath.Join(root, "failed", name), []byte("corrupt"), 0600)
				case "directory":
					if name != "wallet-reuse.pending.json" {
						return nil
					}
					if err := os.Rename(filepath.Join(root, "failed"), filepath.Join(root, "moved")); err != nil {
						return err
					}
					return os.Mkdir(filepath.Join(root, "failed"), 0700)
				}
				return nil
			}
			r, err := assembleWalletReuse(ctx, root, "failed", original, true, write)
			if err == nil || r.Complete || r.Directory == "" {
				t.Fatal("interrupted output completed", r, err)
			}
			if _, err := os.Stat(filepath.Join(root, "failed", "materials.json")); !os.IsNotExist(err) {
				t.Fatal("failure left completion marker", err)
			}
		})
	}
}

func TestWalletReuseAssemblyBadArgumentsAndDefaultCannotCapture(t *testing.T) {
	root, original, _ := walletReuseAssemblyFixture(t)
	base := []string{"assemble-wallet-reuse", "-workspace", root, "-output", "no-write", "-material", original.Path, "-expected-material", original.SHA256}
	for _, extra := range [][]string{{"-capture"}, {"-apply"}, {"-connection-file=/not-read"}, {"-expected-material="}, {"extra"}} {
		var out bytes.Buffer
		if err := run(t.Context(), append(append([]string{}, base...), extra...), &out); err == nil || out.Len() != 0 {
			t.Fatal("invalid/capture command accepted", err)
		}
	}
	var a, b bytes.Buffer
	if err := run(t.Context(), base, &a); err != nil {
		t.Fatal(err)
	}
	if err := run(t.Context(), base, &b); err != nil || !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatal("default unstable", err)
	}
	var r walletReuseReceipt
	if json.Unmarshal(a.Bytes(), &r) != nil || r.Complete || r.Directory != "" {
		t.Fatal("default wrote metadata")
	}
	if _, err := os.Stat(filepath.Join(root, "no-write")); !os.IsNotExist(err) {
		t.Fatal("default created files", err)
	}
	var absent context.Context // Deliberate invalid operator input, not a production call.
	if _, err := assembleWalletReuse(absent, root, "nil", original, false, writeObservationFile); err == nil {
		t.Fatal("nil context accepted")
	}
	if !reflect.DeepEqual(r.Mode, "checked_wallet_reuse_no_output") {
		t.Fatal("wrong mode")
	}
}
