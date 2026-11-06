package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

type walletReuseReceipt struct {
	Mode           string `json:"mode"`
	Accounts       int    `json:"accounts"`
	ManifestSHA256 string `json:"manifest_sha256"`
	MaterialSHA256 string `json:"material_sha256"`
	Directory      string `json:"directory,omitempty"`
	Complete       bool   `json:"complete"`
}

func runWalletReuseMaterial(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("assemble-wallet-reuse", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("material", "", "pinned ORIGINAL data-only materials.json")
	expected := fs.String("expected-material", "", "original material SHA256")
	workspace := fs.String("workspace", "", "private canonical parent for NEW local bundle")
	name := fs.String("output", "", "new bundle name; never overwritten")
	create := fs.Bool("create-output", false, "create NEW source metadata only; default writes nothing")
	if fs.Parse(args) != nil || fs.NArg() != 0 || !validSHA(*expected) {
		return errInput
	}
	r, err := assembleWalletReuse(ctx, *workspace, *name, monthlysource.MonthlyObservationFile{Path: *path, SHA256: *expected}, *create, writeObservationFile)
	if err != nil && r.Directory == "" {
		return err
	}
	return errors.Join(err, json.NewEncoder(out).Encode(r))
}

func walletReuseInputs(ctx context.Context, original monthlysource.MonthlyObservationFile) (monthlysource.MonthlyObservationMaterial, monthlysource.WalletReuseMaterial, error) {
	raw, err := privateRead(original.Path, monthlysource.WalletReuseMaterialLimit)
	if err != nil {
		return monthlysource.MonthlyObservationMaterial{}, monthlysource.WalletReuseMaterial{}, err
	}
	m, err := monthlysource.DecodeWalletReuseOrigin(ctx, raw, original)
	if err != nil {
		return m, monthlysource.WalletReuseMaterial{}, err
	}
	if err := verifyObservationMaterialFiles(ctx, m); err != nil {
		return m, monthlysource.WalletReuseMaterial{}, err
	}
	archives := map[int64][]byte{}
	info, err := privateInfo(m.Database.Path, false)
	if err != nil {
		return m, monthlysource.WalletReuseMaterial{}, err
	}
	used := info.Size() + int64(len(raw))
	for _, f := range m.Tokens {
		remaining := int64(128<<20) - used
		if remaining <= 0 {
			return m, monthlysource.WalletReuseMaterial{}, errInput
		}
		b, err := privateRead(f.File.Path, min(remaining, int64(monthlysource.TokenUsageArchiveLimit)))
		if err != nil {
			return m, monthlysource.WalletReuseMaterial{}, err
		}
		used += int64(len(b))
		archives[f.UserID] = b
	}
	meta, err := monthlysource.BuildWalletReuseMaterial(ctx, raw, original, archives, time.Now())
	return m, meta, err
}

// Writes only a NEW local directory. No SQLite writes or recapture, no task
// selection/publication. Sources are reread before the last completion marker.
func assembleWalletReuse(parent context.Context, workspace, name string, original monthlysource.MonthlyObservationFile, create bool,
	write func(*os.Root, string, []byte) error) (receipt walletReuseReceipt, err error) {
	if parent == nil || write == nil {
		return receipt, errInput
	}
	ctx, cancel := context.WithTimeout(parent, 35*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return receipt, err
	}
	rootPath, output, err := jobPath(workspace, name)
	if err != nil || rootPath != workspace {
		return receipt, errInput
	}
	if _, err := os.Lstat(output); !os.IsNotExist(err) {
		return receipt, errInput
	}
	m, meta, err := walletReuseInputs(ctx, original)
	if err != nil {
		return receipt, err
	}
	metaRaw, err := json.Marshal(meta)
	if err != nil {
		return receipt, err
	}
	m.WalletIDs = []string{}
	m.WalletReuse = &monthlysource.MonthlyObservationFile{Path: filepath.Join(output, "wallet-reuse.json"), SHA256: hash(metaRaw)}
	materialRaw, err := json.Marshal(m)
	if err != nil {
		return receipt, err
	}
	receipt = walletReuseReceipt{Mode: "checked_wallet_reuse_no_output", Accounts: len(meta.Accounts), ManifestSHA256: hash(metaRaw), MaterialSHA256: hash(materialRaw)}
	if !create {
		return receipt, ctx.Err()
	}
	lock, err := workspaceLock(workspace, true, true)
	if err != nil {
		return receipt, err
	}
	defer lock.Close()
	root, err := os.OpenRoot(workspace)
	if err != nil {
		return receipt, err
	}
	defer root.Close()
	if err := observationRootCurrent(root, workspace); err != nil {
		return receipt, err
	}
	if err := root.Mkdir(name, 0700); err != nil {
		return receipt, err
	}
	receipt.Directory, receipt.Mode = output, "incomplete_wallet_reuse_do_not_use"
	dir, err := root.OpenRoot(name)
	if err != nil {
		return receipt, err
	}
	defer dir.Close()
	parts := []struct {
		name string
		raw  []byte
	}{{"wallet-reuse.pending.json", metaRaw}, {"materials.pending.json", materialRaw}}
	for _, p := range parts {
		if err := write(dir, p.name, p.raw); err != nil {
			return receipt, err
		}
	}
	_, again, err := walletReuseInputs(ctx, original)
	if err != nil {
		return receipt, err
	}
	againRaw, err := json.Marshal(again)
	if err != nil || !bytes.Equal(metaRaw, againRaw) {
		return receipt, errInput
	}
	for _, p := range parts {
		if err := verifyObservationFile(dir, p.name, p.raw); err != nil {
			return receipt, err
		}
	}
	if err := observationRootCurrent(root, workspace); err != nil {
		return receipt, err
	}
	if err := observationRootCurrent(dir, output); err != nil {
		return receipt, err
	}
	if err := ctx.Err(); err != nil {
		return receipt, err
	}
	if err := dir.Link("wallet-reuse.pending.json", "wallet-reuse.json"); err != nil {
		return receipt, err
	}
	if err := dir.Remove("wallet-reuse.pending.json"); err != nil {
		return receipt, err
	}
	if err := dir.Link("materials.pending.json", "materials.json"); err != nil {
		return receipt, err
	}
	receipt.Complete, receipt.Mode = true, "assembled_wallet_reuse_metadata_not_bill_approval"
	err = errors.Join(dir.Remove("materials.pending.json"), syncObservationRoot(dir), syncObservationRoot(root), ctx.Err())
	return receipt, err
}
