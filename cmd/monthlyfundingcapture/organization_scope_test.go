package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlydraft"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func currentOrganizationScopeFixture(t *testing.T) (string, string, string) {
	t.Helper()
	root, err := filepath.EvalSymlinks(auditPrivateDir(t))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "authority.sqlite")
	db, err := gorm.Open(sqlite.Open("file:"+path+"?mode=rwc"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&monitor.CustomerGroup{}, &monitor.TrackedUser{}, &monitor.UsageMemberControl{}, &monitor.UsageMemberAudit{}, &monitor.UsageMemberControlMigration{}); err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{&monitor.UsageMemberControlMigration{ID: 1, Version: 1, ManifestHash: strings.Repeat("b", 64), InitializedAt: 1},
		&monitor.CustomerGroup{ID: 6781, Name: "synthetic collection authority", PortalEmail: "collection@example.invalid", PortalAuthVer: 2},
		&monitor.TrackedUser{UserID: 80002, GroupID: 6781}, &monitor.TrackedUser{UserID: 80001, GroupID: 6781}} {
		if err := db.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "scope")
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	receipt, err := monthlydraft.PrepareOrganizationCollection(context.Background(), monthlydraft.OrganizationCollectionRequest{
		AuthorityPath: path, OutputRoot: output, GroupID: 6781, Month: "2026-09", SourceIdentity: strings.Repeat("a", 64), Write: true})
	if err != nil || !receipt.Complete {
		t.Fatal("actual authoritative roster producer failed", err)
	}
	return root, filepath.Join(output, "organization.json"), receipt.ScopeSHA256
}

func TestOrganizationScopeProducerFeedsBothCollectionPlansWithoutManualUsers(t *testing.T) {
	root, path, digest := currentOrganizationScopeFixture(t)
	ctx := context.Background()
	for _, command := range []string{"init-month", "init-observation-month"} {
		args := []string{command, "-workspace", root, "-batch", command, "-organization-scope", path, "-expected-organization", digest}
		if command == "init-month" {
			args = append(args, "-window-seconds=86400")
		}
		var out bytes.Buffer
		if err := run(ctx, args, &out); err != nil {
			t.Fatal("current roster preflight rejected", command, err)
		}
		if _, err := os.Stat(filepath.Join(root, command)); !os.IsNotExist(err) {
			t.Fatal("default roster init wrote output", err)
		}
		out.Reset()
		if err := run(ctx, append(args, "-create"), &out); err != nil {
			t.Fatal("current roster not usable by capture initialization", command, err)
		}
		var funding monthlysource.FundingMonthRequest
		if command == "init-month" {
			body, err := os.ReadFile(filepath.Join(root, command, "batch.json"))
			if err != nil {
				t.Fatal(err)
			}
			var batch monthCaptureBatch
			if json.Unmarshal(body, &batch) != nil || batch.GroupID != 6781 || len(batch.Windows) != 30 {
				t.Fatal("funding batch scope differs")
			}
			funding = batch.Funding
		} else {
			body, err := os.ReadFile(filepath.Join(root, command, "observation-month.json"))
			if err != nil {
				t.Fatal(err)
			}
			var batch observationMonthPlan
			if json.Unmarshal(body, &batch) != nil || batch.GroupID != 6781 {
				t.Fatal("observation batch scope differs")
			}
			funding = batch.Funding
			var receipt observationMonthProgress
			if json.Unmarshal(out.Bytes(), &receipt) != nil || receipt.Captures != 122 || receipt.Captured {
				t.Fatal("roster initialization contacted source or omitted account windows")
			}
		}
		if funding.Month != "2026-09" || !reflect.DeepEqual(funding.UserIDs, []int64{80001, 80002}) {
			t.Fatal("funding and token/wallet scopes diverged", command)
		}
		if fileHash(t, path) != digest {
			t.Fatal("roster source mutated")
		}
		if err := run(ctx, append(args, "-create"), &out); err == nil {
			t.Fatal("existing collection plan replaced")
		}
		if binary := os.Getenv("MONTHLY_CAPTURE_TEST_BINARY"); binary != "" {
			current := append([]string{}, args...)
			for i := range current {
				if current[i] == "-batch" {
					current[i+1] = "cli-" + command
				}
			}
			current = append(current, "-create")
			if raw, err := exec.CommandContext(t.Context(), binary, current...).CombinedOutput(); err != nil {
				t.Fatal("actual scope-consuming CLI failed", err, string(raw))
			}
		}
	}
}

func TestOrganizationScopeRejectsMixedManualOverridesAndWrongDigest(t *testing.T) {
	root, path, digest := currentOrganizationScopeFixture(t)
	for _, command := range []string{"init-month", "init-observation-month"} {
		base := []string{command, "-workspace", root, "-batch", "invalid-" + command, "-organization-scope", path, "-expected-organization", digest, "-create"}
		for _, extra := range [][]string{{"-group-id=6781"}, {"-month=2026-09"}, {"-source-identity", strings.Repeat("a", 64)}, {"-user-ids=80001"}, {"-expected-organization", strings.Repeat("b", 64)}, {"-organization-scope="}, {"-expected-organization="}} {
			var out bytes.Buffer
			if err := run(context.Background(), append(append([]string{}, base...), extra...), &out); err == nil || out.Len() != 0 {
				t.Fatal("manual override or wrong roster digest accepted", command, extra, err)
			}
		}
		if _, err := os.Stat(filepath.Join(root, "invalid-"+command)); !os.IsNotExist(err) {
			t.Fatal("invalid roster created a capture plan", err)
		}
	}
}
