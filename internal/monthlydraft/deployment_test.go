package monthlydraft

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func deploymentFixture(t *testing.T) (generationFixture, DeploymentConfig) {
	t.Helper()
	f := selectionFixture(t)
	_, ro, path := usageAuthorityFixture(t)
	owner, _, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	stageFixture(t, f, "")
	if err := os.Chmod(f.receiverPath, 0600); err != nil {
		t.Fatal(err)
	}
	return f, DeploymentConfig{Rollout: GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: "2026-09"}}}, ReceiverPath: f.receiverPath, FundingPath: f.path, AuthorityPath: path, EvidenceRoot: f.root}
}
func closeDeploymentTest(t *testing.T, d *Deployment) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := d.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDeploymentOpenReadOnlySourcesLockAndLifecycle(t *testing.T) {
	f, cfg := deploymentFixture(t)
	ctx := context.Background()
	authBefore, err := os.ReadFile(cfg.AuthorityPath)
	if err != nil {
		t.Fatal(err)
	}
	d, err := OpenDeployment(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, d) })
	if d.runtime.Phase() != "disabled" {
		t.Fatal("open started worker")
	}
	if _, err := OpenDeployment(ctx, cfg); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("second process lock not rejected", err)
	}
	for _, db := range []*sql.DB{d.funding, d.authority} {
		if _, err := db.Exec("CREATE TABLE must_not_be_created(id INTEGER)"); err == nil {
			t.Fatal("source writable")
		}
	}
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	v, err := d.SubmitMonthly(ctx, 1, 1, "2026-09")
	if err != nil || v.Month != "2026-09" {
		t.Fatal(v, err)
	}
	job, err := d.g.tasks.Find(ctx, generationRequest(f.plan, mustGenerationPlan(t, f)))
	if err != nil {
		t.Fatal(err)
	}
	waitLocalDraft(t, d.g, job)
	closeDeploymentTest(t, d)
	if err := d.Start(ctx); err == nil {
		t.Fatal("closed deployment restarted")
	}
	if _, err := d.ReadMonthly(ctx, 1, 1, "2026-09"); err == nil {
		t.Fatal("closed deployment accessed databases")
	}
	for _, db := range []*sql.DB{d.receiver, d.funding, d.authority} {
		if err := db.Ping(); err == nil {
			t.Fatal("handle left open")
		}
	}
	again, err := OpenDeployment(ctx, cfg)
	if err != nil {
		t.Fatal("lock not released", err)
	}
	closeDeploymentTest(t, again)
	authAfter, err := os.ReadFile(cfg.AuthorityPath)
	if err != nil || generationHash(authBefore) != generationHash(authAfter) {
		t.Fatal("authority file changed", err)
	}
	body, err := os.ReadFile(f.path)
	if err != nil || generationHash(body) != f.sourceHash {
		t.Fatal("funding file changed", err)
	}
}

func TestDeploymentInvalidInputsDoNotCreateOrMigrate(t *testing.T) {
	f, cfg := deploymentFixture(t)
	ctx := context.Background()
	if d, err := OpenDeployment(ctx, DeploymentConfig{ReceiverPath: "/not/read"}); err != nil || d != nil {
		t.Fatal("disabled open touched files", err)
	}
	for _, change := range []func(*DeploymentConfig){
		func(c *DeploymentConfig) { c.ReceiverPath = filepath.Join(f.root, "missing.sqlite") },
		func(c *DeploymentConfig) { c.ReceiverPath = "relative.sqlite" },
		func(c *DeploymentConfig) { c.FundingPath = c.ReceiverPath },
		func(c *DeploymentConfig) { c.AuthorityPath = c.FundingPath },
		func(c *DeploymentConfig) { c.EvidenceRoot = "/" },
	} {
		c := cfg
		change(&c)
		if d, err := OpenDeployment(ctx, c); err == nil || d != nil {
			t.Fatal("invalid deployment accepted", c)
		}
	}
	if _, err := os.Stat(filepath.Join(f.root, "missing.sqlite")); !os.IsNotExist(err) {
		t.Fatal("missing database created")
	}
	link := filepath.Join(f.root, "source-link.sqlite")
	if err := os.Symlink(cfg.FundingPath, link); err != nil {
		t.Fatal(err)
	}
	c := cfg
	c.FundingPath = link
	if _, err := OpenDeployment(ctx, c); err == nil {
		t.Fatal("symlink source accepted")
	}
	if err := os.Chmod(f.receiverPath, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDeployment(ctx, cfg); err == nil {
		t.Fatal("public receiver accepted")
	}
	if err := os.Chmod(f.receiverPath, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.receiver.Exec(`UPDATE portal_monthly_draft_task_control SET version=99 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDeployment(ctx, cfg); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("unknown version accepted", err)
	}
	var version int
	if err := f.receiver.QueryRow(`SELECT version FROM portal_monthly_draft_task_control WHERE id=1`).Scan(&version); err != nil || version != 99 {
		t.Fatal("startup migrated database", err)
	}
	if _, err := f.receiver.Exec(`UPDATE portal_monthly_draft_task_control SET version=1 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	d, err := OpenDeployment(ctx, cfg)
	if err != nil {
		t.Fatal("failed startup leaked lock", err)
	}
	closeDeploymentTest(t, d)
}

func TestDeploymentFileAliasesCannotBypassLock(t *testing.T) {
	_, cfg := deploymentFixture(t)
	ctx := context.Background()
	d, err := OpenDeployment(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, d) })
	alias := filepath.Join(t.TempDir(), "parent-alias")
	if err := os.Symlink(filepath.Dir(cfg.ReceiverPath), alias); err != nil {
		t.Fatal(err)
	}
	c := cfg
	c.ReceiverPath = filepath.Join(alias, filepath.Base(cfg.ReceiverPath))
	if _, err := OpenDeployment(ctx, c); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("directory alias bypassed lock", err)
	}
	closeDeploymentTest(t, d)
	hardlink := filepath.Join(t.TempDir(), "receiver-hardlink.sqlite")
	if err := os.Link(cfg.ReceiverPath, hardlink); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{cfg.ReceiverPath, hardlink} {
		c.ReceiverPath = path
		if _, err := OpenDeployment(ctx, c); !errors.Is(err, ErrGenerationInput) {
			t.Fatal("receiver with multiple hard links accepted", err)
		}
	}
}

func TestDeploymentRejectsAliasedLockFile(t *testing.T) {
	for _, kind := range []string{"symlink", "hardlink"} {
		t.Run(kind, func(t *testing.T) {
			_, cfg := deploymentFixture(t)
			canonical, err := filepath.EvalSymlinks(cfg.ReceiverPath)
			if err != nil {
				t.Fatal(err)
			}
			link := os.Link
			if kind == "symlink" {
				link = os.Symlink
			}
			before, err := os.ReadFile(cfg.FundingPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := link(cfg.FundingPath, canonical+".runtime.lock"); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenDeployment(context.Background(), cfg); !errors.Is(err, ErrGenerationInput) {
				t.Fatal("aliased lock accepted", err)
			}
			after, err := os.ReadFile(cfg.FundingPath)
			if err != nil || generationHash(before) != generationHash(after) {
				t.Fatal("source modified by rejected lock", err)
			}
		})
	}
}

func TestDeploymentCloseTimeoutRetainsHandlesAndLock(t *testing.T) {
	_, cfg := deploymentFixture(t)
	d, err := OpenDeployment(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	b := &runtimeBlockedService{entered: make(chan struct{}), release: make(chan struct{})}
	d.runtime.service = b
	var once sync.Once
	release := func() { once.Do(func() { close(b.release) }) }
	t.Cleanup(func() { release(); closeDeploymentTest(t, d) })
	if err := d.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := d.ReadMonthly(context.Background(), 1, 1, "2026-09"); done <- err }()
	select {
	case <-b.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not enter")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := d.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("close falsely drained", err)
	}
	if err := d.receiver.Ping(); err != nil {
		t.Fatal("active handle closed", err)
	}
	if _, err := OpenDeployment(context.Background(), cfg); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("active lock released", err)
	}
	release()
	if err := <-done; err == nil {
		t.Fatal("cancelled request succeeded")
	}
	closeDeploymentTest(t, d)
}
