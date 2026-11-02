package monthlydraft

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

func automaticDeploymentFixture(t *testing.T) (generationFixture, DeploymentConfig, *gorm.DB) {
	t.Helper()
	f := observedPreparationFixture(t, "")
	auth, ro, path := usageAuthorityFixture(t)
	if err := auth.AutoMigrate(&monitor.PortalMonthlyBill{}); err != nil {
		t.Fatal(err)
	}
	owner, _, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	out, err := f.g.PrepareObservedInput(context.Background(), f.plan, f.raw, "observed_wallet")
	if err != nil {
		t.Fatal(err)
	}
	useWalletPreparation(t, &f, WalletPreparation{Plan: out.Plan, Preparation: out.Preparation})
	stageFixture(t, f, "")
	if err := os.Chmod(f.receiverPath, 0600); err != nil {
		t.Fatal(err)
	}
	return f, DeploymentConfig{Rollout: GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: "2026-09"}}},
		ReceiverPath: f.receiverPath, FundingPath: f.path, AuthorityPath: path, EvidenceRoot: f.root}, auth
}

func automaticDeploymentPublisher(db *gorm.DB) AutomaticPublisher {
	return func(ctx context.Context, req monthlytasks.Request, raw []byte) error {
		_, err := monitor.ImportMonthlyBillForAuthorization(ctx, db, raw, monitor.MonthlyBillExpectedAuthorization{
			GroupID: req.Owner.GroupID, AuthVersion: req.Owner.AuthVersion, MemberFingerprint: req.Owner.MemberFingerprint, MemberManifest: req.Owner.MemberManifest}, time.Now())
		return err
	}
}

func TestAutomaticDeploymentMachineInputPublishRestartAndDraftFallback(t *testing.T) {
	f, cfg, auth := automaticDeploymentFixture(t)
	ctx := context.Background()
	scope := automaticScopeFixture(t, f)
	var loads, publishes atomic.Int64
	load := func(ctx context.Context, _ monthlytasks.Request) (AutomaticScope, error) {
		loads.Add(1)
		return scope, ctx.Err()
	}
	write := automaticDeploymentPublisher(auth)
	publish := func(ctx context.Context, req monthlytasks.Request, raw []byte) error {
		publishes.Add(1)
		return write(ctx, req, raw)
	}
	d, err := OpenAutomaticDeployment(ctx, cfg, load, publish)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, d) })
	if d.runtime.Phase() != "disabled" || loads.Load() != 0 || publishes.Load() != 0 {
		t.Fatal("opener started tasks or read private source metadata")
	}
	for _, open := range []func() (*Deployment, error){
		func() (*Deployment, error) { return OpenDeployment(ctx, cfg) },
		func() (*Deployment, error) { return OpenAutomaticDeployment(ctx, cfg, load, publish) },
	} {
		if other, err := open(); other != nil || !errors.Is(err, ErrGenerationBusy) {
			t.Fatal("automatic and draft processes bypassed common lock", err)
		}
	}
	for _, db := range []*gorm.DB{auth} {
		var n int64
		if err := db.Model(&monitor.PortalMonthlyBill{}).Count(&n).Error; err != nil || n != 0 {
			t.Fatal("constructor saved a bill", err)
		}
	}
	// Keep synthetic tests below the production five-second poll interval.
	d.runtime.worker.pollMin, d.runtime.worker.pollMax = time.Millisecond, 10*time.Millisecond
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SubmitMonthly(ctx, 2, 1, "2026-09"); err == nil {
		t.Fatal("foreign organization admitted")
	}
	if _, err := d.SubmitMonthly(ctx, 1, 1, "2026-09"); err != nil {
		t.Fatal(err)
	}
	if !waitMonthlyAcceptance(t, func() bool { return d.runtime.worker.Status().Published == 1 }) {
		t.Fatal("explicit deployment did not finish", d.runtime.worker.Status())
	}
	closeDeploymentTest(t, d)
	if d.runtime.worker.Status().Published != 1 || loads.Load() != 1 || publishes.Load() != 1 {
		t.Fatal("explicit deployment did not save exactly once", d.runtime.worker.Status())
	}
	var before monitor.PortalMonthlyBill
	if err := auth.First(&before, "group_id=1 AND month='2026-09'").Error; err != nil {
		t.Fatal(err)
	}
	// Reopening must use the same saved task/publication record, not recalculate
	// or issue a second bill. No main config or rollout change is implied.
	reopened, err := OpenAutomaticDeployment(ctx, cfg, load, publish)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, reopened) })
	if worked, err := reopened.runtime.worker.step(ctx); err != nil || worked || loads.Load() != 1 || publishes.Load() != 1 {
		t.Fatal("reopen reissued a saved bill", worked, err)
	}
	closeDeploymentTest(t, reopened)
	// Removing the automatic adapter is a draft-only fallback, not a reverse
	// migration or a request to erase customer statements.
	fallback, err := OpenDeployment(ctx, cfg)
	if err != nil || fallback.runtime.worker.finalize != nil {
		t.Fatal("draft-only fallback not preserved", err)
	}
	closeDeploymentTest(t, fallback)
	var after monitor.PortalMonthlyBill
	if err := auth.First(&after, "group_id=1 AND month='2026-09'").Error; err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("restart or draft fallback changed frozen bill", err)
	}
	if raw, err := os.ReadFile(f.path); err != nil || generationHash(raw) != f.sourceHash {
		t.Fatal("automatic deployment changed source observations", err)
	}
}

func TestAutomaticDeploymentExplicitDependenciesNoStartupMigration(t *testing.T) {
	ctx := context.Background()
	load := func(context.Context, monthlytasks.Request) (AutomaticScope, error) {
		t.Fatal("startup called source loader")
		return AutomaticScope{}, nil
	}
	publish := func(context.Context, monthlytasks.Request, []byte) error {
		t.Fatal("startup published")
		return nil
	}
	if d, err := OpenAutomaticDeployment(ctx, DeploymentConfig{ReceiverPath: "/not/opened"}, nil, nil); err != nil || d != nil {
		t.Fatal("disabled automatic opener touched files", err)
	}
	f, cfg, _ := automaticDeploymentFixture(t)
	for _, deps := range []automaticDeploymentDependencies{{}, {load: load}, {publish: publish}} {
		if d, err := OpenAutomaticDeployment(ctx, cfg, deps.load, deps.publish); err == nil || d != nil {
			t.Fatal("missing trusted dependency accepted")
		}
	}
	for _, query := range []string{"DROP TRIGGER monthly_publication_no_delete", "DROP TRIGGER monthly_publication_identity", "DROP TABLE portal_monthly_publication_jobs", "DROP TABLE portal_monthly_publication_control"} {
		if _, err := f.receiver.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	before, err := os.ReadFile(f.receiverPath)
	if err != nil {
		t.Fatal(err)
	}
	if d, err := OpenAutomaticDeployment(ctx, cfg, load, publish); err == nil || d != nil {
		t.Fatal("automatic opener silently initialized missing journal")
	}
	after, err := os.ReadFile(f.receiverPath)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("failed startup modified receiver", err)
	}
	fallback, err := OpenDeployment(ctx, cfg)
	if err != nil {
		t.Fatal("failed automatic startup leaked lock or broke draft-only fallback", err)
	}
	closeDeploymentTest(t, fallback)
}

func TestAutomaticDeploymentDrainTimeoutKeepsLockAndHandles(t *testing.T) {
	_, cfg, auth := automaticDeploymentFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(release) }) }
	load := func(context.Context, monthlytasks.Request) (AutomaticScope, error) {
		close(entered)
		<-release // deliberately uncooperative synthetic producer; test safe drain
		return AutomaticScope{}, context.Canceled
	}
	d, err := OpenAutomaticDeployment(context.Background(), cfg, load, automaticDeploymentPublisher(auth))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resume(); closeDeploymentTest(t, d) })
	d.runtime.worker.pollMin, d.runtime.worker.pollMax = time.Millisecond, 10*time.Millisecond
	if err := d.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := d.SubmitMonthly(context.Background(), 1, 1, "2026-09"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("automatic finalization never entered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = d.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) || d.closed {
		t.Fatal("close claimed success while finalizer still active", err)
	}
	if err := d.receiver.Ping(); err != nil {
		t.Fatal("close timeout closed active database", err)
	}
	if _, err := OpenDeployment(context.Background(), cfg); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("close timeout released process lock", err)
	}
	if _, err := d.SubmitMonthly(context.Background(), 1, 1, "2026-09"); err == nil {
		t.Fatal("stopping deployment admitted a task")
	}
	resume()
	closeDeploymentTest(t, d)
	var n int64
	if err := auth.Model(&monitor.PortalMonthlyBill{}).Count(&n).Error; err != nil || n != 0 {
		t.Fatal("canceled metadata published partial bill", err)
	}
}
