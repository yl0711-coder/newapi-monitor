package monthlydraft

// Explicit local-only composition for retained, genuinely captured customer
// data. This file is never part of a product binary. It uses the product's
// actual selection, worker, publication, authentication and download handlers;
// no synthetic balances or pre-published customer bills are installed.
import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type retainedAcceptanceConfig struct {
	Owner          monthlytasks.Owner `json:"owner"`
	Month          string             `json:"month"`
	SourceIdentity string             `json:"source_identity"`
	ScopeSourceID  string             `json:"scope_source_id"`
}

func acceptancePrivateRead(t *testing.T, path string, limit int64) []byte {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > limit {
		t.Fatal("private retained acceptance file unavailable")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("private retained acceptance read failed")
	}
	return raw
}

func acceptanceWriteNew(t *testing.T, path string, body []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal("new private acceptance output required", err)
	}
	_, writeErr := f.Write(body)
	syncErr, closeErr := f.Sync(), f.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		t.Fatal("private acceptance output incomplete")
	}
}

func acceptanceAuth(t *testing.T, path string) (*gorm.DB, *sql.DB) {
	t.Helper()
	db := generationDB(t, path, false)
	auth, err := gorm.Open(&sqlite.Dialector{Conn: db}, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	return auth, db
}

func TestRetainedRealMonthlyAcceptance(t *testing.T) {
	root, mode := os.Getenv("MONTHLY_REAL_ACCEPTANCE_ROOT"), os.Getenv("MONTHLY_REAL_ACCEPTANCE_MODE")
	if root == "" {
		t.Skip("explicit retained real-data acceptance workspace required")
	}
	actual, err := filepath.EvalSymlinks(root)
	st, statErr := os.Lstat(root)
	if err != nil || statErr != nil || actual != root || !filepath.IsAbs(root) || !st.IsDir() || st.Mode().Perm() != 0700 {
		t.Fatal("private canonical local workspace required")
	}
	switch mode {
	case "prepare":
		prepareRetainedRealAcceptance(t, root)
	case "serve":
		serveRetainedRealAcceptance(t, root)
	default:
		t.Fatal("explicit prepare or serve mode required")
	}
}

func prepareRetainedRealAcceptance(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatal("fresh empty local acceptance workspace required")
	}
	input, bundleContext := retainedMonthlySnapshotMoneyReconstruction(t)
	if len(input.Accounts) != 1 || len(bundleContext.Accounts) != 1 || len(bundleContext.Groups) != 1 {
		t.Fatal("one explicitly reviewed organization/account required")
	}
	a := &input.Accounts[0]
	group := &bundleContext.Groups[0]
	var meta struct {
		Groups []struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		} `json:"groups"`
		Users []monitor.TrackedUser `json:"users"`
	}
	metaRaw := acceptancePrivateRead(t, os.Getenv("MONTHLY_REAL_DIRECTORY"), 262144)
	if json.Unmarshal(metaRaw, &meta) != nil || len(meta.Groups) != 1 || len(meta.Users) != 1 || meta.Groups[0].ID != a.GroupID || meta.Users[0].GroupID != a.GroupID || meta.Users[0].UserID != a.UserID || meta.Users[0].Username != a.Username {
		t.Fatal("selected customer differs from retained financial and current directory sources")
	}
	if len(bundleContext.Accounts[0].Reconstructions) != 1 {
		t.Fatal("independent reconstruction required")
	}
	proof := bundleContext.Accounts[0].Reconstructions[0]
	start, err := time.ParseInLocation("2006-01", a.Months[0], time.FixedZone("UTC+8", 8*3600))
	if err != nil {
		t.Fatal(err)
	}
	end := start.AddDate(0, 1, 0).Unix()
	// Preserve all originals, but select only this closed month's spans. The
	// independently verified bridge remains in the pinned reconstruction, not
	// in an unrelated active-collection snapshot relabeled as a midnight cut.
	spans := []monitor.MonthlyPreparationSpan{}
	for _, span := range a.Spans {
		if span.From >= start.Unix() && span.To <= end {
			spans = append(spans, span)
		}
	}
	a.Spans, a.Snapshots = spans, []monitor.MonthlyPreparationBalance{}
	movements := []monitor.MonthlyPreparationMovement{}
	for _, movement := range a.Movements {
		if movement.At < proof.ObservedAt {
			movements = append(movements, movement)
		}
	}
	a.Movements = movements
	a.MovementCoverage = []monitor.MonthlyPreparationCoverage{{UserID: a.UserID, From: a.Baseline.At, To: proof.ObservedAt, Source: proof.FundingSource}}
	group.Name = meta.Groups[0].Name
	group.Source = "original_preparation#selected-account-group-and-current-customer-scope"
	evidenceRoot := filepath.Join(root, "evidence")
	if os.Mkdir(evidenceRoot, 0700) != nil {
		t.Fatal("new evidence directory required")
	}
	// Select only referenced originals; do not copy other organizations' large
	// unrelated SQLite files or mutate any retained source.
	references := map[string]bool{"original_preparation": true, "current_directory": true}
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case string:
			id, _, ok := strings.Cut(x, "#")
			if ok {
				references[id] = true
			}
		case []any:
			for _, item := range x {
				walk(item)
			}
		case map[string]any:
			for _, item := range x {
				walk(item)
			}
		}
	}
	selected, _ := json.Marshal(struct {
		Account monitor.MonthlyPreparationAccount
		Context monitor.MonthlyBundleContext
	}{*a, bundleContext})
	var selection any
	if json.Unmarshal(selected, &selection) != nil {
		t.Fatal("source selection encoding")
	}
	walk(selection)
	originalSources := append([]monitor.MonthlyPreparationSource{}, input.Sources...)
	originalRaw := acceptancePrivateRead(t, os.Getenv("MONTHLY_MONEY_INPUT"), monitor.MonthlyPreparationInputLimit)
	originalSources = append(originalSources, monitor.MonthlyPreparationSource{ID: "original_preparation", Path: os.Getenv("MONTHLY_MONEY_INPUT"), SHA256: generationHash(originalRaw)}, monitor.MonthlyPreparationSource{ID: "current_directory", Path: os.Getenv("MONTHLY_REAL_DIRECTORY"), SHA256: generationHash(metaRaw)})
	input.Sources = []monitor.MonthlyPreparationSource{}
	for _, source := range originalSources {
		if !references[source.ID] {
			continue
		}
		if !validScopeSourceID(source.ID) {
			t.Fatal("unsafe retained source id")
		}
		// Some old non-secret query-definition originals are 0644. Preserve
		// their bytes/mode rather than chmod an original or weaken the private
		// generated-file reader. Only a pinned regular original is copied.
		st, err := os.Lstat(source.Path)
		if err != nil || !st.Mode().IsRegular() || st.Size() > 32<<20 {
			t.Fatal("pinned original unavailable")
		}
		raw, err := os.ReadFile(source.Path)
		if err != nil {
			t.Fatal("pinned original read failed")
		}
		if generationHash(raw) != source.SHA256 {
			t.Fatal("retained original changed")
		}
		source.Path = filepath.Join(evidenceRoot, source.ID+".source")
		acceptanceWriteNew(t, source.Path, raw)
		input.Sources = append(input.Sources, source)
	}
	// The actual retained funding store and full original-token archive are
	// separately audited by the product calculator on every generation/reuse.
	fundingRaw := acceptancePrivateRead(t, os.Getenv("MONTHLY_REAL_FUNDING_DB"), 32<<20)
	fundingPath := filepath.Join(evidenceRoot, "funding.sqlite")
	acceptanceWriteNew(t, fundingPath, fundingRaw)
	input.Sources = append(input.Sources, monitor.MonthlyPreparationSource{ID: "fresh_funding", Path: fundingPath, SHA256: generationHash(fundingRaw)})
	tokenRaw := acceptancePrivateRead(t, os.Getenv("MONTHLY_REAL_TOKEN_ARCHIVE"), monthlysource.TokenUsageArchiveLimit)
	archive, report, err := monthlysource.DecodeTokenUsageMonthArchive(context.Background(), tokenRaw, time.Now())
	if err != nil || archive.UserID != a.UserID || archive.Month != a.Months[0] || report.ConsumeQuota != *proof.MonthConsumeQuota || report.RefundQuota != *proof.MonthRefundQuota {
		t.Fatal("fresh original token source differs from independently checked user money", err)
	}
	tokenPath := filepath.Join(evidenceRoot, "tokens.json")
	acceptanceWriteNew(t, tokenPath, tokenRaw)
	input.Sources = append(input.Sources, monitor.MonthlyPreparationSource{ID: "fresh_tokens", Path: tokenPath, SHA256: generationHash(tokenRaw)})
	var fundingPlan struct {
		Version int                               `json:"version"`
		GroupID int64                             `json:"group_id"`
		Funding monthlysource.FundingMonthRequest `json:"funding"`
		Windows []struct {
			Version monthlysource.FundingMonthWindowVersion `json:"version"`
		} `json:"windows"`
	}
	if json.Unmarshal(acceptancePrivateRead(t, os.Getenv("MONTHLY_REAL_FUNDING_PLAN"), 2<<20), &fundingPlan) != nil || fundingPlan.Version != 2 || fundingPlan.GroupID != a.GroupID || fundingPlan.Funding.Month != a.Months[0] || len(fundingPlan.Funding.UserIDs) != 1 || fundingPlan.Funding.UserIDs[0] != a.UserID || fundingPlan.Funding.SourceIdentity != archive.SourceIdentity {
		t.Fatal("funding source scope differs")
	}
	storePath := filepath.Join(root, "usage.db")
	m, err := monitor.New(monitor.Settings{StorePath: storePath, LocalSnapshotOnly: true, AlertsDisabled: true, PortalMonthlyStatementEnabled: true, SessionSecret: retainedAcceptanceSecret(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	auth, _ := acceptanceAuth(t, storePath)
	pw, err := bcrypt.GenerateFromPassword([]byte("LocalUsage2026!"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatal(err)
	}
	customer := monitor.CustomerGroup{ID: a.GroupID, Name: group.Name, PortalEmail: "usage-local@example.invalid", PortalPwAdmin: string(pw), PortalAuthVer: 1}
	if auth.Create(&customer).Error != nil || auth.Create(&meta.Users).Error != nil {
		t.Fatal("local customer metadata creation failed")
	}
	owner, _, err := LoadUsageGenerationOwner(context.Background(), auth, a.GroupID, 1)
	if err != nil {
		t.Fatal(err)
	}
	receiver := generationDB(t, filepath.Join(root, "tasks.sqlite"), false)
	if InitGenerationReceiver(context.Background(), receiver) != nil || InitGenerationSelections(context.Background(), receiver) != nil {
		t.Fatal("fresh local task schema")
	}
	funding := generationDB(t, fundingPath, true)
	selector, err := GenerationSelector(receiver)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := NewUsageGenerationGuard(auth, selector)
	if err != nil {
		t.Fatal(err)
	}
	g, err := NewGeneration(receiver, funding, evidenceRoot, guard)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	raw, _ := json.Marshal(input)
	plan := GenerationPlan{Version: 2, Owner: owner, Funding: fundingPlan.Funding, PreparationSHA256: generationHash(raw), TokenUsageSources: []TokenUsageSourceSelection{{UserID: a.UserID, SourceID: "fresh_tokens"}}}
	for _, window := range fundingPlan.Windows {
		plan.Windows = append(plan.Windows, window.Version)
	}
	normalized, err := g.PrepareTokenUsageInput(context.Background(), plan, raw)
	if err != nil {
		t.Fatal("retained token normalization", err)
	}
	if json.Unmarshal(normalized.Plan, &plan) != nil || json.Unmarshal(normalized.Preparation, &input) != nil {
		t.Fatal("normalized local input")
	}
	scope := AutomaticScope{Group: *group, Accounts: []AutomaticWalletScope{{UserID: a.UserID, WalletOnly: bundleContext.Accounts[0].WalletOnly, WalletSource: bundleContext.Accounts[0].WalletSource, Reconstructions: bundleContext.Accounts[0].Reconstructions}}}
	scopeID := "independent_retained_scope"
	doc := RetainedAutomaticScope{Version: 1, Owner: owner, Month: plan.Funding.Month, SourceIdentity: plan.Funding.SourceIdentity, Scope: scope}
	scopeRaw, _ := json.Marshal(doc)
	scopePath := filepath.Join(evidenceRoot, "scope.json")
	acceptanceWriteNew(t, scopePath, scopeRaw)
	input.Sources = append(input.Sources, monitor.MonthlyPreparationSource{ID: scopeID, Path: scopePath, SHA256: generationHash(scopeRaw)})
	raw, _ = json.Marshal(input)
	plan.PreparationSHA256 = generationHash(raw)
	plan.AutomaticScopeSourceID = scopeID
	plan, planRaw, _, err := canonicalGeneration(plan, raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := g.verifyFiles(context.Background(), input.Sources); err != nil {
		t.Fatal(err)
	}
	windows, err := g.auditWindows(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	// Evaluate the actual independent source scope before admitting customer
	// requests. This uses no review flags or altered consumption figures.
	if _, err := observedAutomaticContext(context.Background(), plan, raw, windows, scope); err != nil {
		t.Fatal("real source scope/money not eligible", err)
	}
	if _, err := StageGenerationVersion(context.Background(), receiver, plan, raw, ""); err != nil {
		t.Fatal(err)
	}
	acceptanceWriteNew(t, filepath.Join(root, "plan.json"), planRaw)
	acceptanceWriteNew(t, filepath.Join(root, "input.json"), raw)
	config, _ := json.Marshal(retainedAcceptanceConfig{Owner: owner, Month: plan.Funding.Month, SourceIdentity: plan.Funding.SourceIdentity, ScopeSourceID: scopeID})
	acceptanceWriteNew(t, filepath.Join(root, "config.json"), config)
	acceptanceWriteNew(t, filepath.Join(root, "session-secret"), []byte(retainedAcceptanceSecret(t)))
	t.Log("Local real-data preparation completed; selected inputs only, no customer task or bill pre-created")
}

func retainedAcceptanceSecret(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func serveRetainedRealAcceptance(t *testing.T, root string) {
	t.Helper()
	var cfg retainedAcceptanceConfig
	if json.Unmarshal(acceptancePrivateRead(t, filepath.Join(root, "config.json"), 4096), &cfg) != nil {
		t.Fatal("prepared config")
	}
	storePath := filepath.Join(root, "usage.db")
	secret := string(acceptancePrivateRead(t, filepath.Join(root, "session-secret"), 128))
	m, err := monitor.New(monitor.Settings{StorePath: storePath, LocalSnapshotOnly: true, AlertsDisabled: true, PortalMonthlyStatementEnabled: true, SessionSecret: secret})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	auth, _ := acceptanceAuth(t, storePath)
	receiver := generationDB(t, filepath.Join(root, "tasks.sqlite"), false)
	funding := generationDB(t, filepath.Join(root, "evidence/funding.sqlite"), true)
	selector, err := GenerationSelector(receiver)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := NewUsageGenerationGuard(auth, selector)
	if err != nil {
		t.Fatal(err)
	}
	g, err := NewGeneration(receiver, funding, filepath.Join(root, "evidence"), guard)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	load, err := NewRetainedAutomaticScopeLoader(g, cfg.ScopeSourceID)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := NewObservedAutomaticContext(g, load)
	if err != nil {
		t.Fatal(err)
	}
	pub := func(ctx context.Context, req monthlytasks.Request, body []byte) error {
		_, err := m.PublishMonthlyBillForAuthorization(ctx, body, monitor.MonthlyBillExpectedAuthorization{GroupID: req.Owner.GroupID, AuthVersion: req.Owner.AuthVersion, MemberFingerprint: req.Owner.MemberFingerprint, MemberManifest: req.Owner.MemberManifest})
		return err
	}
	runtime, err := NewAutomaticLocalRuntime(g, auth, GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: cfg.Owner.GroupID, Month: cfg.Month}}}, metadata, pub)
	if err != nil {
		t.Fatal(err)
	}
	stop, stopSignal := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignal()
	if runtime.Start(stop) != nil {
		t.Fatal("local generation runtime start")
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Stop(ctx); err != nil {
			t.Error(err)
			_ = runtime.Stop(context.Background())
		}
	}()
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	_ = r.SetTrustedProxies(nil)
	r.Use(func(c *gin.Context) {
		if c.Request.Host != "localhost:8197" && c.Request.Host != "127.0.0.1:8197" {
			c.AbortWithStatus(403)
			return
		}
		c.Header("Cache-Control", "private, no-store")
		c.Header("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; form-action 'self'")
		if c.Request.Method != http.MethodGet && !(c.Request.Method == http.MethodPost && (c.Request.URL.Path == "/login" || c.Request.URL.Path == "/api/monthly-statement/generation")) {
			c.AbortWithStatus(403)
			return
		}
		if c.Request.Method == http.MethodGet {
			switch c.Request.URL.Path {
			case "/", "/login", "/logout", "/echarts.js", "/flatpickr.js", "/flatpickr.css", "/range-picker.js", "/react.js", "/react-dom.js", "/semi-ui.js", "/semi-ui.css", "/api/monthly-statements", "/api/monthly-statement", "/api/monthly-statement/download", "/api/monthly-statement/generation":
			default:
				c.AbortWithStatusJSON(503, gin.H{"error": "本机真实账单验收；使用日志请继续访问原8196服务"})
				return
			}
		}
		c.Next()
	})
	if err := m.RegisterPortalRoutesWithGeneration(r, runtime); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp4", "127.0.0.1:8197")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: r, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	defer srv.Close()
	go func() { <-stop.Done(); _ = srv.Close() }()
	t.Log("REAL_MONTHLY_READY http://localhost:8197/login ; local login usage-local@example.invalid / LocalUsage2026! ; retained real customer sources ; no source connection ; runs until user asks to stop")
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		t.Fatal(err)
	}
}
