package monthlydraft

// Opt-in local acceptance adapter, not production code or a customer-specific
// money rule. It preserves pinned originals and builds the SAME product inputs
// from an explicit multi-account retained source selection. No source network.
import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"golang.org/x/crypto/bcrypt"
)

type retainedMultiAccountRequest struct {
	Version, GroupID                                int64
	Month                                           string
	Input, Scope, Directory, FundingPlan, FundingDB monthlysource.MonthlyObservationFile
	ExpectedBill, ExpectedWorkbook                  monthlysource.MonthlyObservationFile
	Tokens                                          []monthlysource.MonthlyObservationTokenFile
}

func TestRetainedMultiAccountPreparation(t *testing.T) {
	requestPath, output := os.Getenv("MONTHLY_MULTI_SOURCE_REQUEST"), os.Getenv("MONTHLY_MULTI_SOURCE_OUTPUT")
	if requestPath == "" {
		t.Skip("explicit pinned retained multi-account request and new output required")
	}
	actual, err := filepath.EvalSymlinks(output)
	info, statErr := os.Lstat(output)
	if err != nil || statErr != nil || actual != output || !filepath.IsAbs(output) || !info.IsDir() || info.Mode().Perm() != 0700 {
		t.Fatal("new canonical private output required")
	}
	if entries, err := os.ReadDir(output); err != nil || len(entries) != 0 {
		t.Fatal("empty output required")
	}
	raw := acceptancePrivateRead(t, requestPath, generationPlanLimit)
	if generationHash(raw) != os.Getenv("MONTHLY_MULTI_SOURCE_SHA256") {
		t.Fatal("private request differs from explicit pin")
	}
	var r retainedMultiAccountRequest
	if json.Unmarshal(raw, &r) != nil || r.Version != 1 || r.GroupID <= 0 || len(r.Tokens) < 2 || len(r.Tokens) > 200 {
		t.Fatal("multi-account scope invalid")
	}
	originals := map[string]string{requestPath: generationHash(raw)}
	remaining := generationEvidenceLimit
	read := func(f monthlysource.MonthlyObservationFile) []byte {
		t.Helper()
		if !generationHex(f.SHA256, 32) || !filepath.IsAbs(f.Path) {
			t.Fatal("invalid pinned source")
		}
		body := acceptancePrivateRead(t, f.Path, remaining)
		if generationHash(body) != f.SHA256 {
			t.Fatal("pinned original changed")
		}
		remaining -= int64(len(body))
		originals[f.Path] = f.SHA256
		return body
	}
	defer func() {
		for p, sha := range originals {
			body, err := os.ReadFile(p)
			if err != nil || generationHash(body) != sha {
				t.Error("retained original changed", err)
			}
		}
	}()
	input, err := monitor.DecodeMonthlyPreparationInput(read(r.Input))
	if err != nil || len(input.Accounts) != len(r.Tokens) {
		t.Fatal("complete normalized account scope required", err)
	}
	financialBefore, err := json.Marshal(input.Accounts)
	if err != nil {
		t.Fatal(err)
	}
	var scope AutomaticScope
	if json.Unmarshal(read(r.Scope), &scope) != nil || scope.Group.GroupID != r.GroupID || len(scope.Group.Months) != 1 || scope.Group.Months[0] != r.Month || len(scope.Accounts) != len(r.Tokens) {
		t.Fatal("independent scope differs")
	}
	var directory struct {
		Groups []struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		} `json:"groups"`
		Users []monitor.TrackedUser `json:"users"`
	}
	directoryRaw := read(r.Directory)
	if json.Unmarshal(directoryRaw, &directory) != nil || len(directory.Groups) != 1 || directory.Groups[0].ID != r.GroupID || directory.Groups[0].Name != scope.Group.Name || len(directory.Users) != len(input.Accounts) {
		t.Fatal("complete retained directory differs")
	}
	var fundingPlan struct {
		Version int                               `json:"version"`
		GroupID int64                             `json:"group_id"`
		Funding monthlysource.FundingMonthRequest `json:"funding"`
		Windows []struct {
			Version monthlysource.FundingMonthWindowVersion `json:"version"`
		} `json:"windows"`
	}
	if json.Unmarshal(read(r.FundingPlan), &fundingPlan) != nil || fundingPlan.GroupID != r.GroupID || fundingPlan.Funding.Month != r.Month {
		t.Fatal("captured funding scope differs")
	}
	ids := []int64{}
	for _, a := range input.Accounts {
		if a.GroupID != r.GroupID || !slices.Equal(a.Months, []string{r.Month}) || len(a.TokenMonths) != 0 {
			t.Fatal("account month changed or already normalized")
		}
		ids = append(ids, a.UserID)
	}
	slices.Sort(ids)
	if !slices.Equal(ids, fundingPlan.Funding.UserIDs) {
		t.Fatal("funding capture omitted organization members")
	}
	tokenIDs := []int64{}
	for _, token := range r.Tokens {
		tokenIDs = append(tokenIDs, token.UserID)
	}
	slices.Sort(tokenIDs)
	if !slices.Equal(tokenIDs, ids) {
		t.Fatal("token selectors differ from complete organization scope")
	}
	evidence := filepath.Join(output, "evidence")
	if err := os.Mkdir(evidence, 0700); err != nil {
		t.Fatal(err)
	}
	for i, source := range input.Sources {
		if !validScopeSourceID(source.ID) {
			t.Fatal("invalid source name")
		}
		body := read(monthlysource.MonthlyObservationFile{Path: source.Path, SHA256: source.SHA256})
		p := filepath.Join(evidence, source.ID+".source")
		acceptanceWriteNew(t, p, body)
		input.Sources[i].Path = p
	}
	addSource := func(id, name string, body []byte) {
		t.Helper()
		p := filepath.Join(evidence, name)
		acceptanceWriteNew(t, p, body)
		input.Sources = append(input.Sources, monitor.MonthlyPreparationSource{ID: id, Path: p, SHA256: generationHash(body)})
	}
	addSource("current_directory", "directory.json", directoryRaw)
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(r.FundingDB.Path + suffix); !os.IsNotExist(err) {
			t.Fatal("funding original must be sealed")
		}
	}
	addSource("fresh_funding", "funding.sqlite", read(r.FundingDB))
	tokenSources := []TokenUsageSourceSelection{}
	for _, token := range r.Tokens {
		if !slices.Contains(ids, token.UserID) {
			t.Fatal("token source belongs to another user")
		}
		id := "token_" + strconv.FormatInt(token.UserID, 10)
		addSource(id, id+".json", read(token.File))
		tokenSources = append(tokenSources, TokenUsageSourceSelection{UserID: token.UserID, SourceID: id})
	}
	storePath := filepath.Join(output, "usage.db")
	m, err := monitor.New(monitor.Settings{StorePath: storePath, LocalSnapshotOnly: true, AlertsDisabled: true,
		PortalMonthlyStatementEnabled: true, SessionSecret: retainedAcceptanceSecret(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	auth, _ := acceptanceAuth(t, storePath)
	hash, err := bcrypt.GenerateFromPassword([]byte("SyntheticLocalOnly!2026"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	customer := monitor.CustomerGroup{ID: r.GroupID, Name: directory.Groups[0].Name,
		PortalEmail: "multi-account-offline@example.invalid", PortalPwAdmin: string(hash), PortalAuthVer: 1}
	if auth.Create(&customer).Error != nil || auth.Create(&directory.Users).Error != nil {
		t.Fatal("local customer metadata creation failed")
	}
	owner, _, err := LoadUsageGenerationOwner(t.Context(), auth, r.GroupID, 1)
	if err != nil {
		t.Fatal(err)
	}
	receiverPath := filepath.Join(output, "tasks.sqlite")
	receiver := generationDB(t, receiverPath, false)
	if InitGenerationReceiver(t.Context(), receiver) != nil || InitGenerationSelections(t.Context(), receiver) != nil || os.Chmod(receiverPath, 0600) != nil {
		t.Fatal("fresh local receiver initialization failed")
	}
	funding := generationDB(t, filepath.Join(evidence, "funding.sqlite"), true)
	g, err := NewGeneration(receiver, funding, evidence, func(context.Context, monthlytasks.Request) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	inputRaw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	plan := GenerationPlan{Version: fundingPlan.Version, Owner: owner, Funding: fundingPlan.Funding,
		PreparationSHA256: generationHash(inputRaw), TokenUsageSources: tokenSources}
	for _, w := range fundingPlan.Windows {
		plan.Windows = append(plan.Windows, w.Version)
	}
	normalized, err := g.PrepareTokenUsageInput(t.Context(), plan, inputRaw)
	if err != nil {
		t.Fatal("full multi-account token normalization failed", err)
	}
	if json.Unmarshal(normalized.Plan, &plan) != nil || json.Unmarshal(normalized.Preparation, &input) != nil {
		t.Fatal("normalized preparation unavailable")
	}
	accounts := append([]monitor.MonthlyPreparationAccount{}, input.Accounts...)
	for i := range accounts {
		accounts[i].TokenMonths = nil
	}
	after, err := json.Marshal(accounts)
	if err != nil || !reflect.DeepEqual(after, financialBefore) {
		t.Fatal("token normalization changed original money or history")
	}
	const scopeID = "independent_retained_scope"
	doc := RetainedAutomaticScope{Version: 1, Owner: owner, Month: r.Month, SourceIdentity: plan.Funding.SourceIdentity, Scope: scope}
	scopeRaw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	addSource(scopeID, "scope.json", scopeRaw)
	inputRaw, err = json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	plan.AutomaticScopeSourceID, plan.PreparationSHA256 = scopeID, generationHash(inputRaw)
	plan, planRaw, _, err := canonicalGeneration(plan, inputRaw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	checkRoot := filepath.Join(evidence, "preflight")
	if err := os.Mkdir(checkRoot, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := DeploymentConfig{ReceiverPath: receiverPath, FundingPath: filepath.Join(evidence, "funding.sqlite"), AuthorityPath: storePath,
		EvidenceRoot: evidence, Rollout: GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: r.GroupID, Month: r.Month}}}}
	checked, err := PrepareRetainedGenerationFiles(t.Context(), cfg, RetainedPreparationRequest{Plan: planRaw, Preparation: inputRaw, OutputRoot: checkRoot})
	if err != nil || checked.Complete {
		t.Fatal("formal multi-account retained preparation rejected", err)
	}
	acceptanceWriteNew(t, filepath.Join(output, "plan.json"), planRaw)
	acceptanceWriteNew(t, filepath.Join(output, "input.json"), inputRaw)
	config, err := json.Marshal(retainedAcceptanceConfig{Owner: owner, Month: r.Month, SourceIdentity: plan.Funding.SourceIdentity, ScopeSourceID: scopeID})
	if err != nil {
		t.Fatal(err)
	}
	acceptanceWriteNew(t, filepath.Join(output, "config.json"), config)
	acceptanceWriteNew(t, filepath.Join(output, "actual-customer-bill.json"), read(r.ExpectedBill))
	acceptanceWriteNew(t, filepath.Join(output, "actual-customer-bill.xlsx"), read(r.ExpectedWorkbook))
	t.Logf("pinned multi-account preparation passed for %d members; no customer task/publication created", len(ids))
}
