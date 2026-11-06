package monthlydraft

// Opt-in retained-data check, not a customer-specific product path. This checks
// the NEW filled-history adapter against old originals and newer sealed raw
// captures. It does not attest missing wallet boundaries or issue a bill.
import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

type retainedHistoryPreparationRequest struct {
	Version, FundingVersion               int
	UserID                                int64
	Month                                 string
	Template, Selection, Database, Tokens monthlysource.MonthlyObservationFile
}

func TestRetainedFilledHistoryObservedPreparation(t *testing.T) {
	testRetainedFilledHistoryObservedPreparation(t, false)
}

func TestRetainedHistoryWithReusedWalletPreparation(t *testing.T) {
	testRetainedFilledHistoryObservedPreparation(t, true)
}

func testRetainedFilledHistoryObservedPreparation(t *testing.T, reuse bool) {
	path := os.Getenv("MONTHLY_REAL_HISTORY_PREPARATION")
	if path == "" {
		t.Skip("explicit private pinned historical/raw source selection required")
	}
	expected := os.Getenv("MONTHLY_REAL_HISTORY_PREPARATION_SHA256")
	raw := acceptancePrivateRead(t, path, generationPlanLimit)
	var r retainedHistoryPreparationRequest
	if !generationHex(expected, 32) || generationHash(raw) != expected || json.Unmarshal(raw, &r) != nil {
		t.Fatal("wrong private source selection")
	}
	canonical, err := json.Marshal(r)
	if err != nil || !bytes.Equal(bytes.TrimSpace(raw), canonical) || r.Version != 1 || r.UserID <= 0 {
		t.Fatal("invalid private source selection")
	}
	remaining := generationEvidenceLimit
	originals := map[string]string{}
	read := func(ref monthlysource.MonthlyObservationFile) []byte {
		t.Helper()
		actual, err := filepath.EvalSymlinks(ref.Path)
		info, statErr := os.Lstat(ref.Path)
		if err != nil || statErr != nil || actual != ref.Path || !filepath.IsAbs(ref.Path) || !info.Mode().IsRegular() || info.Size() > remaining || !generationHex(ref.SHA256, 32) {
			t.Fatal("unsafe/missing pinned original")
		}
		body, err := os.ReadFile(ref.Path)
		if err != nil || generationHash(body) != ref.SHA256 {
			t.Fatal("original differs from pinned source")
		}
		if previous, exists := originals[ref.Path]; exists && previous != ref.SHA256 {
			t.Fatal("original path has conflicting references")
		}
		originals[ref.Path] = ref.SHA256
		remaining -= int64(len(body))
		return body
	}
	defer func() {
		for path, sha := range originals {
			raw, err := os.ReadFile(path)
			if err != nil || generationHash(raw) != sha {
				t.Error("retained source changed", err)
			}
		}
	}()
	historyRaw, selectionRaw, databaseRaw, tokenRaw := read(r.Template), read(r.Selection), read(r.Database), read(r.Tokens)
	input, err := monitor.DecodeMonthlyPreparationInput(historyRaw)
	if err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(input.Accounts, func(a monitor.MonthlyPreparationAccount) bool { return a.UserID == r.UserID })
	if index < 0 || !slices.Contains(input.Accounts[index].Months, r.Month) {
		t.Fatal("selected original member/month missing")
	}
	original := input.Accounts[index]
	original.Months = []string{r.Month}
	if len(original.TokenMonths) != 0 {
		t.Fatal("test requires original history before token filling")
	}
	input.Accounts = []monitor.MonthlyPreparationAccount{original}
	refs, digests := organizationHistoryRefs(input.Accounts, AutomaticScope{})
	root := t.TempDir()
	retainedOutput := ""
	if reuse {
		retainedOutput = os.Getenv("MONTHLY_REAL_WALLET_REUSE_OUTPUT")
	}
	if retainedOutput != "" {
		actual, err := filepath.EvalSymlinks(retainedOutput)
		info, statErr := os.Lstat(retainedOutput)
		if err != nil || statErr != nil || actual != retainedOutput || !filepath.IsAbs(retainedOutput) || !info.IsDir() || info.Mode().Perm() != 0700 {
			t.Fatal("new canonical private real-test output required")
		}
		entries, err := os.ReadDir(retainedOutput)
		if err != nil || len(entries) != 0 {
			t.Fatal("real-test output must be empty")
		}
		root = retainedOutput
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	sources := []monitor.MonthlyPreparationSource{}
	for _, s := range input.Sources {
		if !refs[s.ID] && !digests[s.SHA256] {
			continue
		}
		body := read(monthlysource.MonthlyObservationFile{Path: s.Path, SHA256: s.SHA256})
		if !validScopeSourceID(s.ID) {
			t.Fatal("invalid old source ID")
		}
		file := filepath.Join(root, s.ID+".source")
		acceptanceWriteNew(t, file, body)
		s.Path = file
		sources = append(sources, s)
		delete(refs, s.ID)
		delete(digests, s.SHA256)
	}
	if len(refs) != 0 || len(digests) != 0 {
		t.Fatal("required original evidence is not available")
	}
	input.Sources = sources
	for name, body := range map[string][]byte{"retained-history": historyRaw, "retained-observation": databaseRaw, "retained-tokens": tokenRaw, "retained-selection": selectionRaw} {
		file := filepath.Join(root, name+".source")
		acceptanceWriteNew(t, file, body)
		input.Sources = append(input.Sources, monitor.MonthlyPreparationSource{ID: name, Path: file, SHA256: generationHash(body)})
	}
	var selected struct {
		GroupID int64                                     `json:"group_id"`
		Funding monthlysource.FundingMonthRequest         `json:"funding"`
		Windows []monthlysource.FundingMonthWindowVersion `json:"funding_windows"`
		Usage   []monthlysource.UsageWindowVersion        `json:"usage_windows"`
		Wallets []string                                  `json:"wallet_ids"`
	}
	if json.Unmarshal(selectionRaw, &selected) != nil || selected.GroupID != original.GroupID || selected.Funding.Month != r.Month || !slices.Equal(selected.Funding.UserIDs, []int64{r.UserID}) {
		t.Fatal("raw captures differ from retained account/month")
	}
	databasePath := filepath.Join(root, "retained-observation.source")
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Stat(r.Database.Path + suffix); !os.IsNotExist(err) {
			t.Fatal("raw observation DB not sealed")
		}
	}
	receiver := generationDB(t, filepath.Join(root, "test-receiver.sqlite"), false)
	if err := InitGenerationReceiver(t.Context(), receiver); err != nil {
		t.Fatal(err)
	}
	if err := InitGenerationSelections(t.Context(), receiver); err != nil {
		t.Fatal(err)
	}
	funding := generationDB(t, databasePath, true)
	g, err := NewGeneration(receiver, funding, root, func(context.Context, monthlytasks.Request) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	template := draftJSON(t, input)
	p := GenerationPlan{Version: r.FundingVersion, Owner: monthlytasks.Owner{GroupID: original.GroupID, AuthVersion: 1,
		MemberFingerprint: generationMemberHash([]int64{r.UserID}), MemberManifest: strings.Repeat("b", 64)},
		Funding: selected.Funding, PreparationSHA256: generationHash(template), Windows: selected.Windows, UsageWindows: selected.Usage,
		ObservationStoreSourceID: "retained-observation", WalletObservations: &WalletObservationSelection{SourceID: "retained-observation", IDs: selected.Wallets},
		TokenUsageSources: []TokenUsageSourceSelection{{UserID: r.UserID, SourceID: "retained-tokens"}}}
	added := len(selected.Wallets)
	if reuse {
		m := monthlysource.MonthlyObservationMaterial{Version: 1, Mode: monthlysource.MonthlyObservationMaterialMode, GroupID: original.GroupID, FundingVersion: r.FundingVersion,
			Funding: selected.Funding, FundingWindows: selected.Windows, UsageWindows: selected.Usage, WalletIDs: selected.Wallets,
			Database: monthlysource.MonthlyObservationFile{Path: databasePath, SHA256: generationHash(databaseRaw)},
			Tokens:   []monthlysource.MonthlyObservationTokenFile{{UserID: r.UserID, File: monthlysource.MonthlyObservationFile{Path: filepath.Join(root, "retained-tokens.source"), SHA256: generationHash(tokenRaw)}}}}
		originRaw := materialPreparationJSON(t, m)
		origin := monthlysource.MonthlyObservationFile{Path: filepath.Join(root, "original-material.json"), SHA256: generationHash(originRaw)}
		acceptanceWriteNew(t, origin.Path, originRaw)
		meta, err := monthlysource.BuildWalletReuseMaterial(t.Context(), originRaw, origin, map[int64][]byte{r.UserID: tokenRaw}, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		body := materialPreparationJSON(t, meta)
		metaPath := filepath.Join(root, "wallet-reuse.json")
		acceptanceWriteNew(t, metaPath, body)
		input.Sources = append(input.Sources, monitor.MonthlyPreparationSource{ID: "reused-wallets", Path: metaPath, SHA256: generationHash(body)},
			monitor.MonthlyPreparationSource{ID: "reuse-origin", Path: origin.Path, SHA256: origin.SHA256})
		p.WalletObservations = &WalletObservationSelection{SourceID: "retained-observation", IDs: []string{}, ReuseSourceID: "reused-wallets"}
		template = draftJSON(t, input)
		p.PreparationSHA256 = generationHash(template)
		added = 2
	}
	prepared, err := g.prepareObservedHistoryInput(t.Context(), p, template, "retained-observation")
	if err != nil {
		t.Fatal("real filled history rejected by new adapter", err)
	}
	result, err := monitor.DecodeMonthlyPreparationInput(prepared.Preparation)
	if err != nil || len(result.Accounts) != 1 {
		t.Fatal("prepared scope wrong", err)
	}
	a := result.Accounts[0]
	if !reflect.DeepEqual(a.Baseline, original.Baseline) || !reflect.DeepEqual(a.Spans, original.Spans) || !reflect.DeepEqual(a.MovementCoverage, original.MovementCoverage) || len(a.Movements) != len(original.Movements) || len(a.Snapshots) != len(original.Snapshots)+added || len(a.TokenMonths) != 1 {
		t.Fatal("real history or original money changed")
	}
	for i, m := range a.Movements {
		old := original.Movements[i]
		old.Source = m.Source
		if old != m {
			t.Fatal("real funding changed")
		}
	}
	var outPlan GenerationPlan
	if json.Unmarshal(prepared.Plan, &outPlan) != nil {
		t.Fatal("invalid prepared plan")
	}
	view, release, err := g.bindObservationStore(t.Context(), outPlan, result)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := view.auditUsageInput(t.Context(), outPlan, result); err != nil {
		t.Fatal("real usage re-audit failed", err)
	}
	if _, err := view.auditTokenInput(t.Context(), outPlan, result); err != nil {
		t.Fatal("real token re-audit failed", err)
	}
	if _, err := view.auditWalletInput(t.Context(), outPlan, result); err != nil {
		t.Fatal("real observation re-audit failed", err)
	}
	month, _ := time.ParseInLocation("2006-01", r.Month, time.FixedZone("UTC+8", 8*3600))
	var consume, refund int64
	for _, s := range a.Spans {
		if s.From >= month.Unix() && s.To <= month.AddDate(0, 1, 0).Unix() {
			var ok bool
			consume, ok = usagePreparationAdd(consume, s.ConsumeQuota)
			if !ok {
				t.Fatal("consume overflow")
			}
			refund, ok = usagePreparationAdd(refund, s.RefundQuota)
			if !ok {
				t.Fatal("refund overflow")
			}
		}
	}
	var count int
	for _, table := range []string{"portal_monthly_generation_inputs", "portal_monthly_generation_selections", "portal_monthly_draft_tasks"} {
		if err := receiver.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatal("preparation wrote task or input", table, err)
		}
	}
	if retainedOutput != "" {
		acceptanceWriteNew(t, filepath.Join(root, "prepared-input.json"), prepared.Preparation)
		acceptanceWriteNew(t, filepath.Join(root, "prepared-plan.json"), prepared.Plan)
	}
	t.Logf("real retained history preserved and newer raw usage/funding/token/wallet re-audited: user=%d month=%s consume_quota=%d refund_quota=%d reuse=%t observations_added=%d; synthetic receiver/authorization only, NOT historical boundary or customer-bill approval", r.UserID, r.Month, consume, refund, reuse, added)
}
