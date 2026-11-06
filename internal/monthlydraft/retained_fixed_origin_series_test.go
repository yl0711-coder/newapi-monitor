package monthlydraft

// Opt-in full real-source series acceptance. This file is test-only: it cannot
// enable a production target, create an upstream observation or change money.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"golang.org/x/crypto/bcrypt"
)

type retainedFixedMonth struct {
	Month           string `json:"month"`
	Directory       string `json:"directory"`
	PlanSHA256      string `json:"plan_sha256"`
	InputSHA256     string `json:"input_sha256"`
	ReferenceKind   string `json:"reference_kind"`
	ReferenceSHA256 string `json:"reference_sha256"`
}
type retainedFixedManifest struct {
	Version        int                  `json:"version"`
	GroupID        int64                `json:"group_id"`
	SourceIdentity string               `json:"source_identity"`
	From           string               `json:"from"`
	Through        string               `json:"through"`
	Months         []retainedFixedMonth `json:"months"`
}
type retainedFixedPrepared struct {
	plan, input []byte
	config      retainedAcceptanceConfig
	fundingPath string
	reference   []byte
	kind        string
}

func TestRetainedRealFixedOriginSeriesCustomerJourney(t *testing.T) {
	manifestPath, expected, output := os.Getenv("MONTHLY_REAL_SERIES_MANIFEST"), os.Getenv("MONTHLY_REAL_SERIES_SHA256"), os.Getenv("MONTHLY_REAL_SERIES_OUTPUT")
	if manifestPath == "" {
		t.Skip("explicit hash-pinned real monthly series and fresh private output required")
	}
	capacityMode := os.Getenv("MONTHLY_REAL_SERIES_CAPACITY")
	if capacityMode != "" && capacityMode != "published-v2" {
		t.Fatal("unknown explicit real-series capacity test mode")
	}
	raw := acceptancePrivateRead(t, manifestPath, 65536)
	var manifest retainedFixedManifest
	if !generationHex(expected, 32) || generationHash(raw) != expected || json.Unmarshal(raw, &manifest) != nil {
		t.Fatal("real series selection changed")
	}
	canonical, err := json.Marshal(manifest)
	if err != nil || !bytes.Equal(raw, canonical) || manifest.Version != 1 || manifest.GroupID <= 0 || !generationHex(manifest.SourceIdentity, 32) || len(manifest.Months) < 1 || len(manifest.Months) > 120 {
		t.Fatal("invalid real series manifest")
	}
	start, err := time.Parse("2006-01", manifest.From)
	if err != nil || manifest.Months[0].Month != manifest.From || manifest.Months[len(manifest.Months)-1].Month != manifest.Through {
		t.Fatal("fixed real origin and through must match the complete selected series")
	}
	for i, month := range manifest.Months {
		if start.AddDate(0, i, 0).Format("2006-01") != month.Month || !generationHex(month.PlanSHA256, 32) || !generationHex(month.InputSHA256, 32) || !generationHex(month.ReferenceSHA256, 32) || (month.ReferenceKind != "original_quota" && month.ReferenceKind != "saved_bill") {
			t.Fatal("noncontiguous/unpinned real monthly series")
		}
		retainedFixedPrivateDirectory(t, month.Directory)
		if month.Directory == output || strings.HasPrefix(output, month.Directory+string(filepath.Separator)) || strings.HasPrefix(month.Directory, output+string(filepath.Separator)) {
			t.Fatal("original sources and output must be independent")
		}
	}
	retainedFixedPrivateDirectory(t, output)
	if entries, err := os.ReadDir(output); err != nil || len(entries) != 0 {
		t.Fatal("fresh empty real-series output required")
	}
	originals := map[string]string{}
	read := func(path string, limit int64) []byte {
		t.Helper()
		raw := acceptancePrivateRead(t, path, limit)
		pin := generationHash(raw)
		if old, ok := originals[path]; ok && old != pin {
			t.Fatal("original changed between series reads")
		}
		originals[path] = pin
		return raw
	}
	defer func() {
		for p, pin := range originals {
			if generationHash(acceptancePrivateRead(t, p, generationEvidenceLimit)) != pin {
				t.Error("real original changed during series acceptance")
			}
		}
	}()
	var financialReference map[string][]byte
	var workbookReference map[string]map[string][]byte
	for _, reverse := range []bool{false, true} {
		name := "forward"
		if reverse {
			name = "reverse"
		}
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(output, name)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			bills, books := retainedFixedOrder(t, manifest, dir, reverse, read, capacityMode == "published-v2")
			if !reverse {
				financialReference, workbookReference = bills, books
				return
			}
			for month, body := range bills {
				if !bytes.Equal(body, financialReference[month]) {
					t.Fatal("real financial/display payload depends on request order", month)
				}
				for _, part := range []string{"xl/worksheets/sheet2.xml", "xl/worksheets/sheet3.xml"} {
					if !bytes.Equal(books[month][part], workbookReference[month][part]) {
						t.Fatal("real account/token worksheet depends on request order", month, part)
					}
				}
			}
		})
	}
}

func retainedFixedPrivateDirectory(t *testing.T, path string) {
	t.Helper()
	actual, err := filepath.EvalSymlinks(path)
	st, statErr := os.Lstat(path)
	if err != nil || statErr != nil || actual != path || !filepath.IsAbs(path) || !st.IsDir() || st.Mode().Perm() != 0700 {
		t.Fatal("canonical 0700 real-series directory required")
	}
}

func retainedFixedOrder(t *testing.T, manifest retainedFixedManifest, dir string, reverse bool, read func(string, int64) []byte, capacity bool) (map[string][]byte, map[string]map[string][]byte) {
	t.Helper()
	ctx := context.Background()
	evidence := filepath.Join(dir, "evidence")
	if err := os.Mkdir(evidence, 0700); err != nil {
		t.Fatal(err)
	}
	profiles := map[string]retainedFixedPrepared{}
	var owner monthlytasks.Owner
	var directoryRaw []byte
	var directoryGroup monitor.MonthlyBundleGroup
	for _, selected := range manifest.Months {
		planRaw := read(filepath.Join(selected.Directory, "plan.json"), generationPlanLimit)
		inputRaw := read(filepath.Join(selected.Directory, "input.json"), monitor.MonthlyPreparationInputLimit)
		if generationHash(planRaw) != selected.PlanSHA256 || generationHash(inputRaw) != selected.InputSHA256 {
			t.Fatal("real source selection digest differs")
		}
		var p GenerationPlan
		if json.Unmarshal(planRaw, &p) != nil {
			t.Fatal("invalid real plan")
		}
		p, canonical, input, err := canonicalGeneration(p, inputRaw, time.Now())
		if err != nil || !bytes.Equal(canonical, planRaw) || p.Owner.GroupID != manifest.GroupID || p.Funding.Month != selected.Month || p.Funding.SourceIdentity != manifest.SourceIdentity {
			t.Fatal("real month plan/scope differs", err)
		}
		if owner.GroupID == 0 {
			owner = p.Owner
		} else if owner != p.Owner {
			t.Fatal("this real-series fixture requires one unchanged current authority")
		}
		var config retainedAcceptanceConfig
		if json.Unmarshal(read(filepath.Join(selected.Directory, "config.json"), 4096), &config) != nil || config.Owner != owner || config.Month != selected.Month || config.SourceIdentity != manifest.SourceIdentity || config.ScopeSourceID != p.AutomaticScopeSourceID {
			t.Fatal("real monthly configuration differs")
		}
		if config.ScopeSourceID != "independent_retained_scope" {
			t.Fatal("unexpected explicit real-scope source selection")
		}
		monthDir := filepath.Join(evidence, "data-"+selected.Month)
		if err := os.Mkdir(monthDir, 0700); err != nil {
			t.Fatal(err)
		}
		financialBefore, err := json.Marshal(input.Accounts)
		if err != nil {
			t.Fatal(err)
		}
		fundingPath := ""
		for i, s := range input.Sources {
			rel, err := filepath.Rel(filepath.Join(selected.Directory, "evidence"), s.Path)
			if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || !validScopeSourceID(s.ID) {
				t.Fatal("real source outside the fixed private month")
			}
			body := read(s.Path, generationEvidenceLimit)
			if generationHash(body) != s.SHA256 {
				t.Fatal("real original digest changed")
			}
			file := filepath.Join(monthDir, s.ID+".source")
			if filepath.Base(s.Path) == "funding.sqlite" {
				file, fundingPath = filepath.Join(monthDir, "funding.sqlite"), filepath.Join(monthDir, "funding.sqlite")
			}
			if s.ID == "current_directory" {
				if directoryRaw != nil && !bytes.Equal(directoryRaw, body) {
					t.Fatal("real current directory changed between selected months")
				}
				directoryRaw = bytes.Clone(body)
			}
			if s.ID == p.AutomaticScopeSourceID {
				var retained RetainedAutomaticScope
				if json.Unmarshal(body, &retained) != nil || retained.Version != 1 || retained.Owner != owner || retained.Month != selected.Month || retained.SourceIdentity != manifest.SourceIdentity || retained.Scope.Group.GroupID != owner.GroupID {
					t.Fatal("independent directory scope differs")
				}
				if directoryGroup.GroupID != 0 && (directoryGroup.GroupID != retained.Scope.Group.GroupID || directoryGroup.Name != retained.Scope.Group.Name) {
					t.Fatal("independent directory group changed between months")
				}
				directoryGroup = retained.Scope.Group
			}
			acceptanceWriteNew(t, file, body)
			input.Sources[i].Path = file
		}
		financialAfter, err := json.Marshal(input.Accounts)
		if err != nil || !bytes.Equal(financialBefore, financialAfter) || fundingPath == "" {
			t.Fatal("real relocation changed financial input or omitted funding")
		}
		inputRaw, err = json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		p.PreparationSHA256 = generationHash(inputRaw)
		_, planRaw, _, err = canonicalGeneration(p, inputRaw, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		refName := "actual-customer-bill.json"
		if selected.ReferenceKind == "original_quota" {
			refName = "independent-expected-quota.json"
		}
		reference := read(filepath.Join(selected.Directory, refName), 2<<20)
		if generationHash(reference) != selected.ReferenceSHA256 {
			t.Fatal("independent monetary reference changed")
		}
		profiles[selected.Month] = retainedFixedPrepared{plan: planRaw, input: inputRaw, config: config, fundingPath: fundingPath, reference: reference, kind: selected.ReferenceKind}
	}
	var directory struct {
		Groups []struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		} `json:"groups"`
		Users []monitor.TrackedUser `json:"users"`
	}
	if json.Unmarshal(directoryRaw, &directory) != nil {
		t.Fatal("invalid original organization directory")
	}
	// Older retained formats carry the group identity in the independently
	// pinned scope, and only the member rows in current_directory. Match the
	// existing inbox compatibility; never infer it from a customer label.
	if len(directory.Groups) == 0 && directoryGroup.GroupID == owner.GroupID && directoryGroup.Name != "" {
		directory.Groups = append(directory.Groups, struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		}{ID: directoryGroup.GroupID, Name: directoryGroup.Name})
	}
	if len(directory.Groups) != 1 || directory.Groups[0].ID != owner.GroupID || len(directory.Users) == 0 {
		t.Fatal("complete original organization directory required")
	}
	storePath := filepath.Join(dir, "usage.db")
	m, err := monitor.New(monitor.Settings{StorePath: storePath, LocalSnapshotOnly: true, AlertsDisabled: true, PortalMonthlyStatementEnabled: true, SessionSecret: retainedAcceptanceSecret(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	auth, _ := acceptanceAuth(t, storePath)
	const password = "SyntheticOfflineSeries!2026"
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	customer := monitor.CustomerGroup{ID: owner.GroupID, Name: directory.Groups[0].Name, PortalEmail: "series-offline@example.invalid", PortalPwAdmin: string(hash), PortalAuthVer: owner.AuthVersion}
	if auth.Create(&customer).Error != nil || auth.Create(&directory.Users).Error != nil {
		t.Fatal("isolated real-series authority creation failed")
	}
	actual, _, err := LoadUsageGenerationOwner(ctx, auth, owner.GroupID, owner.AuthVersion)
	if err != nil || actual != owner {
		t.Fatal("isolated authority differs from both original months", err)
	}
	receiverPath := filepath.Join(dir, "receiver.sqlite")
	receiver := generationDB(t, receiverPath, false)
	if InitGenerationReceiver(ctx, receiver) != nil || InitGenerationSelections(ctx, receiver) != nil || os.Chmod(receiverPath, 0600) != nil {
		t.Fatal("isolated real-series receiver initialization failed")
	}
	cfg := DeploymentConfig{Rollout: GenerationRollout{Enabled: true}, ReceiverPath: receiverPath, AuthorityPath: storePath, EvidenceRoot: evidence, FundingPath: profiles[manifest.From].fundingPath}
	entries := map[string]PreparedInputInboxEntry{}
	for _, month := range manifest.Months {
		target := GenerationTarget{GroupID: owner.GroupID, Month: month.Month}
		cfg.Rollout.Targets = append(cfg.Rollout.Targets, target)
		one := cfg
		one.Rollout = GenerationRollout{Enabled: true, Targets: []GenerationTarget{target}}
		prepared := filepath.Join(evidence, "prepared-"+month.Month)
		if err := os.Mkdir(prepared, 0700); err != nil {
			t.Fatal(err)
		}
		p := profiles[month.Month]
		pack, err := PrepareRetainedGenerationFiles(ctx, one, RetainedPreparationRequest{Plan: p.plan, Preparation: p.input, OutputRoot: prepared, Write: true})
		if err != nil || !pack.Complete {
			t.Fatal("real month failed unchanged full product preparation", month.Month, err)
		}
		entries[month.Month] = PreparedInputInboxEntry{GroupID: owner.GroupID, Month: month.Month, Revision: 1, Directory: filepath.Base(prepared), ManifestSHA256: pack.ManifestSHA256}
	}
	ranges, err := json.Marshal(AutomaticDisplaySeriesIndex{Version: 1, Ranges: []AutomaticDisplaySeriesRange{{GroupID: owner.GroupID, SourceIdentity: manifest.SourceIdentity, From: manifest.From, Through: manifest.Through}}})
	if err != nil {
		t.Fatal(err)
	}
	acceptanceWriteNew(t, filepath.Join(evidence, "display-series.json"), ranges)
	open := func() *Deployment {
		t.Helper()
		d, err := OpenRetainedAutomaticDisplaySeriesDeployment(ctx, cfg, "independent_retained_scope", "display-series.json", func(ctx context.Context, req monthlytasks.Request, raw []byte, p *monitor.PreparedMonthlyDisplayPublication) error {
			_, err := m.PublishMonthlyBillWithDisplaySeries(ctx, raw, monitor.MonthlyBillExpectedAuthorization{GroupID: req.Owner.GroupID, AuthVersion: req.Owner.AuthVersion, MemberFingerprint: req.Owner.MemberFingerprint, MemberManifest: req.Owner.MemberManifest}, p)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { closeDeploymentTest(t, d) })
		return d
	}
	var d *Deployment
	var router *gin.Engine
	var maintenance *publishedCapacityMaintenance
	coldDirectory := filepath.Join(dir, "cold-payloads")
	if capacity {
		if err := os.Mkdir(coldDirectory, 0700); err != nil {
			t.Fatal(err)
		}
	}
	startRuntime := func() {
		t.Helper()
		d = open()
		if capacity {
			// Test seam ONLY: retained inputs use the existing inbox rather than
			// a live SQL collector. Exercise the same maintenance implementation
			// on real financial copies, with a low test watermark, never a new
			// production switch or an invented missing observation.
			maintenance, err = newPublishedCapacityMaintenance(d.g, auth, coldDirectory)
			if err != nil {
				t.Fatal("real cold owner unavailable", err)
			}
			maintenance.hotCount = 1
			maintenance.scope = d.runtime.scope
			d.capacity = maintenance
			if err := InitPublishedGenerationRetention(ctx, d.receiver); err != nil {
				t.Fatal(err)
			}
		}
		gin.SetMode(gin.TestMode)
		router = gin.New()
		router.Use(gin.Recovery())
		if router.SetTrustedProxies(nil) != nil || m.RegisterPortalRoutesWithGeneration(router, d) != nil || d.Start(ctx) != nil {
			t.Fatal("real-series runtime/routes could not start")
		}
	}
	startRuntime()
	selected := map[string]bool{}
	selectMonth := func(month string) {
		t.Helper()
		if selected[month] {
			return
		}
		r, err := d.receivePreparedInput(ctx, entries[month], "independent_retained_scope")
		if err != nil || !r.Applied {
			t.Fatal("real product input intake failed", month, err)
		}
		selected[month] = true
	}
	request := func(method, path, body string, cookies []*http.Cookie) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, "http://127.0.0.1"+path, strings.NewReader(body))
		for k, v := range map[string]string{"Content-Type": "application/json", "Origin": "http://127.0.0.1", "X-Usage-Bill-Request": "1", "X-Monthly-Statistics": "nullable-v1", "X-Monthly-Accounting": "balance-correction-v1", "X-Monthly-Display": "cents-chain-v1"} {
			r.Header.Set(k, v)
		}
		for _, c := range cookies {
			r.AddCookie(c)
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		return w
	}
	login := request(http.MethodPost, "/login", `{"email":"series-offline@example.invalid","password":"`+password+`"}`, nil)
	if login.Code != http.StatusOK || len(login.Result().Cookies()) == 0 {
		t.Fatal("real-series isolated customer login failed")
	}
	cookies := login.Result().Cookies()
	first, err := time.Parse("2006-01", manifest.From)
	if err != nil {
		t.Fatal(err)
	}
	outside := first.AddDate(0, -1, 0).Format("2006-01")
	if w := request(http.MethodPost, "/api/monthly-statement/generation", `{"month":"`+outside+`"}`, cookies); w.Code < http.StatusBadRequest {
		t.Fatal("customer request before the real fixed origin was accepted", outside, w.Code)
	}
	// Compute the independent cents chain in calendar order from original
	// monetary references, before either customer request order. Never use
	// the returned display_balances as its own acceptance expectation.
	monthNames, references := []string{}, map[string][]byte{}
	for _, month := range manifest.Months {
		p := profiles[month.Month]
		reference := p.reference
		if p.kind == "original_quota" {
			reference = retainedQuotaReference(t, reference, owner.GroupID, month.Month, customer.Name)
		}
		monthNames = append(monthNames, month.Month)
		references[month.Month] = reference
	}
	displayReference, err := retainedIndependentDisplaySeries(monthNames, references)
	if err != nil {
		t.Fatal("independent real chronological cents reference failed", err)
	}
	order := append([]retainedFixedMonth(nil), manifest.Months...)
	if reverse {
		for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
			order[i], order[j] = order[j], order[i]
		}
		for _, month := range manifest.Months {
			selectMonth(month.Month) // predecessor evidence, never customer tasks
		}
	}
	bodies, books := map[string][]byte{}, map[string][]byte{}
	publications := map[string]string{}
	anchor := ""
	for i, month := range order {
		selectMonth(month.Month)
		if !reverse && i == 0 && len(manifest.Months) > 1 {
			s, err := ReadGenerationSelection(ctx, receiver, owner.GroupID, manifest.Months[1].Month)
			if err != nil || s.Request != nil {
				t.Fatal("forward case accidentally selected unrequested future evidence")
			}
		}
		for n := 0; n < 2; n++ {
			w := request(http.MethodPost, "/api/monthly-statement/generation", `{"month":"`+month.Month+`"}`, cookies)
			if w.Code != http.StatusOK && w.Code != http.StatusAccepted {
				t.Fatal("real fixed-origin customer request failed", month.Month, w.Code)
			}
		}
		deadline := time.Now().Add(45 * time.Second)
		for {
			w := request(http.MethodGet, "/api/monthly-statement/generation?month="+month.Month, "", cookies)
			if w.Code == http.StatusOK && strings.Contains(w.Body.String(), `"state":"available"`) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("real fixed-origin generation did not finish", month.Month, d.runtime.worker.Status())
			}
			time.Sleep(10 * time.Millisecond)
		}
		bill := request(http.MethodGet, "/api/monthly-statement?month="+month.Month, "", cookies)
		if bill.Code != http.StatusOK {
			t.Fatal("real fixed-origin bill read failed")
		}
		_, publication := retainedCustomerBillProjection(t, bill.Body.Bytes())
		// Identical cents alone cannot prove that a later-first request kept
		// the real origin. Inspect the immutable stored issuance marker too.
		var frozen monitor.PortalMonthlyBill
		if err := auth.First(&frozen, "group_id=? AND month=?", owner.GroupID, month.Month).Error; err != nil {
			t.Fatal(err)
		}
		marker, found := strings.CutPrefix(frozen.PublicationID, "cents-chain-v1:series-v1:")
		bound := strings.Split(marker, ":")
		if !found || len(bound) != 4 || bound[0] != manifest.From || bound[1] != manifest.SourceIdentity || !generationHex(bound[2], 32) || bound[3] == "" {
			t.Fatal("real frozen issuance marker shifted origin or source", month.Month)
		}
		if anchor != "" && anchor != bound[2] {
			t.Fatal("different monthly publications adopted different source anchors")
		}
		anchor = bound[2]
		p := profiles[month.Month]
		reference := p.reference
		if p.kind == "original_quota" {
			reference = retainedQuotaReference(t, reference, owner.GroupID, month.Month, customer.Name)
			retainedQuotaResponseMatches(t, reference, bill.Body.Bytes())
		} else {
			before, _ := retainedCustomerBillProjection(t, reference)
			now, _ := retainedCustomerBillProjection(t, bill.Body.Bytes())
			if !bytes.Equal(before, now) {
				t.Fatal("real fixed-origin original financial payload changed", month.Month)
			}
		}
		book := request(http.MethodGet, "/api/monthly-statement/download?month="+month.Month+"&publication="+publication, "", cookies)
		_, params, err := mime.ParseMediaType(book.Header().Get("Content-Disposition"))
		if book.Code != http.StatusOK || err != nil || params["filename"] != "NexusAPI-"+customer.Name+"-月度账单-"+month.Month+".xlsx" {
			t.Fatal("real fixed-origin download failed", month.Month)
		}
		retainedSeriesWorkbookMatchesSource(t, reference, bill.Body.Bytes(), book.Body.Bytes(), displayReference[month.Month])
		bodies[month.Month], books[month.Month], publications[month.Month] = bytes.Clone(bill.Body.Bytes()), bytes.Clone(book.Body.Bytes()), publication
		var count, attempts int
		if receiver.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(SUM(attempts),0) FROM portal_monthly_draft_tasks").Scan(&count, &attempts) != nil || count != i+1 || attempts != i+1 {
			t.Fatal("unrequested month or duplicate request created extra tasks", count, attempts)
		}
		var billCount int64
		if auth.Model(&monitor.PortalMonthlyBill{}).Count(&billCount).Error != nil || billCount != int64(i+1) {
			t.Fatal("preparing predecessor unexpectedly published a customer bill")
		}
		if capacity {
			selection, err := ReadGenerationSelection(ctx, receiver, owner.GroupID, month.Month)
			if err != nil || selection.Request == nil {
				t.Fatal("real archived selection missing", err)
			}
			before, err := d.g.loadRetainedGeneration(ctx, *selection.Request, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if worked, err := maintenance.step(ctx); err != nil || !worked {
				t.Fatal("real payload maintenance failed", month.Month, worked, err)
			}
			if _, err := loadGeneration(ctx, receiver, *selection.Request, time.Now()); !errors.Is(err, ErrGenerationArchived) {
				t.Fatal("real current payload not cold", err)
			}
			after, err := d.g.loadRetainedGeneration(ctx, *selection.Request, time.Now())
			if err != nil || !bytes.Equal(before.planRaw, after.planRaw) || !bytes.Equal(before.inputRaw, after.inputRaw) || !bytes.Equal(before.artifact, after.artifact) || before.digest != after.digest {
				t.Fatal("real payload bytes changed on cold transition", err)
			}
			page := request(http.MethodGet, "/api/monthly-statement?month="+month.Month, "", cookies)
			download := request(http.MethodGet, "/api/monthly-statement/download?month="+month.Month+"&publication="+publication, "", cookies)
			if page.Code != http.StatusOK || download.Code != http.StatusOK || !bytes.Equal(page.Body.Bytes(), bodies[month.Month]) || !bytes.Equal(download.Body.Bytes(), books[month.Month]) {
				t.Fatal("real customer bill changed after maintenance")
			}
			var archived int
			if err := receiver.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_generation_archives`).Scan(&archived); err != nil || archived != i+1 {
				t.Fatal("wrong real archive count", archived, err)
			}
		}
		closeDeploymentTest(t, d)
		startRuntime() // Preserve receiver, selected sources and bill storage.
		for savedMonth, saved := range bodies {
			w := request(http.MethodGet, "/api/monthly-statement?month="+savedMonth, "", cookies)
			x := request(http.MethodGet, "/api/monthly-statement/download?month="+savedMonth+"&publication="+publications[savedMonth], "", cookies)
			if w.Code != http.StatusOK || x.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), saved) || !bytes.Equal(x.Body.Bytes(), books[savedMonth]) {
				t.Fatal("real saved bill/workbook changed across intermediate runtime restart", savedMonth)
			}
		}
	}
	for i := 1; i < len(manifest.Months); i++ {
		var left, right struct {
			Data retainedSeriesView `json:"data"`
		}
		if json.Unmarshal(bodies[manifest.Months[i-1].Month], &left) != nil || json.Unmarshal(bodies[manifest.Months[i].Month], &right) != nil || left.Data.Closing != right.Data.Opening || left.Data.Display.Organization.Closing != right.Data.Display.Organization.Opening || len(left.Data.Members) != len(right.Data.Members) {
			t.Fatal("real raw or displayed organization month boundary does not join")
		}
		for j, a := range left.Data.Members {
			b := right.Data.Members[j]
			if a.UserID != b.UserID || a.Closing != b.Opening || left.Data.Display.Members[j].Closing != right.Data.Display.Members[j].Opening {
				t.Fatal("real account identity/raw/display month boundary does not join")
			}
		}
	}
	comparable, parts := map[string][]byte{}, map[string]map[string][]byte{}
	for month, raw := range bodies {
		var envelope struct {
			Data map[string]json.RawMessage `json:"data"`
		}
		if json.Unmarshal(raw, &envelope) != nil {
			t.Fatal("invalid saved bill")
		}
		delete(envelope.Data, "publication")
		delete(envelope.Data, "as_of")
		body, err := json.Marshal(envelope.Data)
		if err != nil {
			t.Fatal(err)
		}
		comparable[month], parts[month] = body, retainedWorkbookParts(t, books[month])
		acceptanceWriteNew(t, filepath.Join(dir, month+"-customer-bill.json"), raw)
		acceptanceWriteNew(t, filepath.Join(dir, month+"-customer-bill.xlsx"), books[month])
	}
	closeDeploymentTest(t, d)
	if owner != profiles[manifest.From].config.Owner {
		t.Fatal("owner changed during the isolated series")
	}
	receipt, err := json.Marshal(map[string]any{"group_id": owner.GroupID, "from": manifest.From, "through": manifest.Through,
		"request_order": retainedFixedOrderMonths(order), "fixed_origin_preserved": true, "stored_origin_and_anchor_marker_verified": true,
		"adjacent_month_boundaries_checked":  len(manifest.Months) - 1,
		"earlier_unsupported_month_rejected": true, "raw_and_display_account_organization_boundaries_join": true,
		"independent_original_money_and_workbook_verified": true, "original_account_input_bytes_unchanged": true,
		"one_task_per_requested_month_no_predecessor_bill_created": true, "intermediate_restart_bill_and_workbook_bytes_unchanged": true,
		"published_payload_capacity_mode": capacity, "cold_payload_bytes_and_customer_output_unchanged": capacity,
		"production_access": false, "production_publication": false})
	if err != nil {
		t.Fatal(err)
	}
	acceptanceWriteNew(t, filepath.Join(dir, "acceptance.json"), receipt)
	return comparable, parts
}

func retainedFixedOrderMonths(order []retainedFixedMonth) []string {
	out := make([]string, 0, len(order))
	for _, month := range order {
		out = append(out, month.Month)
	}
	return out
}
