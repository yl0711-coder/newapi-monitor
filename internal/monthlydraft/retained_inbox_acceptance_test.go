package monthlydraft

// Explicit offline acceptance only. No code in this file enters the product
// binary. The old retained normalized input is NOT relabeled as a fresh raw
// observation; the product compatibility packager must recheck and seal it.
import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"math/big"
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

func TestRetainedRealInboxCustomerJourney(t *testing.T) {
	retainedRealInboxCustomerJourney(t, false)
}

func TestRetainedRealDisplaySeriesCustomerJourney(t *testing.T) {
	retainedRealInboxCustomerJourney(t, true)
}

func retainedRealInboxCustomerJourney(t *testing.T, series bool) {
	source, output := os.Getenv("MONTHLY_REAL_INBOX_SOURCE"), os.Getenv("MONTHLY_REAL_INBOX_OUTPUT")
	if source == "" {
		t.Skip("explicit private retained source and fresh offline workspace required")
	}
	for _, path := range []string{source, output} {
		actual, err := filepath.EvalSymlinks(path)
		st, statErr := os.Lstat(path)
		if err != nil || statErr != nil || !filepath.IsAbs(path) || actual != path || !st.IsDir() || st.Mode().Perm() != 0700 {
			t.Fatal("canonical private source and new output directories required")
		}
	}
	if source == output || strings.HasPrefix(output, source+string(filepath.Separator)) || strings.HasPrefix(source, output+string(filepath.Separator)) {
		t.Fatal("independent source/output roots required")
	}
	if entries, err := os.ReadDir(output); err != nil || len(entries) != 0 {
		t.Fatal("new empty acceptance output required")
	}
	ctx := context.Background()
	originals := map[string]string{}
	readOriginal := func(path string, limit int64) []byte {
		t.Helper()
		raw := acceptancePrivateRead(t, path, limit)
		originals[path] = generationHash(raw)
		return raw
	}
	defer func() {
		for path, expected := range originals {
			if raw := acceptancePrivateRead(t, path, generationEvidenceLimit); generationHash(raw) != expected {
				t.Error("an immutable retained source was changed")
			}
		}
	}()
	var config retainedAcceptanceConfig
	if json.Unmarshal(readOriginal(filepath.Join(source, "config.json"), 4096), &config) != nil {
		t.Fatal("retained configuration unavailable")
	}
	planRaw := readOriginal(filepath.Join(source, "plan.json"), generationPlanLimit)
	inputRaw := readOriginal(filepath.Join(source, "input.json"), monitor.MonthlyPreparationInputLimit)
	var plan GenerationPlan
	if json.Unmarshal(planRaw, &plan) != nil {
		t.Fatal("retained plan unavailable")
	}
	_, canonical, input, err := canonicalGeneration(plan, inputRaw, time.Now())
	if err != nil || !bytes.Equal(canonical, planRaw) || plan.Owner != config.Owner || plan.Funding.Month != config.Month || plan.AutomaticScopeSourceID != config.ScopeSourceID {
		t.Fatal("retained scope/plan differs", err)
	}
	// Preserve ALL financial input fields and original evidence bytes. Only
	// copied source paths and their resulting plan/input hashes may differ.
	accountsBefore, err := json.Marshal(input.Accounts)
	if err != nil {
		t.Fatal(err)
	}
	evidence := filepath.Join(output, "evidence")
	prepared := filepath.Join(evidence, "prepared")
	if err := os.Mkdir(evidence, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(prepared, 0700); err != nil {
		t.Fatal(err)
	}
	var scopeRaw, fundingRaw, directoryRaw []byte
	fundingPath := filepath.Join(evidence, "funding.sqlite")
	for i, original := range input.Sources {
		rel, err := filepath.Rel(filepath.Join(source, "evidence"), original.Path)
		if err != nil || !filepath.IsAbs(original.Path) || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || !validScopeSourceID(original.ID) {
			t.Fatal("retained source outside explicit evidence scope")
		}
		raw := readOriginal(original.Path, generationEvidenceLimit)
		if generationHash(raw) != original.SHA256 {
			t.Fatal("retained original hash differs")
		}
		path := filepath.Join(evidence, original.ID+".source")
		switch {
		case original.ID == config.ScopeSourceID:
			path, scopeRaw = filepath.Join(evidence, "retained-scope.json"), raw
		case filepath.Base(original.Path) == "funding.sqlite":
			path, fundingRaw = fundingPath, raw
		case original.ID == "current_directory":
			directoryRaw = raw
		}
		acceptanceWriteNew(t, path, raw)
		input.Sources[i].Path = path
	}
	if len(scopeRaw) == 0 || len(fundingRaw) == 0 || len(directoryRaw) == 0 {
		t.Fatal("independent scope, funding and customer directory required")
	}
	accountsAfter, err := json.Marshal(input.Accounts)
	if err != nil || !bytes.Equal(accountsBefore, accountsAfter) {
		t.Fatal("acceptance relocation changed financial input")
	}
	inputRaw, err = json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	plan.PreparationSHA256 = generationHash(inputRaw)
	plan, planRaw, _, err = canonicalGeneration(plan, inputRaw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var directory struct {
		Groups []struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		} `json:"groups"`
		Users []monitor.TrackedUser `json:"users"`
	}
	if json.Unmarshal(directoryRaw, &directory) != nil {
		t.Fatal("retained directory malformed")
	}
	if series && len(directory.Groups) == 0 {
		// Some retained CURRENT directories contain users only. Read the label
		// from the separately pinned scope, never invent users or money. Live
		// IDs/manifest must still match the fresh authorization below.
		var scope RetainedAutomaticScope
		if json.Unmarshal(scopeRaw, &scope) != nil || scope.Owner != plan.Owner || scope.Scope.Group.GroupID != plan.Owner.GroupID {
			t.Fatal("retained organization label scope differs")
		}
		directory.Groups = append(directory.Groups, struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		}{ID: scope.Owner.GroupID, Name: scope.Scope.Group.Name})
	}
	if len(directory.Groups) != 1 || directory.Groups[0].ID != plan.Owner.GroupID || len(directory.Users) != len(plan.Funding.UserIDs) {
		t.Fatal("retained directory scope differs")
	}
	storePath := filepath.Join(output, "usage.db")
	m, err := monitor.New(monitor.Settings{StorePath: storePath, LocalSnapshotOnly: true, AlertsDisabled: true,
		PortalMonthlyStatementEnabled: true, SessionSecret: retainedAcceptanceSecret(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	auth, _ := acceptanceAuth(t, storePath)
	const password = "SyntheticOfflineAcceptance!2026"
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	customer := monitor.CustomerGroup{ID: plan.Owner.GroupID, Name: directory.Groups[0].Name,
		PortalEmail: "retained-offline@example.invalid", PortalPwAdmin: string(hash), PortalAuthVer: plan.Owner.AuthVersion}
	if auth.Create(&customer).Error != nil || auth.Create(&directory.Users).Error != nil {
		t.Fatal("fresh local authorization scope creation failed")
	}
	owner, _, err := LoadUsageGenerationOwner(ctx, auth, customer.ID, customer.PortalAuthVer)
	if err != nil || owner != plan.Owner {
		t.Fatal("new isolated authority differs from the retained scope", err)
	}
	receiverPath := filepath.Join(output, "receiver.sqlite")
	receiver := generationDB(t, receiverPath, false)
	if InitGenerationReceiver(ctx, receiver) != nil || InitGenerationSelections(ctx, receiver) != nil {
		t.Fatal("fresh isolated receiver initialization failed")
	}
	if err := os.Chmod(receiverPath, 0600); err != nil {
		t.Fatal(err)
	}
	cfg := DeploymentConfig{Rollout: GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: owner.GroupID, Month: config.Month}}},
		ReceiverPath: receiverPath, FundingPath: fundingPath, AuthorityPath: storePath, EvidenceRoot: evidence}
	checked, err := PrepareGenerationInput(ctx, cfg, planRaw, inputRaw, "", false)
	if err != nil || checked.Applied {
		t.Fatal("retained real normalized input failed full financial preflight", err)
	}
	// No test-authored completion marker: use the product's explicit legacy
	// compatibility preparation, then the same runtime inbox and customer API.
	pack, err := PrepareRetainedGenerationFiles(ctx, cfg, RetainedPreparationRequest{
		Plan: planRaw, Preparation: inputRaw, OutputRoot: prepared, Write: true})
	if err != nil || !pack.Complete {
		t.Fatal("product retained preparation failed", err)
	}
	entry := PreparedInputInboxEntry{GroupID: owner.GroupID, Month: config.Month, Revision: 1,
		Directory: "prepared", ManifestSHA256: pack.ManifestSHA256}
	cfg.PreparationInbox = "inbox.json"
	writePreparationInboxTest(t, evidence, cfg.PreparationInbox, entry)
	if series {
		// This single retained month is a REAL TEST WINDOW, not an approval
		// of a production origin or a claim about missing earlier months.
		ranges, err := json.Marshal(AutomaticDisplaySeriesIndex{Version: 1, Ranges: []AutomaticDisplaySeriesRange{{GroupID: owner.GroupID, SourceIdentity: config.SourceIdentity, From: config.Month, Through: config.Month}}})
		if err != nil {
			t.Fatal(err)
		}
		acceptanceWriteNew(t, filepath.Join(evidence, "display-series.json"), ranges)
	}
	open := func() *Deployment {
		t.Helper()
		var d *Deployment
		var err error
		if series {
			d, err = OpenRetainedAutomaticDisplaySeriesDeployment(ctx, cfg, config.ScopeSourceID, "display-series.json", func(ctx context.Context, req monthlytasks.Request, body []byte, p *monitor.PreparedMonthlyDisplayPublication) error {
				_, err := m.PublishMonthlyBillWithDisplaySeries(ctx, body, monitor.MonthlyBillExpectedAuthorization{GroupID: req.Owner.GroupID, AuthVersion: req.Owner.AuthVersion, MemberFingerprint: req.Owner.MemberFingerprint, MemberManifest: req.Owner.MemberManifest}, p)
				return err
			})
		} else {
			d, err = OpenRetainedAutomaticDeployment(ctx, cfg, config.ScopeSourceID, func(ctx context.Context, req monthlytasks.Request, body []byte) error {
				_, err := m.PublishMonthlyBillForAuthorization(ctx, body, monitor.MonthlyBillExpectedAuthorization{
					GroupID: req.Owner.GroupID, AuthVersion: req.Owner.AuthVersion, MemberFingerprint: req.Owner.MemberFingerprint, MemberManifest: req.Owner.MemberManifest})
				return err
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { closeDeploymentTest(t, d) })
		return d
	}
	d := open()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(gin.Recovery())
	if err := r.SetTrustedProxies(nil); err != nil {
		t.Fatal(err)
	}
	if err := m.RegisterPortalRoutesWithGeneration(r, d); err != nil {
		t.Fatal(err)
	}
	request := func(method, path, body string, cookies []*http.Cookie) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, "http://127.0.0.1"+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "http://127.0.0.1")
		req.Header.Set("X-Usage-Bill-Request", "1")
		req.Header.Set("X-Monthly-Statistics", "nullable-v1")
		req.Header.Set("X-Monthly-Accounting", "balance-correction-v1")
		req.Header.Set("X-Monthly-Display", "cents-chain-v1")
		for _, cookie := range cookies {
			req.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	login := request(http.MethodPost, "/login", `{"email":"retained-offline@example.invalid","password":"`+password+`"}`, nil)
	if login.Code != http.StatusOK || len(login.Result().Cookies()) == 0 {
		t.Fatal("isolated customer login failed", login.Code)
	}
	cookies := login.Result().Cookies()
	statusPath := "/api/monthly-statement/generation?month=" + config.Month
	if w := request(http.MethodGet, statusPath, "", cookies); w.Code != http.StatusServiceUnavailable {
		t.Fatal("not-started generator did not enforce temporary unavailability", w.Code)
	}
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		selected, err := ReadGenerationSelection(ctx, receiver, owner.GroupID, config.Month)
		if err != nil {
			t.Fatal(err)
		}
		if selected.Request != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("real retained input was not selected by the running inbox")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, table := range []string{"portal_monthly_draft_tasks", "portal_monthly_bills"} {
		var count int64
		db := receiver
		if table == "portal_monthly_bills" {
			if err := auth.Model(&monitor.PortalMonthlyBill{}).Count(&count).Error; err != nil || count != 0 {
				t.Fatal("intake pre-published a customer bill", err)
			}
			continue
		}
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
			t.Fatal("intake created a customer task", err)
		}
	}
	if w := request(http.MethodGet, statusPath, "", cookies); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"state":"not_requested"`) {
		t.Fatal("real allowed month was not made requestable", w.Code)
	}
	for i := 0; i < 2; i++ {
		w := request(http.MethodPost, "/api/monthly-statement/generation", `{"month":"`+config.Month+`"}`, cookies)
		if w.Code != http.StatusAccepted && w.Code != http.StatusOK {
			t.Fatal("real customer generation request failed", w.Code)
		}
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		w := request(http.MethodGet, statusPath, "", cookies)
		if w.Code == http.StatusOK && strings.Contains(w.Body.String(), `"state":"available"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("real customer generation failed to publish", d.runtime.worker.Status())
		}
		time.Sleep(10 * time.Millisecond)
	}
	w := request(http.MethodGet, "/api/monthly-statement?month="+config.Month, "", cookies)
	if w.Code != http.StatusOK {
		t.Fatal("generated real bill could not be viewed", w.Code)
	}
	var baseline []byte
	_, publication := retainedCustomerBillProjection(t, w.Body.Bytes())
	expectedQuota := os.Getenv("MONTHLY_REAL_INBOX_QUOTA_EXPECTATION_SHA256")
	if expectedQuota != "" {
		if !series || !generationHex(expectedQuota, 32) {
			t.Fatal("explicit historical quota expectation requires series acceptance")
		}
		raw := readOriginal(filepath.Join(source, "independent-expected-quota.json"), 262144)
		if generationHash(raw) != expectedQuota {
			t.Fatal("independent original quota expectation changed")
		}
		baseline = retainedQuotaReference(t, raw, owner.GroupID, config.Month, customer.Name)
		retainedQuotaResponseMatches(t, baseline, w.Body.Bytes())
	} else {
		baseline = readOriginal(filepath.Join(source, "actual-customer-bill.json"), 2<<20)
		projection, _ := retainedCustomerBillProjection(t, w.Body.Bytes())
		before, _ := retainedCustomerBillProjection(t, baseline)
		if !bytes.Equal(projection, before) {
			t.Fatal("real customer bill changed beyond issuance metadata")
		}
	}
	acceptanceWriteNew(t, filepath.Join(output, "customer-bill.json"), w.Body.Bytes())
	xlsx := request(http.MethodGet, "/api/monthly-statement/download?month="+config.Month+"&publication="+publication, "", cookies)
	if xlsx.Code != http.StatusOK || !bytes.HasPrefix(xlsx.Body.Bytes(), []byte("PK")) {
		t.Fatal("real bill download failed", xlsx.Code)
	}
	_, params, err := mime.ParseMediaType(xlsx.Header().Get("Content-Disposition"))
	if err != nil || params["filename"] != "NexusAPI-"+customer.Name+"-月度账单-"+config.Month+".xlsx" {
		t.Fatal("real customer download filename differs", err)
	}
	if series {
		retainedSeriesWorkbookMatchesSource(t, baseline, w.Body.Bytes(), xlsx.Body.Bytes())
	} else {
		originalWorkbook := readOriginal(filepath.Join(source, "actual-customer-bill.xlsx"), 8<<20)
		originalParts, currentParts := retainedWorkbookParts(t, originalWorkbook), retainedWorkbookParts(t, xlsx.Body.Bytes())
		for _, name := range []string{"xl/worksheets/sheet2.xml", "xl/worksheets/sheet3.xml"} {
			retainedWorkbookCentDetails(t, name, originalParts[name], currentParts[name])
		}
	}
	acceptanceWriteNew(t, filepath.Join(output, "customer-bill.xlsx"), xlsx.Body.Bytes())
	var count, attempts int
	if err := receiver.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(SUM(attempts),0) FROM portal_monthly_draft_tasks").Scan(&count, &attempts); err != nil || count != 1 || attempts != 1 {
		t.Fatal("customer repeated requests duplicated billing work", count, attempts, err)
	}
	closeDeploymentTest(t, d)
	// Reopening the SAME receiver must reverify the exact selected request,
	// not reselect/queue/reissue any customer bill. Disable the polling loop
	// for this explicit recheck so two admissions cannot race in this test.
	cfg.PreparationInbox = ""
	d = open()
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	result, err := d.receivePreparedInput(ctx, entry, config.ScopeSourceID)
	if err != nil || !result.Applied || result.Changed {
		t.Fatal("restart did not recover idempotently", err)
	}
	closeDeploymentTest(t, d)
	if again := request(http.MethodGet, "/api/monthly-statement?month="+config.Month, "", cookies); again.Code != http.StatusOK || !bytes.Equal(again.Body.Bytes(), w.Body.Bytes()) {
		t.Fatal("saved real bill depended on running generation resources")
	}
	if again := request(http.MethodGet, "/api/monthly-statement/download?month="+config.Month+"&publication="+publication, "", cookies); again.Code != http.StatusOK || !bytes.Equal(again.Body.Bytes(), xlsx.Body.Bytes()) {
		t.Fatal("saved real workbook was unstable after restart/drain")
	}
	receipt := map[string]any{"mode": "offline_retained_real_inbox_acceptance", "month": config.Month,
		"fixed_origin_service_mode": series, "test_window_only_not_production_origin_approval": series,
		"accounts_input_bytes_unchanged":                                 true,
		"independent_original_quota_expectation_used":                    expectedQuota != "",
		"account_and_token_identity_and_independent_cent_money_verified": true, "task_count": count, "task_attempts": attempts,
		"customer_bill_sha256": generationHash(w.Body.Bytes()), "workbook_sha256": generationHash(xlsx.Body.Bytes())}
	if expectedQuota == "" {
		receipt["financial_payload_matches_original"] = true
		receipt["reference_kind"] = "previously_saved_customer_bill"
	} else {
		receipt["financial_payload_matches_independent_original_quota"] = true
		receipt["reference_kind"] = "independent_original_quota_not_previously_issued_bill"
	}
	report, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	acceptanceWriteNew(t, filepath.Join(output, "acceptance.json"), report)
	t.Log("offline retained real intake, customer generation/read/download and restart completed; source files unchanged")
}

func retainedCustomerBillProjection(t *testing.T, raw []byte) ([]byte, string) {
	t.Helper()
	var envelope struct {
		OK   bool                       `json:"ok"`
		Data map[string]json.RawMessage `json:"data"`
	}
	if json.Unmarshal(raw, &envelope) != nil || !envelope.OK || len(envelope.Data) == 0 {
		t.Fatal("customer bill response missing")
	}
	var publication string
	if json.Unmarshal(envelope.Data["publication"], &publication) != nil || len(publication) != 64 {
		t.Fatal("saved bill publication unavailable")
	}
	delete(envelope.Data, "publication")
	delete(envelope.Data, "as_of")            // issuance time, NEVER a financial amount
	delete(envelope.Data, "display_balances") // independently checked cent presentation, not original financial facts
	projection, err := json.Marshal(envelope.Data)
	if err != nil {
		t.Fatal(err)
	}
	return projection, publication
}

// Retained workbooks predate the cents display contract. Compare every actual
// account/token identity and amount using an independent rational oracle,
// rather than requiring the deliberately replaced six-decimal XML bytes.
// Frozen-byte stability across restart is still checked by the journey above.
func retainedWorkbookCentDetails(t *testing.T, name string, original, current []byte) {
	t.Helper()
	member := name == "xl/worksheets/sheet2.xml"
	parse := func(raw []byte) map[string][]string {
		t.Helper()
		var sheet struct {
			Rows []struct {
				Number int `xml:"r,attr"`
				Cells  []struct {
					Value string `xml:"v"`
					Text  string `xml:"is>t"`
				} `xml:"c"`
			} `xml:"sheetData>row"`
		}
		if len(raw) == 0 || xml.Unmarshal(raw, &sheet) != nil {
			t.Fatal("retained account/token sheet missing", name)
		}
		rows := map[string][]string{}
		for _, row := range sheet.Rows {
			if row.Number < 4 {
				continue
			}
			if len(row.Cells) < 7 || len(row.Cells) > 8 {
				t.Fatal("unexpected account/token column shape", name)
			}
			start, key := 1, row.Cells[0].Text
			if member {
				if key == "分项舍入尾差" || key == "组织合计" {
					continue // independent six-organization footer audit covers these non-account rows
				}
				if !strings.Contains(key, "\nID ") {
					t.Fatal("missing immutable account identity")
				}
			} else {
				start = 4
				if row.Cells[3].Text == "—" && (row.Cells[2].Text == "分项舍入尾差" || row.Cells[2].Text == "组织汇总舍入尾差") {
					continue // not a token; never merge a residual with a real ID
				}
				for _, cell := range row.Cells[1:4] {
					key += "\x00" + cell.Text
				}
			}
			if _, duplicate := rows[key]; duplicate {
				t.Fatal("duplicate exported account/token identity")
			}
			for _, cell := range row.Cells[start:] {
				if _, ok := new(big.Rat).SetString(cell.Value); !ok {
					t.Fatal("exported money must be numeric", name)
				}
				rows[key] = append(rows[key], cell.Value)
			}
		}
		return rows
	}
	before, after := parse(original), parse(current)
	if len(before) == 0 || len(before) != len(after) {
		t.Fatal("account/token row count changed", name)
	}
	for key, source := range before {
		got, exists := after[key]
		if !exists || (member && len(got) != len(source)) || (!member && len(got) != 3 && len(got) != 4) {
			t.Fatal("account/token identity or columns changed", name)
		}
		want := make([]string, len(got))
		for i := range want {
			if i < len(source) {
				want[i] = customerMoneyCents(t, source[i])
			}
		}
		derived := new(big.Rat)
		if member {
			for i, value := range want[:len(want)-1] {
				v, _ := new(big.Rat).SetString(value)
				if i == 3 {
					derived.Sub(derived, v)
				} else {
					derived.Add(derived, v)
				}
			}
			want[len(want)-1] = derived.FloatString(2)
		} else if len(want) == 4 {
			for i, sign := range []int64{-1, 1, 1} {
				v, _ := new(big.Rat).SetString(want[i])
				derived.Add(derived, v.Mul(v, big.NewRat(sign, 1)))
			}
			want[3] = derived.FloatString(2)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatal("independent retained workbook money mismatch", name, i, got[i], want[i])
			}
		}
	}
}

func retainedWorkbookParts(t *testing.T, raw []byte) map[string][]byte {
	t.Helper()
	r, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil || len(r.File) > 64 {
		t.Fatal("invalid customer workbook")
	}
	parts := make(map[string][]byte)
	for _, f := range r.File {
		if f.UncompressedSize64 > 2<<20 {
			t.Fatal("unexpected workbook part size")
		}
		stream, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(io.LimitReader(stream, (2<<20)+1))
		closeErr := stream.Close()
		if readErr != nil || closeErr != nil || len(data) > 2<<20 {
			t.Fatal("workbook part could not be checked")
		}
		parts[f.Name] = data
	}
	return parts
}
