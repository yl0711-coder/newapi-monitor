package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

// Opt-in retained-real-data acceptance. This test never reads credentials,
// connects to a source, writes the retained captures or publishes a bill.
// Customer data stays outside the repository; ordinary CI skips this test.
func TestObservationRetainedPilotReadOnly(t *testing.T) {
	root := os.Getenv("MONTHLY_CAPTURE_RETAINED_PILOT")
	archive := os.Getenv("MONTHLY_CAPTURE_RETAINED_FUNDING")
	if root == "" || archive == "" {
		t.Skip("explicit private retained pilot and original funding archive required")
	}
	if _, err := privateInfo(root, true); err != nil {
		t.Fatal("private pilot root required", err)
	}
	before := map[string]string{}
	read := func(path string, max int64) []byte {
		t.Helper()
		st, err := os.Lstat(path)
		if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 || st.Size() <= 0 || st.Size() > max {
			t.Fatal("invalid retained file", filepath.Base(path))
		}
		raw, err := os.ReadFile(path)
		if err != nil || int64(len(raw)) != st.Size() {
			t.Fatal("read retained file", err)
		}
		before[path] = hash(raw)
		return raw
	}
	load := func(name string) (observationRequest, []byte) {
		t.Helper()
		dir := filepath.Join(root, name)
		var req observationRequest
		raw := read(filepath.Join(dir, "request.json"), 4096)
		if json.Unmarshal(raw, &req) != nil || req.validate(time.Now()) != nil {
			t.Fatal("invalid retained request")
		}
		payload := read(filepath.Join(dir, "observation.json"), 65536)
		var marker observationReceipt
		if json.Unmarshal(read(filepath.Join(dir, "manifest.json"), 4096), &marker) != nil ||
			!marker.Complete || marker.Directory != dir || marker.Mode != "retained_private_observation_not_bill_approval" ||
			marker.RequestSHA256 != hash(raw) || marker.ObservationSHA256 != hash(payload) || validateObservation(payload, req) != nil {
			t.Fatal("retained observation seal or payload mismatch")
		}
		observationRoundTripStore(t, req.Kind, payload)
		return req, payload
	}
	aReq, aRaw := load("sep-28-usage-a")
	bReq, bRaw := load("sep-28-usage-c")
	earlyReq, earlyRaw := load("sep-28-usage-b")
	emptyReq, emptyRaw := load("sep-23-usage-a")
	walletReq, walletRaw := load("wallet-a")
	aReqRaw, _ := json.Marshal(aReq)
	bReqRaw, _ := json.Marshal(bReq)
	earlyReqRaw, _ := json.Marshal(earlyReq)
	if !bytes.Equal(aReqRaw, bReqRaw) || len(aReq.UserIDs) != 1 || aReq.UserIDs[0] != 148 ||
		emptyReq.SourceIdentity != aReq.SourceIdentity || walletReq.SourceIdentity != aReq.SourceIdentity ||
		!slices.Equal(emptyReq.UserIDs, aReq.UserIDs) || !slices.Equal(walletReq.UserIDs, aReq.UserIDs) || !bytes.Equal(earlyReqRaw, aReqRaw) {
		t.Fatal("pilot source or account scope differs")
	}
	var a, b, early, empty monthlysource.UsageWindowObservation
	var wallet monthlysource.WalletObservation
	if json.Unmarshal(aRaw, &a) != nil || json.Unmarshal(bRaw, &b) != nil || json.Unmarshal(earlyRaw, &early) != nil || json.Unmarshal(emptyRaw, &empty) != nil || json.Unmarshal(walletRaw, &wallet) != nil {
		t.Fatal("typed retained observations")
	}
	if !slices.Equal(a.Rows, b.Rows) || a.CompletedAtMS > b.StartedAtMS || len(a.Rows) != 1 ||
		a.Rows[0].Type != 2 || a.Rows[0].BillingSource != "wallet" || a.Rows[0].Records != 130 || a.Rows[0].Quota != 2768698 || len(empty.Rows) != 0 {
		t.Fatal("closed historical usage changed or empty capture invented rows")
	}
	ctx := context.Background()
	receiverPath := filepath.Join(t.TempDir(), "pilot.sqlite")
	if err := exclusiveFile(receiverPath, nil); err != nil {
		t.Fatal(err)
	}
	receiver, err := openReceiver(ctx, receiverPath, true)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	if err := monthlysource.InitUsageWindowStore(ctx, receiver); err != nil {
		t.Fatal(err)
	}
	aID, err := monthlysource.SaveUsageWindowObservation(ctx, receiver, a)
	if err != nil {
		t.Fatal(err)
	}
	bID, err := monthlysource.SaveUsageWindowObservation(ctx, receiver, b)
	if err != nil {
		t.Fatal(err)
	}
	request := monthlysource.FundingMonthRequest{Month: "2026-09", SourceIdentity: aReq.SourceIdentity, Rules: a.Request.Rules, UserIDs: aReq.UserIDs}
	earlyID, err := monthlysource.SaveUsageWindowObservation(ctx, receiver, early)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := monthlysource.ReviewUsageMonth(ctx, receiver, request, []monthlysource.UsageWindowVersion{{OlderID: aID, NewerID: earlyID}}); err == nil {
		t.Fatal("premature repeat capture accepted despite five-minute requirement")
	}
	usage, err := monthlysource.ReviewUsageMonth(ctx, receiver, request, []monthlysource.UsageWindowVersion{{OlderID: aID, NewerID: bID}})
	if err != nil {
		t.Fatal(err)
	}
	account := usage.Report().Accounts[0]
	if account.MatchingSeconds != 86400 || account.Totals != nil || len(account.MissingWindows) == 0 {
		t.Fatal("one real day misrepresented as complete month")
	}

	fundingRoot := filepath.Join(root, "sep-23-funding-hour")
	mRaw := read(filepath.Join(fundingRoot, "manifest.json"), 4096)
	m, _, err := readManifest(fundingRoot, hash(mRaw))
	if err != nil || m.Older.SourceIdentity != aReq.SourceIdentity || !slices.Equal(m.Older.UserIDs, aReq.UserIDs) {
		t.Fatal("funding source or scope", err)
	}
	fundingPath := filepath.Join(fundingRoot, "funding.sqlite")
	read(fundingPath, 2<<20)
	funding, err := openReceiver(ctx, fundingPath, false)
	if err != nil {
		t.Fatal(err)
	}
	defer funding.Close()
	window, err := monthlysource.ReviewFundingWindow(ctx, funding, m.Older.ID, m.Newer.ID)
	if err != nil || !window.Comparison().ObservationsMatch {
		t.Fatal("independent funding observations differ", err)
	}
	page, err := monthlysource.LoadLatestFundingPage(ctx, funding, m.Newer.ID)
	if err != nil || !page.EndReached || len(page.Rows) != 3 {
		t.Fatal("funding traversal incomplete", err)
	}
	archivedRaw := read(archive, 1<<20)
	// Digest independently retained by the earlier source inventory (s006).
	if hash(archivedRaw) != "de27c057994b1a379f091dd59f05a5ad849580ffe9bea232acc7423e01e70525" {
		t.Fatal("original funding archive differs")
	}
	var archived map[string]json.RawMessage
	if json.Unmarshal(archivedRaw, &archived) != nil {
		t.Fatal("original funding archive")
	}
	matches := 0
	for _, row := range page.Rows {
		normalized := monthlysource.NormalizeFundingLog(row, m.Newer.Rules)
		if normalized.Status != monthlysource.FundingMovement || normalized.TargetUserID != 148 {
			continue // Never attribute another account's management event to this pilot.
		}
		if row.ID != 3715551 || normalized.DeltaQuota == nil || *normalized.DeltaQuota != 500000000 {
			t.Fatal("unexpected pilot funding amount")
		}
		for _, raw := range archived {
			var rows []monthlysource.FundingLog
			if json.Unmarshal(raw, &rows) != nil {
				continue
			}
			for _, old := range rows {
				if old.ID == row.ID {
					oldRaw, _ := json.Marshal(old)
					newRaw, _ := json.Marshal(row)
					if !bytes.Equal(oldRaw, newRaw) {
						t.Fatal("original funding event changed")
					}
					matches++
				}
			}
		}
	}
	if matches != 1 {
		t.Fatal("funding missing, duplicated or not independently matched")
	}
	partial, err := monthlysource.ReviewFundingMoney(ctx, request, []monthlysource.FundingWindowObservation{window})
	if err != nil || len(partial.Accounts) != 1 || partial.Accounts[0].Totals != nil {
		t.Fatal("single funding hour exposed as whole-month totals", err)
	}
	for path, digest := range before {
		raw, err := os.ReadFile(path)
		if err != nil || hash(raw) != digest {
			t.Fatal("retained original changed during local audit", filepath.Base(path))
		}
	}
	t.Log("retained real pilot: 130 consumption logs = 2,768,698 quota; independent funding event = 500,000,000 quota; repeat captures match; original bytes unchanged; no month-completeness claim or bill publication")
}
