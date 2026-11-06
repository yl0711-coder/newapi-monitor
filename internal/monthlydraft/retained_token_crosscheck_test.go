package monthlydraft

// Explicit offline real-data acceptance. No customer-specific ID, amount or
// exception enters production code. This proves consumption provenance, NOT
// wallet settlement, historical month boundaries or publication eligibility.
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

type retainedTokenCrosscheckRequest struct {
	Version      int                                  `json:"version"`
	UserID       int64                                `json:"user_id"`
	Month        string                               `json:"month"`
	Archive      monthlysource.MonthlyObservationFile `json:"archive"`
	Selection    monthlysource.MonthlyObservationFile `json:"selection"`
	Database     monthlysource.MonthlyObservationFile `json:"database"`
	OlderData    monthlysource.MonthlyObservationFile `json:"older_data"`
	OlderQueries monthlysource.MonthlyObservationFile `json:"older_queries"`
}

func TestRetainedTokenUsageCrosscheck(t *testing.T) {
	path := os.Getenv("MONTHLY_REAL_TOKEN_CROSSCHECK")
	if path == "" {
		t.Skip("explicit hash-pinned private real source selection required")
	}
	expected := os.Getenv("MONTHLY_REAL_TOKEN_CROSSCHECK_SHA256")
	raw := acceptancePrivateRead(t, path, generationPlanLimit)
	var request retainedTokenCrosscheckRequest
	if !generationHex(expected, 32) || generationHash(raw) != expected || json.Unmarshal(raw, &request) != nil {
		t.Fatal("real source selection differs from explicit digest")
	}
	canonical, err := json.Marshal(request)
	if err != nil || !bytes.Equal(bytes.TrimSpace(raw), canonical) || request.Version != 1 || request.UserID <= 0 {
		t.Fatal("invalid real source selection")
	}
	refs := []monthlysource.MonthlyObservationFile{request.Archive, request.Selection, request.Database, request.OlderData, request.OlderQueries}
	inputs := make([][]byte, len(refs))
	total := int64(0)
	for i, ref := range refs {
		actual, err := filepath.EvalSymlinks(ref.Path)
		if err != nil || !filepath.IsAbs(ref.Path) || actual != ref.Path || !generationHex(ref.SHA256, 32) {
			t.Fatal("unsafe retained reference")
		}
		inputs[i] = acceptancePrivateRead(t, ref.Path, generationEvidenceLimit-total)
		total += int64(len(inputs[i]))
		if generationHash(inputs[i]) != ref.SHA256 {
			t.Fatal("real original source digest differs")
		}
	}
	defer func() {
		for _, ref := range refs {
			if body := acceptancePrivateRead(t, ref.Path, generationEvidenceLimit); generationHash(body) != ref.SHA256 {
				t.Error("read-only crosscheck changed original source")
			}
		}
	}()
	ctx := t.Context()
	archive, tokens, err := monthlysource.DecodeTokenUsageMonthArchive(ctx, inputs[0], time.Now())
	if err != nil || archive.UserID != request.UserID || archive.Month != request.Month {
		t.Fatal("real token capture scope mismatch", err)
	}
	var selection struct {
		GroupID int64                              `json:"group_id"`
		Funding monthlysource.FundingMonthRequest  `json:"funding"`
		Usage   []monthlysource.UsageWindowVersion `json:"usage_windows"`
	}
	if json.Unmarshal(inputs[1], &selection) != nil || selection.GroupID <= 0 || len(selection.Funding.UserIDs) != 1 ||
		selection.Funding.UserIDs[0] != request.UserID || selection.Funding.Month != request.Month ||
		selection.Funding.SourceIdentity != archive.SourceIdentity || selection.Funding.Rules != archive.Rules {
		t.Fatal("independent usage selection differs from token month")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(request.Database.Path + suffix); !os.IsNotExist(err) {
			t.Fatal("usage store is not a sealed original")
		}
	}
	u := url.URL{Scheme: "file", Path: request.Database.Path}
	u.RawQuery = url.Values{"mode": {"ro"}, "immutable": {"1"}, "_pragma": {"query_only(1)"}}.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	observed, err := monthlysource.ReviewUsageMonth(queryCtx, db, selection.Funding, selection.Usage)
	usage := observed.Report()
	if err != nil || len(usage.Accounts) != 1 || !tokenSourceMatchesBaseline(archive, usage.Accounts[0], tokens) {
		t.Fatal("real token groups differ from independently captured user windows", err)
	}
	var older map[string]json.RawMessage
	var queries map[string]string
	if json.Unmarshal(inputs[3], &older) != nil || json.Unmarshal(inputs[4], &queries) != nil {
		t.Fatal("retained older query unavailable")
	}
	key := "month_" + request.Month
	wantQuery := fmt.Sprintf("SELECT type,CASE WHEN JSON_VALID(other) THEN COALESCE(JSON_UNQUOTE(JSON_EXTRACT(other,'$.billing_source')),'unspecified') ELSE 'invalid_json' END AS billing_source,COUNT(*) AS records,COALESCE(SUM(quota),0) AS quota,COALESCE(SUM(prompt_tokens),0) AS input_tokens,COALESCE(SUM(completion_tokens),0) AS output_tokens FROM logs FORCE INDEX(idx_user_created_type) WHERE user_id=%d AND created_at>=%d AND created_at<%d AND type IN (2,6) GROUP BY type,billing_source", request.UserID, tokens.From, tokens.To)
	if queries[key] != wantQuery {
		t.Fatal("older query is not the exact same account/calendar window")
	}
	var rows []struct {
		Type    int             `json:"type"`
		Source  string          `json:"billing_source"`
		Records int64           `json:"records"`
		Quota   json.RawMessage `json:"quota"`
	}
	if json.Unmarshal(older[key], &rows) != nil || rows == nil {
		t.Fatal("missing older result is not an empty month")
	}
	consume, refund := new(big.Int), new(big.Int)
	var requests int64
	for _, row := range rows {
		text := string(row.Quota)
		if len(text) > 0 && text[0] == '"' && json.Unmarshal(row.Quota, &text) != nil {
			t.Fatal("invalid exact quota string")
		}
		q, ok := new(big.Int).SetString(text, 10)
		if !ok || q.Sign() < 0 || row.Source != "wallet" || row.Records <= 0 || (row.Type != 2 && row.Type != 6) {
			t.Fatal("invalid old wallet source or exact money")
		}
		if row.Type == 2 {
			consume.Add(consume, q)
			requests += row.Records
		} else {
			refund.Add(refund, q)
		}
	}
	if consume.Cmp(big.NewInt(tokens.ConsumeQuota)) != 0 || refund.Cmp(big.NewInt(tokens.RefundQuota)) != 0 {
		t.Fatal("older source query and full token archive money differ")
	}
	var sourceRequests int64
	for _, window := range usage.Accounts[0].Windows {
		for _, row := range window.Rows {
			if row.Type == 2 {
				sourceRequests += row.Records
			}
		}
	}
	if sourceRequests != requests {
		t.Fatal("independent consumption record counts differ")
	}
	t.Logf("real consumption exact: user=%d month=%s requests=%d tokens=%d consume_quota=%d refund_quota=%d; wallet/month-boundary eligibility NOT asserted", request.UserID, request.Month, requests, len(tokens.Rows), tokens.ConsumeQuota, tokens.RefundQuota)
}
