package monitor

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type monthlyBatchMember struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
}

type monthlyBatchEntry struct {
	Account  string                   `json:"account"`
	Login    string                   `json:"login"`
	Month    string                   `json:"month"`
	Issues   []string                 `json:"issues"`
	Evidence json.RawMessage          `json:"evidence"`
	Members  []monthlyBatchMember     `json:"members"`
	Expected []monthlyMemberStatement `json:"expected"`
}

// Test-only adapter. Production import resolves names from its authorized
// member snapshot; the direct local exporter must bind both views identically.
func monthlyBatchBindNames(p *monthlyStatement, members []monthlyBatchMember) error {
	names := make(map[int64]string, len(members))
	for _, member := range members {
		if member.ID <= 0 || names[member.ID] != "" || strings.TrimSpace(member.Username) == "" {
			return errors.New("invalid local member directory")
		}
		names[member.ID] = member.Username
	}
	if len(p.Members) != len(names) {
		return errors.New("local member directory does not cover statement")
	}
	for i := range p.Members {
		name, ok := names[p.Members[i].UserID]
		if !ok {
			return errors.New("missing statement account name")
		}
		p.Members[i].Username = name
	}
	for i := range p.Credits.Rows {
		name, ok := names[p.Credits.Rows[i].UserID]
		if !ok {
			return errors.New("missing funding account name")
		}
		p.Credits.Rows[i].Username = name
	}
	if !monthlyStatementAmountsComplete(*p) {
		return errors.New("local statement failed shared amount validation")
	}
	return nil
}

func monthlyBatchWrite(path string, raw []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(raw)
	return errors.Join(err, f.Close())
}

// Local export only: no database, login, collection, publication or network.
func TestMonthlyReviewedBatchLocalExport(t *testing.T) {
	input, out := os.Getenv("MONTHLY_BATCH_INPUT"), os.Getenv("MONTHLY_BATCH_OUTPUT")
	if input == "" || out == "" {
		t.Skip("explicit local batch paths required")
	}
	b, err := os.ReadFile(input)
	if err != nil {
		t.Fatal(err)
	}
	var entries []monthlyBatchEntry
	if err := json.Unmarshal(b, &entries); err != nil {
		t.Fatal(err)
	}
	var exported []monthlyStatement
	for _, entry := range entries {
		if len(entry.Issues) != 0 {
			t.Logf("NOT EXPORTED %s %s: %v", entry.Account, entry.Month, entry.Issues)
			continue
		}
		e, err := decodeMonthlyBillEvidence(entry.Evidence)
		if err != nil {
			t.Fatal(err)
		}
		p, _, err := buildPublishedMonthlyBill(e, time.Now())
		if err != nil {
			t.Fatalf("%s %s: %v", entry.Account, entry.Month, err)
		}
		p.Account = entry.Account
		if err := monthlyBatchBindNames(&p, entry.Members); err != nil {
			t.Fatal(err)
		}
		// No local-only accounting adjustment: export exactly the shared DTO.
		name := strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(entry.Account)
		if name == "" || name == "." || name == ".." {
			t.Fatal("invalid filename")
		}
		dir := filepath.Join(out, name)
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		xlsx, err := monthlyStatementXLSX(p)
		if err != nil {
			t.Fatal(err)
		}
		if err := monthlyBatchWrite(filepath.Join(dir, name+"-"+entry.Month+"-月度账单.xlsx"), xlsx); err != nil {
			t.Fatal(err)
		}
		exported = append(exported, p)
		t.Logf("EXPORTED %s %s requests=%d consume=%s closing=%s", p.Account, p.Month, p.Consumption.Requests, p.Consumption.Amount, *p.MonthEndBalance)
	}
	b, err = json.MarshalIndent(exported, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := monthlyBatchWrite(filepath.Join(out, "exported-statements.json"), b); err != nil {
		t.Fatal(err)
	}
}

func TestMonthlyBatchBindingAndArtifactProtection(t *testing.T) {
	now := time.Now()
	start, _, _ := monthlyWindow("", now)
	e := monthlyEvidenceFixture(1, start.Format("2006-01"), now)
	for name, members := range map[string][]monthlyBatchMember{
		"valid": {{43, "local user"}}, "missing": {}, "wrong ID": {{44, "other"}},
		"duplicate": {{43, "one"}, {43, "two"}}, "blank": {{43, " "}},
	} {
		t.Run(name, func(t *testing.T) {
			p, _, err := buildPublishedMonthlyBill(e, now)
			if err != nil {
				t.Fatal(err)
			}
			err = monthlyBatchBindNames(&p, members)
			if (err == nil) != (name == "valid") {
				t.Fatal("directory validation", err)
			}
			if err == nil {
				if p.Members[0].Username != "local user" || p.Credits.Rows[0].Username != "local user" {
					t.Fatal("member and funding names differ")
				}
				if _, err := monthlyStatementXLSX(p); err != nil {
					t.Fatal("direct local export failed", err)
				}
			}
		})
	}
	path := filepath.Join(t.TempDir(), "original.json")
	if err := monthlyBatchWrite(path, []byte("original")); err != nil {
		t.Fatal(err)
	}
	if err := monthlyBatchWrite(path, []byte("replacement")); !os.IsExist(err) {
		t.Fatal("existing artifact could be overwritten", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil || string(raw) != "original" {
		t.Fatal("original artifact changed", err)
	}
}
