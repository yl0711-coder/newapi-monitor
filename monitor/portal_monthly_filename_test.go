package monitor

import (
	"encoding/json"
	"mime"
	"net/http"
	"os"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestMonthlyDownloadFilenameSharedCases(t *testing.T) {
	raw, err := os.ReadFile("testdata/monthly-bill-filenames.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Account string  `json:"account"`
		Month   string  `json:"month"`
		Want    *string `json:"want"`
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		want := ""
		if tc.Want != nil {
			want = *tc.Want
		}
		got := monthlyBillFilename(tc.Account, tc.Month)
		if got != want {
			t.Fatal("filename differs from shared browser/server case", tc, got)
		}
		if got == "" {
			continue
		}
		header := mime.FormatMediaType("attachment", map[string]string{"filename": got})
		kind, params, err := mime.ParseMediaType(header)
		if err != nil || kind != "attachment" || params["filename"] != got || strings.ContainsAny(header, "\r\n") {
			t.Fatal("unsafe or lossy UTF-8 download header", header, err)
		}
	}
	got := monthlyBillFilename(strings.Repeat("😀", 100), "2026-09")
	if got != "NexusAPI-"+strings.Repeat("😀", 48)+"-月度账单-2026-09.xlsx" || !utf8.ValidString(got) || len(got) > 255 {
		t.Fatal("long Unicode filename was split or exceeds filesystem component budget")
	}
}

func TestMonthlyDownloadFilenameUsesFrozenOrganization(t *testing.T) {
	f := newMonthlyRecoveryFixture(t)
	t.Cleanup(f.m.Close)
	if err := f.m.storeDB.Model(&CustomerGroup{}).Where("id = ?", f.g.ID).Update("name", "renamed synthetic organization").Error; err != nil {
		t.Fatal(err)
	}
	w := portalDo(f.r, http.MethodGet, "/api/monthly-statement/download?month="+f.e.Month+"&publication="+f.version, "", f.cookie)
	kind, params, err := mime.ParseMediaType(w.Header().Get("Content-Disposition"))
	if w.Code != 200 || err != nil || kind != "attachment" || params["filename"] != monthlyBillFilename(f.g.Name, f.e.Month) {
		t.Fatal("download lost frozen organization identity", w.Code, params, err)
	}
	var saved PortalMonthlyBill
	if err := f.m.storeDB.First(&saved, "group_id = ? AND month = ?", f.g.ID, f.e.Month).Error; err != nil || saved != f.old {
		t.Fatal("file naming rewrote the frozen bill", err)
	}
}
