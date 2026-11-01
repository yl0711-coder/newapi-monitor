package monthlysource

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestOrganizationMonthScopeCanonicalAndNotHistory(t *testing.T) {
	sum := sha256.Sum256([]byte("201,202,"))
	s := OrganizationMonthScope{Version: 1, Mode: OrganizationMonthScopeMode, GroupID: 98451, AuthVersion: 3, MemberFingerprint: hex.EncodeToString(sum[:16]), MemberManifest: strings.Repeat("b", 64),
		Funding: FundingMonthRequest{Month: "2026-09", SourceIdentity: strings.Repeat("a", 64), UserIDs: []int64{201, 202}, Rules: FundingRules{Producer: FundingProducerRC4RC26, QuotaPerUnit: 500000}}}
	now := time.Date(2026, 10, 25, 0, 0, 0, 0, fundingMonthZone)
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := DecodeOrganizationMonthScope(raw, now); err != nil || got.GroupID != s.GroupID || len(got.Funding.UserIDs) != 2 {
		t.Fatal("valid current roster rejected", err)
	}
	for _, kind := range []string{"unknown", "duplicate", "case", "version", "mode", "group", "auth", "manifest", "fingerprint", "unsorted", "duplicate_user", "missing_user", "month", "future", "identity", "rules", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			p := s
			p.Funding.UserIDs = append([]int64{}, s.Funding.UserIDs...)
			switch kind {
			case "version":
				p.Version++
			case "mode":
				p.Mode = "historical_scope_verified"
			case "group":
				p.GroupID = 0
			case "auth":
				p.AuthVersion = -1
			case "manifest":
				p.MemberManifest = ""
			case "fingerprint":
				p.MemberFingerprint = strings.Repeat("a", 32)
			case "unsorted":
				p.Funding.UserIDs = []int64{202, 201}
			case "duplicate_user":
				p.Funding.UserIDs = []int64{201, 201}
			case "missing_user":
				p.Funding.UserIDs = p.Funding.UserIDs[:1]
			case "month":
				p.Funding.Month = "2026-9"
			case "future":
				p.Funding.Month = "2026-10"
			case "identity":
				p.Funding.SourceIdentity = "unknown"
			case "rules":
				p.Funding.Rules.QuotaPerUnit = 1000000
			}
			body, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "unknown":
				body = append(body[:len(body)-1], []byte(`,"approved":true}`)...)
			case "duplicate":
				body = bytes.Replace(body, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
			case "case":
				body = bytes.Replace(body, []byte(`"version":1`), []byte(`"VERSION":1`), 1)
			case "oversize":
				body = bytes.Repeat([]byte(" "), OrganizationMonthScopeLimit+1)
			}
			if _, err := DecodeOrganizationMonthScope(body, now); err == nil {
				t.Fatal("ambiguous or approval-like roster accepted")
			}
		})
	}
}
