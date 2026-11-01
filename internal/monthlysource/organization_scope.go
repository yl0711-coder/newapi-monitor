package monthlysource

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"
)

// OrganizationMonthScope is a current authorized collection roster, NOT
// historical membership, a wallet boundary or permission to issue a bill.
// The source collector deliberately has no dependency on Monitor/Generation.
type OrganizationMonthScope struct {
	Version           int                 `json:"version"`
	Mode              string              `json:"mode"`
	GroupID           int64               `json:"group_id"`
	AuthVersion       int64               `json:"auth_version"`
	MemberFingerprint string              `json:"member_fingerprint"`
	MemberManifest    string              `json:"member_manifest"`
	Funding           FundingMonthRequest `json:"funding"`
}

const OrganizationMonthScopeMode = "current_usage_roster_not_history_or_bill_approval"
const OrganizationMonthScopeLimit = 64 << 10

func DecodeOrganizationMonthScope(raw []byte, now time.Time) (OrganizationMonthScope, error) {
	var s OrganizationMonthScope
	fail := func() (OrganizationMonthScope, error) {
		return OrganizationMonthScope{}, errors.New("invalid current organization collection roster")
	}
	if len(raw) == 0 || len(raw) > OrganizationMonthScopeLimit || json.Unmarshal(raw, &s) != nil {
		return fail()
	}
	canonical, err := json.Marshal(s)
	month, monthErr := time.ParseInLocation("2006-01", s.Funding.Month, fundingMonthZone)
	if err != nil || !bytes.Equal(bytes.TrimSpace(raw), canonical) || s.Version != 1 || s.Mode != OrganizationMonthScopeMode ||
		s.GroupID <= 0 || s.AuthVersion < 0 || !validDigest(s.MemberManifest) || !validDigest(s.Funding.SourceIdentity) ||
		monthErr != nil || month.Format("2006-01") != s.Funding.Month || month.Year() < 2020 || month.AddDate(0, 1, 0).After(now) ||
		s.Funding.Rules != (FundingRules{Producer: FundingProducerRC4RC26, QuotaPerUnit: 500000}) || len(s.Funding.UserIDs) == 0 || len(s.Funding.UserIDs) > WalletBatchLimit {
		return fail()
	}
	h := sha256.New()
	for i, id := range s.Funding.UserIDs {
		if id <= 0 || (i > 0 && s.Funding.UserIDs[i-1] >= id) {
			return fail()
		}
		_, _ = h.Write([]byte(strconv.FormatInt(id, 10) + ","))
	}
	if s.MemberFingerprint != hex.EncodeToString(h.Sum(nil)[:16]) {
		return fail()
	}
	return s, nil
}
