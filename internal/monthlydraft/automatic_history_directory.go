package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

const automaticHistoryDirectoryMode = "directory_labels_not_historical_wallet_proof"

// Private server-owned metadata, never customer-supplied balances or an
// assertion that current group membership proves historical financial scope.
type automaticHistoryDirectory struct {
	Version int                           `json:"version"`
	Mode    string                        `json:"mode"`
	Owner   monthlytasks.Owner            `json:"owner"`
	Name    string                        `json:"name"`
	Members []monitor.MonthlyBundleMember `json:"members"`
}

func captureAutomaticHistoryDirectory(ctx context.Context, db *gorm.DB, owner monthlytasks.Owner) ([]byte, error) {
	d, err := monitor.LoadMonthlyGenerationDirectory(ctx, db, owner.GroupID, owner.AuthVersion)
	if err != nil {
		if errors.Is(err, monitor.ErrMonthlyGenerationUnauthorized) {
			return nil, ErrGenerationChanged
		}
		return nil, err
	}
	if (monthlytasks.Owner{GroupID: d.Scope.GroupID, AuthVersion: d.Scope.AuthVersion, MemberFingerprint: d.Scope.MemberFingerprint, MemberManifest: d.Scope.MemberManifest}) != owner {
		return nil, ErrGenerationChanged
	}
	raw, err := json.Marshal(automaticHistoryDirectory{Version: 1, Mode: automaticHistoryDirectoryMode, Owner: owner, Name: d.Name, Members: d.Members})
	if err != nil {
		return nil, err
	}
	if _, err := decodeAutomaticHistoryDirectory(raw, owner); err != nil {
		return nil, err
	}
	return raw, nil
}

func decodeAutomaticHistoryDirectory(raw []byte, owner monthlytasks.Owner) (automaticHistoryDirectory, error) {
	var d automaticHistoryDirectory
	if len(raw) == 0 || len(raw) > 1<<20 || json.Unmarshal(raw, &d) != nil {
		return automaticHistoryDirectory{}, ErrGenerationIntegrity
	}
	canonical, err := json.Marshal(d)
	validLabel := func(s string) bool { return len(s) <= 256 && strings.TrimSpace(s) != "" && utf8.ValidString(s) }
	if err != nil || !bytes.Equal(raw, canonical) || d.Version != 1 || d.Mode != automaticHistoryDirectoryMode || d.Owner != owner || owner.GroupID <= 0 || owner.AuthVersion < 0 ||
		!generationHex(owner.MemberManifest, 32) || !generationHex(owner.MemberFingerprint, 16) || !validLabel(d.Name) || len(d.Members) == 0 || len(d.Members) > 200 {
		return automaticHistoryDirectory{}, ErrGenerationIntegrity
	}
	ids := make([]int64, len(d.Members))
	for i, m := range d.Members {
		if m.UserID <= 0 || !validLabel(m.Username) || i > 0 && d.Members[i-1].UserID >= m.UserID {
			return automaticHistoryDirectory{}, ErrGenerationIntegrity
		}
		ids[i] = m.UserID
	}
	if generationMemberHash(ids) != owner.MemberFingerprint {
		return automaticHistoryDirectory{}, ErrGenerationChanged
	}
	return d, nil
}
