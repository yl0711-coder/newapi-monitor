package monthlysource

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"time"
)

const WalletReuseMaterialMode = "original_per_account_wallet_views_not_historical_boundaries"
const WalletReuseMaterialLimit = 2 << 20
const walletReuseInputLimit = 128 << 20

type WalletReuseAccount struct {
	UserID   int64                  `json:"user_id"`
	Archive  MonthlyObservationFile `json:"archive"`
	Selected []TokenWalletReuseView `json:"selected"`
}

// BuildWalletReuseMaterial never reads a path, writes a DB, or approves a bill.
// Every account must have its own two original eligible views. Different
// accounts' times are retained independently, never combined into one DB view.
func BuildWalletReuseMaterial(ctx context.Context, originalRaw []byte, original MonthlyObservationFile, archives map[int64][]byte, now time.Time) (WalletReuseMaterial, error) {
	var empty WalletReuseMaterial
	m, err := DecodeWalletReuseOrigin(ctx, originalRaw, original)
	if err != nil {
		return empty, err
	}
	if len(archives) != len(m.Funding.UserIDs) {
		return empty, errors.New("wallet reuse requires full account scope")
	}
	files := map[int64]MonthlyObservationFile{}
	for _, f := range m.Tokens {
		files[f.UserID] = f.File
	}
	result := WalletReuseMaterial{Version: 1, Mode: WalletReuseMaterialMode, GroupID: m.GroupID, Month: m.Funding.Month,
		SourceIdentity: m.Funding.SourceIdentity, OriginalMaterial: original, Accounts: []WalletReuseAccount{}}
	used := len(originalRaw)
	for _, id := range m.Funding.UserIDs {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		raw, exists := archives[id]
		used += len(raw)
		if !exists || used > walletReuseInputLimit {
			return empty, errors.New("missing or excessive wallet reuse archive data")
		}
		r, err := ReviewTokenWalletReuse(ctx, raw, files[id].SHA256, now)
		if err != nil {
			return empty, err
		}
		if r.SourceIdentity != m.Funding.SourceIdentity || r.UserID != id || r.Month != m.Funding.Month || len(r.Selected) != 2 || r.Status != "two_original_views_available_not_boundary_proof" {
			return empty, errors.New("wallet reuse account has no complete separated source views")
		}
		result.Accounts = append(result.Accounts, WalletReuseAccount{UserID: id, Archive: files[id], Selected: r.Selected})
	}
	raw, err := json.Marshal(result)
	if err != nil || len(raw) > WalletReuseMaterialLimit {
		return empty, errors.New("wallet reuse metadata exceeds budget")
	}
	return result, ctx.Err()
}

// A consumer supplies actual pinned originals, not monetary values from this
// manifest. Rebuilding must reproduce ALL canonical bytes before any reuse.
func VerifyWalletReuseMaterial(ctx context.Context, raw, originalRaw []byte, archives map[int64][]byte, now time.Time) (WalletReuseMaterial, error) {
	var m WalletReuseMaterial
	if len(raw) == 0 || len(raw) > WalletReuseMaterialLimit || json.Unmarshal(raw, &m) != nil {
		return m, errors.New("invalid wallet reuse metadata")
	}
	want, err := BuildWalletReuseMaterial(ctx, originalRaw, m.OriginalMaterial, archives, now)
	if err != nil {
		return WalletReuseMaterial{}, err
	}
	canonical, err := json.Marshal(want)
	if err != nil || !bytes.Equal(raw, canonical) {
		return WalletReuseMaterial{}, errors.New("wallet reuse metadata differs from original sources")
	}
	return want, ctx.Err()
}

type WalletReuseMaterial struct {
	Version          int                    `json:"version"`
	Mode             string                 `json:"mode"`
	GroupID          int64                  `json:"group_id"`
	Month            string                 `json:"month"`
	SourceIdentity   string                 `json:"source_identity"`
	OriginalMaterial MonthlyObservationFile `json:"original_material"`
	Accounts         []WalletReuseAccount   `json:"accounts"`
}

func validWalletReuseFile(f MonthlyObservationFile) bool {
	return filepath.IsAbs(f.Path) && filepath.Clean(f.Path) == f.Path && validDigest(f.SHA256)
}

// DecodeWalletReuseOrigin checks only a data-only ORIGINAL handoff, not its
// financial completeness. Source consumers must still re-audit its sealed DB.
// Recursion/overlays are forbidden; old original bytes remain unchanged.
func DecodeWalletReuseOrigin(ctx context.Context, raw []byte, ref MonthlyObservationFile) (MonthlyObservationMaterial, error) {
	var m MonthlyObservationMaterial
	if ctx == nil || len(raw) == 0 || len(raw) > WalletReuseMaterialLimit || !validWalletReuseFile(ref) || digest(raw) != ref.SHA256 || json.Unmarshal(raw, &m) != nil {
		return MonthlyObservationMaterial{}, errors.New("invalid pinned original wallet-reuse material")
	}
	canonical, err := json.Marshal(m)
	if err != nil || !bytes.Equal(raw, canonical) || m.Version != 1 || m.Mode != MonthlyObservationMaterialMode || m.GroupID <= 0 || m.WalletReuse != nil ||
		(m.FundingVersion != 1 && m.FundingVersion != 2) || len(m.WalletIDs) == 0 || len(m.WalletIDs) > 64 || len(m.Tokens) != len(m.Funding.UserIDs) ||
		len(m.UsageWindows) == 0 || len(m.UsageWindows) > 4096 || len(m.FundingWindows) == 0 || len(m.FundingWindows) > 4096 || !validWalletReuseFile(m.Database) {
		return MonthlyObservationMaterial{}, errors.New("invalid original material scope")
	}
	// Reuse the existing closed-month/account/rule validator; nil windows are
	// an inspection of scope only, never treated as observed financial coverage.
	if _, err := ReviewFundingMonth(ctx, m.Funding, nil); err != nil {
		return MonthlyObservationMaterial{}, err
	}
	seen := map[string]bool{}
	for _, id := range m.WalletIDs {
		if !validDigest(id) || seen[id] {
			return MonthlyObservationMaterial{}, errors.New("invalid original wallet IDs")
		}
		seen[id] = true
	}
	for i, id := range m.Funding.UserIDs {
		if i > 0 && id <= m.Funding.UserIDs[i-1] {
			return MonthlyObservationMaterial{}, errors.New("unsorted original member scope")
		}
	}
	users := map[int64]bool{}
	for _, f := range m.Tokens {
		if !slices.Contains(m.Funding.UserIDs, f.UserID) || users[f.UserID] || !validWalletReuseFile(f.File) {
			return MonthlyObservationMaterial{}, errors.New("invalid token archive selection")
		}
		users[f.UserID] = true
	}
	return m, ctx.Err()
}
