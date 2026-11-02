package monthlysource

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

// No file paths or monetary values are manufactured here. The selection pins
// actual original records. It still requires a new immutable scoped source
// copy and independently established history before it can prepare a bill.
const CapturedMonthSelectionMode = "audited_capture_selection_requires_sealed_sources_and_history"

// Version 2 admits bounded, exact child windows, not synthetic daily parents.
// Version 1 readers continue to reject this distinct handoff.
const RecoveredCapturedMonthSelectionMode = "audited_child_capture_selection_requires_sealed_sources_and_history"

type CapturedTokenSelection struct {
	UserID  int64                `json:"user_id"`
	Parents []string             `json:"parents"`
	Usage   []UsageWindowVersion `json:"usage"`
}

type CapturedMonthSelection struct {
	Version    int                         `json:"version"`
	Mode       string                      `json:"mode"`
	Key        string                      `json:"key"`
	PlanSHA256 string                      `json:"plan_sha256"`
	Scope      OrganizationMonthScope      `json:"scope"`
	Funding    []FundingMonthWindowVersion `json:"funding_windows"`
	WalletIDs  []string                    `json:"wallet_ids"`
	Tokens     []CapturedTokenSelection    `json:"tokens"`
}

type CapturedTokenArchive struct {
	UserID int64
	Raw    []byte
}

type CapturedMonthAssembly struct {
	Selection []byte
	Tokens    []CapturedTokenArchive
}

// BuildCapturedMonthSelection is read-only LOCAL assembly. It never fingerprints
// a still-changing SQLite file as sealed evidence and never writes a statement,
// opening balance, historical-scope approval, or source-selection ledger.
func BuildCapturedMonthSelection(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope) (CapturedMonthAssembly, error) {
	return buildCapturedMonthSelection(parent, receiver, key, scope, false, false)
}

// BuildRecoveredCapturedMonthSelection explicitly opts into original adaptive
// child pairs for failed days. It never captures, repairs or initializes a DB.
// With no selected split it emits the unchanged version 1 representation.
func BuildRecoveredCapturedMonthSelection(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope) (CapturedMonthAssembly, error) {
	return buildCapturedMonthSelection(parent, receiver, key, scope, true, false)
}

// Explicit partition-aware assembly keeps the original selection bytes and
// token archive identities; it does not upgrade legacy callers or approve bills.
func BuildPartitionedCapturedMonthSelection(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope, recovered bool) (CapturedMonthAssembly, error) {
	return buildCapturedMonthSelection(parent, receiver, key, scope, recovered, true)
}

func buildCapturedMonthSelection(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope, recovered, partitioned bool) (CapturedMonthAssembly, error) {
	var empty CapturedMonthAssembly
	if parent == nil || receiver == nil {
		return empty, ErrTokenMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	plan, err := loadFundingMonthCapturePlan(ctx, receiver, key, scope, time.Now())
	if err != nil {
		return empty, err
	}
	scope = plan.Scope // use the detached, validated pinned roster throughout
	progress, err := InspectFundingMonthCapture(ctx, receiver, key, scope)
	if err != nil {
		return empty, err
	}
	if !progress.TraversalsFinished {
		return empty, ErrFundingMonthCaptureIntegrity
	}
	wallet, err := InspectWalletMonthCapture(ctx, receiver, key, scope)
	if err != nil {
		return empty, err
	}
	if wallet.Completed != 2 {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	if !recovered {
		token, err := InspectTokenMonthCapture(ctx, receiver, key, scope)
		if err != nil {
			return empty, err
		}
		if !token.TraversalsFinished {
			return empty, ErrTokenMonthCaptureIntegrity
		}
	}
	planRaw, err := json.Marshal(plan)
	if err != nil {
		return empty, err
	}
	selection := CapturedMonthSelection{Version: 1, Mode: CapturedMonthSelectionMode, Key: key, PlanSHA256: digest(planRaw), Scope: scope, WalletIDs: wallet.IDs,
		Funding: []FundingMonthWindowVersion{}, Tokens: []CapturedTokenSelection{}}
	windows := make([]FundingWindowObservation, 0, len(plan.Scans)/2)
	for i := 0; i < len(plan.Scans)/2; i++ {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		o, err := ReviewFundingWindow(ctx, receiver, plan.Scans[i].ID, plan.Scans[i+len(plan.Scans)/2].ID)
		if err != nil {
			return empty, err
		}
		windows = append(windows, o)
	}
	funding, err := ReviewFundingMoney(ctx, scope.Funding, windows)
	if err != nil {
		return empty, err
	}
	if len(funding.Windows.MissingWindows) != 0 || len(funding.Accounts) != len(scope.Funding.UserIDs) {
		return empty, ErrFundingMonthCaptureIntegrity
	}
	for _, a := range funding.Accounts {
		if a.Status != "observed_month" || a.Totals == nil {
			return empty, ErrFundingMonthCaptureIntegrity
		}
	}
	selection.Funding = funding.Windows.Windows
	// These are derived only from fully validated ORIGINAL token observations,
	// including their matching user-total projection. Their names/IDs/times and
	// integer amounts stay unchanged in the later token archive.
	archives := make([]TokenUsageMonthArchive, 0, len(scope.Funding.UserIDs))
	used := 0
	windowsUsed := 0
	for _, id := range scope.Funding.UserIDs {
		var pairs []TokenUsageWindowPair
		if recovered {
			var split bool
			pairs, _, split, err = loadRecoveredTokenMonth(ctx, receiver, key, scope, id)
			if split {
				selection.Version, selection.Mode = 2, RecoveredCapturedMonthSelectionMode
			}
		} else {
			// Keep a fresh indexed-digest/byte-identity check per member, as
			// before. Only reuse the deep decoding of this SAME pinned plan;
			// never reuse a plan after its bytes or digest have changed.
			// The existing per-account ten-second budget also includes this
			// reread, not a fresh budget added before the original reader.
			accountCtx, accountCancel := context.WithTimeout(ctx, 10*time.Second)
			err = confirmFundingCapturePlan(accountCtx, receiver, key, planRaw)
			if err == nil {
				pairs, _, err = loadCapturedTokenMonthPlan(accountCtx, receiver, plan, id)
			}
			accountCancel()
		}
		if err != nil {
			return empty, err
		}
		windowsUsed += len(pairs)
		if !partitioned && recovered && windowsUsed > 4096 {
			return empty, ErrTokenMonthCaptureBudget
		}
		entry := CapturedTokenSelection{UserID: id, Parents: []string{}, Usage: []UsageWindowVersion{}}
		for _, pair := range pairs {
			var versions UsageWindowVersion
			for phase, o := range []TokenUsageWindowObservation{pair.Older, pair.Newer} {
				raw, err := json.Marshal(o)
				if err != nil {
					return empty, err
				}
				used += len(raw)
				if used > tokenMonthCaptureBytes {
					return empty, ErrTokenMonthCaptureBudget
				}
				entry.Parents = append(entry.Parents, digest(raw))
				u, err := UsageObservationFromToken(o)
				if err != nil {
					return empty, err
				}
				body, err := usageWindowBytes(u)
				if err != nil {
					return empty, err
				}
				if phase == 0 {
					versions.OlderID = digest(body)
				} else {
					versions.NewerID = digest(body)
				}
			}
			entry.Usage = append(entry.Usage, versions)
		}
		// Audit each account independently within the EXISTING window limit.
		// Grouping many members must neither net their differences nor create
		// an unnecessarily huge aggregate window list for this local audit.
		accountRequest := scope.Funding
		accountRequest.UserIDs = []int64{id}
		usage, err := ReviewUsageMonth(ctx, receiver, accountRequest, entry.Usage)
		if err != nil {
			return empty, err
		}
		report := usage.Report()
		if len(report.Accounts) != 1 {
			return empty, ErrTokenMonthCaptureIntegrity
		}
		a := report.Accounts[0]
		if a.UserID != id || a.Totals == nil || len(a.MissingWindows) != 0 || a.MatchingSeconds != report.To-report.From {
			return empty, ErrTokenMonthCaptureIntegrity
		}
		selection.Tokens = append(selection.Tokens, entry)
		archives = append(archives, TokenUsageMonthArchive{Version: 1, Basis: TokenUsageArchiveBasis, SourceIdentity: scope.Funding.SourceIdentity,
			Rules: scope.Funding.Rules, UserID: id, Month: scope.Funding.Month, Pairs: pairs})
	}
	if recovered && selection.Version == 1 {
		// No selected child means the output still claims the legacy protocol.
		// Preserve its WHOLE-sweep wait, not merely each pair's separation.
		token, err := InspectTokenMonthCapture(ctx, receiver, key, scope)
		if err != nil {
			return empty, err
		}
		if !token.TraversalsFinished {
			return empty, ErrTokenMonthCaptureIntegrity
		}
	}
	// A plan changed during the last member's read must not produce an
	// apparently complete selection either. This is still local/read-only.
	if err := confirmFundingCapturePlan(ctx, receiver, key, planRaw); err != nil {
		return empty, err
	}
	raw, err := json.Marshal(selection)
	if err != nil || len(raw) > 2<<20 {
		return empty, ErrTokenMonthCaptureBudget
	}
	if partitioned {
		parts, err := PartitionCapturedUsageIndex(ctx, raw, time.Now())
		if err != nil {
			return empty, err
		}
		if _, err := ReviewPartitionedCapturedUsage(ctx, receiver, digest(raw), scope, parts); err != nil {
			return empty, err
		}
	}
	out := CapturedMonthAssembly{Selection: raw, Tokens: []CapturedTokenArchive{}}
	// Selection contains parent/usage IDs, not its own hash or archive hashes.
	// Hashing it first therefore creates no circular evidence reference.
	for _, archive := range archives {
		archive.SelectionSHA256 = digest(raw)
		body, err := json.Marshal(archive)
		if err != nil || len(body) > TokenUsageArchiveLimit {
			return empty, ErrTokenMonthCaptureBudget
		}
		if _, _, err := DecodeTokenUsageMonthArchive(ctx, body, time.Now()); err != nil {
			return empty, err
		}
		out.Tokens = append(out.Tokens, CapturedTokenArchive{UserID: archive.UserID, Raw: body})
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return out, nil
}
