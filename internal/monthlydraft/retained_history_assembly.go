package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// A process-local producer binding, never a serialized permission or money
// attestation. It allows only month selection in existing original history.
// Balances, counters, movements and refund evidence still come from originals
// and pass the unchanged normalization, reconciliation and publication gates.
type retainedHistoryAssembly struct {
	owner                        monthlytasks.Owner
	month, identity, scopeSHA256 string
	history, material, selection monthlysource.MonthlyObservationFile
}

func (a *retainedHistoryAssembly) matches(owner monthlytasks.Owner, month, identity string, history, material, selection monthlysource.MonthlyObservationFile, scope []byte) bool {
	return a != nil && a.owner == owner && a.month == month && a.identity == identity && a.history == history &&
		a.material == material && a.selection == selection && a.scopeSHA256 == generationHash(scope)
}

// Background only. Uses existing original account history and the complete
// audited captured month. Registration eligibility may use its bounded lookup;
// no financial source capture, wallet-as-month-end conversion, guessed origin,
// new balance or copying of another month's corrections occurs here.
func (d *Deployment) assembleRetainedHistoryScope(ctx context.Context, spool *CapturedSourceSpool, req SourcePreparationRequest, files CapturedSourceFiles,
	history monthlysource.MonthlyObservationFile) (RetainedAutomaticScope, *retainedHistoryAssembly, error) {
	var empty RetainedAutomaticScope
	if ctx == nil || d == nil || d.g == nil || d.runtime == nil || d.runtime.scope == nil || !d.runtime.scope.registered || d.runtime.scope.authorize == nil ||
		spool == nil || spool.wallet == nil || spool.wallet.funding == nil || d.runtime.worker == nil || !d.runtime.worker.dynamicSeries ||
		spool.wallet.funding.sourceIdentity != d.runtime.worker.seriesIdentity || files.Mode != capturedSourceFilesMode {
		return empty, nil, ErrGenerationInput
	}
	if err := d.runtime.scope.authorize(ctx, monthlytasks.Request{Owner: req.Owner, Month: req.Month}); err != nil {
		return empty, nil, err
	}
	zone := time.FixedZone("Usage-UTC+8", 8*3600)
	month, err := time.ParseInLocation("2006-01", req.Month, zone)
	if err != nil || month.Format("2006-01") != req.Month || month.AddDate(0, 1, 0).After(time.Now()) {
		return empty, nil, ErrGenerationInput
	}
	current, err := spool.wallet.funding.scope(ctx, req)
	if err != nil {
		return empty, nil, err
	}
	raw, err := d.g.readPrivatePreparationFile(ctx, history, monitor.MonthlyPreparationInputLimit)
	if err != nil {
		return empty, nil, err
	}
	original, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil {
		return empty, nil, err
	}
	accounts := map[int64]monitor.MonthlyPreparationAccount{}
	for _, account := range original.Accounts {
		accounts[account.UserID] = account
	}
	registrations, err := d.g.retainedLateMemberRegistrations(ctx, spool.wallet.funding, req, original, month.Unix(), month.AddDate(0, 1, 0).Unix())
	if err != nil {
		return empty, nil, err
	}
	var group monitor.CustomerGroup
	if err := spool.wallet.funding.authorization.WithContext(ctx).Select("id", "name").Where("id = ?", req.Owner.GroupID).Take(&group).Error; err != nil {
		return empty, nil, err
	}
	document := RetainedAutomaticScope{Version: 1, Owner: req.Owner, Month: req.Month, SourceIdentity: current.Funding.SourceIdentity,
		Scope: AutomaticScope{Group: monitor.MonthlyBundleGroup{GroupID: req.Owner.GroupID, Name: group.Name, Source: organizationRosterSourceID + "#current-authorized-scope", Months: []string{req.Month}}}}
	for _, id := range current.Funding.UserIDs {
		account, exists := accounts[id]
		// A later origin is supported ONLY through actual original registration
		// and zero-origin proof, followed by the unchanged empty-month audits.
		if !exists || account.GroupID != req.Owner.GroupID || account.CounterBasis != "net_consumption" || account.Baseline.At <= 0 ||
			(account.Baseline.At > month.Unix() && registrations[id] == nil) || len(account.Username) > 256 {
			return empty, nil, ErrGenerationEvidence
		}
		document.Scope.Group.Members = append(document.Scope.Group.Members, monitor.MonthlyBundleMember{UserID: id, Username: account.Username})
		document.Scope.Accounts = append(document.Scope.Accounts, AutomaticWalletScope{UserID: id, WalletOnly: true,
			WalletSource: observationMaterialSourceID + "#captured-wallet-month:" + req.Month + ":user:" + strconv.FormatInt(id, 10), Registration: registrations[id]})
	}
	// This re-audits the existing LOCAL capture (no source SELECT), including
	// every account/token, funding and wallet pair. Its token-month validator
	// rejects subscription/unspecified billing sources; matching totals alone
	// cannot establish WalletOnly. Compare to the exact sealed selection too.
	assembled, err := spool.wallet.AssembleCapturedSources(ctx, req)
	if err != nil {
		return empty, nil, err
	}
	selection, err := d.g.readPrivatePreparationFile(ctx, files.Selection, 2<<20)
	if err != nil {
		return empty, nil, err
	}
	if !bytes.Equal(selection, assembled.Selection) {
		return empty, nil, ErrGenerationChanged
	}
	if _, err := d.g.readPrivatePreparationFile(ctx, files.Material, 2<<20); err != nil {
		return empty, nil, err
	}
	if err := d.runtime.scope.authorize(ctx, monthlytasks.Request{Owner: req.Owner, Month: req.Month}); err != nil {
		return empty, nil, err
	}
	body, err := json.Marshal(document)
	if err != nil || len(body) > monitor.MonthlyPreparationInputLimit {
		return empty, nil, ErrGenerationCapacity
	}
	return document, &retainedHistoryAssembly{owner: req.Owner, month: req.Month, identity: document.SourceIdentity, scopeSHA256: generationHash(body),
		history: history, material: files.Material, selection: files.Selection}, ctx.Err()
}
