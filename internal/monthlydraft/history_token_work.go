package monthlydraft

// Source-only work accounting, NEVER financial admission or permission to
// publish another month. The process-local capability is not customer input.
import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

type historyTokenWorkKey struct{}
type historyTokenWorkCapability struct {
	producer *automaticHistoryPreparation
	parent   SourcePreparationRequest
	scope    tokenHistoryWorkScope
}

func (historyTokenWorkCapability) MarshalJSON() ([]byte, error) {
	return nil, ErrGenerationInput
}

func (c *historyTokenWorkCapability) UnmarshalJSON([]byte) error {
	if c != nil {
		*c = historyTokenWorkCapability{}
	}
	return ErrGenerationInput
}

// An immutable locator/range, not editable monetary values or a new budget.
// Every source dispatch reconstructs it from the registered/tail originals.
type tokenHistoryWorkScope struct {
	Version        int    `json:"version"`
	From           string `json:"from"`
	Through        string `json:"through"`
	Boundary       int64  `json:"boundary"`
	RegisteredID   string `json:"registered_id"`
	SourceIdentity string `json:"source_identity"`
	ParentScopeSHA string `json:"parent_scope_sha256"`
}

// Explicit pre-Start binding to the existing queue. It creates no worker,
// schema, task or source connection, and changes no generation eligibility.
func (p *automaticHistoryPreparation) bindWorkAccounting(r *LocalRuntime) error {
	if p == nil || p.wallet == nil || p.wallet.funding == nil || r == nil {
		return ErrGenerationInput
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	q := r.preparation
	if r.phase != "disabled" {
		return ErrGenerationBusy
	}
	if q == nil || !q.accountTokenWork || !p.wallet.funding.accountTokenWork || q.historySourceWork != nil || q.continuationEnabled {
		return ErrGenerationInput
	}
	a, err := q.auth.DB()
	if err != nil {
		return err
	}
	b, err := p.wallet.funding.authorization.DB()
	if err != nil {
		return err
	}
	one, err := generationDatabaseFile(a)
	if err != nil {
		return err
	}
	two, err := generationDatabaseFile(b)
	if err != nil || !os.SameFile(one, two) {
		return ErrGenerationInput
	}
	q.historySourceWork = p
	return nil
}

func (p *automaticHistoryPreparation) workDispatch(ctx context.Context, parent SourcePreparationRequest) error {
	if !p.wallet.funding.accountTokenWork {
		return nil
	}
	d, ok := ctx.Value(tokenDispatchKey{}).(*tokenWorkDispatch)
	if !ok || d == nil || d.p == nil || !d.p.accountTokenWork || d.p.historySourceWork != p || d.receipt != nil || d.job.request != parent || d.job.key != parent.Key() {
		return ErrGenerationInput
	}
	return nil
}

// A partial restore must not recapture registration data and manufacture a
// new history work binding. Validate existing v2 locators before ANY source
// read. Tail pairs may legitimately advance after a month rollover; actual
// token checkpoints and budgets are independently checked before each capture.
func (p *automaticHistoryPreparation) checkRetainedWorkSource(ctx context.Context, parent SourcePreparationRequest) error {
	if !p.wallet.funding.accountTokenWork {
		return nil
	}
	d := ctx.Value(tokenDispatchKey{}).(*tokenWorkDispatch) // workDispatch already checked
	rows, err := d.p.db.QueryContext(ctx, `SELECT sha256,payload FROM portal_monthly_token_work_grants WHERE parent_key=? LIMIT 121`, parent.Key())
	if err != nil {
		return err
	}
	proofs := []tokenHistoryWorkScope{}
	for rows.Next() {
		var sha string
		var raw []byte
		var grant tokenWorkGrant
		if err := rows.Scan(&sha, &raw); err != nil {
			_ = rows.Close()
			return err
		}
		if len(raw) > 4096 || json.Unmarshal(raw, &grant) != nil {
			_ = rows.Close()
			return ErrGenerationIntegrity
		}
		canonical, _ := json.Marshal(grant)
		if !bytes.Equal(canonical, raw) || generationHash(raw) != sha || grant.Parent != parent || !validTokenWorkGrantVersion(grant, true) {
			_ = rows.Close()
			return ErrGenerationIntegrity
		}
		if grant.HistoryScope != nil {
			proofs = append(proofs, *grant.HistoryScope)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(proofs) == 0 {
		return nil
	}
	scope, err := p.wallet.funding.scope(ctx, parent)
	if err != nil {
		return err
	}
	registered, err := monthlysource.InspectRegisteredWalletMonthCapture(ctx, p.wallet.funding.capture, parent.Key(), scope)
	if err != nil || registered.Completed != 1 {
		return ErrGenerationIntegrity
	}
	raw, err := json.Marshal(scope)
	if err != nil {
		return err
	}
	for _, proof := range proofs {
		if proof.RegisteredID != registered.ID || proof.ParentScopeSHA != generationHash(raw) || proof.SourceIdentity != p.wallet.funding.sourceIdentity {
			return ErrGenerationIntegrity
		}
	}
	return nil
}

func (p *automaticHistoryPreparation) workContext(ctx context.Context, parent SourcePreparationRequest, months []SourcePreparationRequest) (context.Context, error) {
	if !p.wallet.funding.accountTokenWork {
		return ctx, nil
	}
	if err := p.workDispatch(ctx, parent); err != nil {
		return nil, err
	}
	scope, err := p.wallet.funding.scope(ctx, parent)
	if err != nil {
		return nil, err
	}
	registered, err := monthlysource.InspectRegisteredWalletMonthCapture(ctx, p.wallet.funding.capture, parent.Key(), scope)
	if err != nil || registered.Completed != 1 || len(months) == 0 {
		return nil, ErrGenerationIntegrity
	}
	tail, err := monthlysource.InspectWalletTailMonthCapture(ctx, p.wallet.funding.capture, parent.Key(), scope)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(scope)
	if err != nil {
		return nil, err
	}
	proof := tokenHistoryWorkScope{Version: 1, From: months[0].Month, Through: months[len(months)-1].Month, Boundary: tail.Boundary,
		RegisteredID: registered.ID, SourceIdentity: scope.Funding.SourceIdentity, ParentScopeSHA: generationHash(raw)}
	if !proof.valid(parent, parent.Month) {
		return nil, ErrGenerationIntegrity
	}
	return context.WithValue(ctx, historyTokenWorkKey{}, historyTokenWorkCapability{p, parent, proof}), nil
}

func (s tokenHistoryWorkScope) valid(parent SourcePreparationRequest, sourceMonth string) bool {
	zone := time.FixedZone("Usage-UTC+8", 8*3600)
	first, e1 := time.ParseInLocation("2006-01", s.From, zone)
	last, e2 := time.ParseInLocation("2006-01", s.Through, zone)
	if e1 != nil || e2 != nil || first.Format("2006-01") != s.From || last.Format("2006-01") != s.Through || first.Year() < 2020 {
		return false
	}
	count := (last.Year()-first.Year())*12 + int(last.Month()-first.Month()) + 1
	return s.Version == 1 && count >= 1 && count <= 120 && s.From <= parent.Month && parent.Month <= s.Through &&
		validSelectionScope(parent.Owner.GroupID, parent.Month) && validSelectionScope(parent.Owner.GroupID, sourceMonth) &&
		s.From <= sourceMonth && sourceMonth <= s.Through && s.Boundary == last.AddDate(0, 1, 0).Unix() && s.Boundary <= time.Now().Unix() &&
		generationHex(s.RegisteredID, 32) && generationHex(s.SourceIdentity, 32) && generationHex(s.ParentScopeSHA, 32)
}

// A later genuine boundary may expand the required raw range after rollover.
// Existing grants keep their FIRST bytes/budget/baseline; never replace them.
func (s tokenHistoryWorkScope) permitsRetained(old tokenHistoryWorkScope, parent SourcePreparationRequest, sourceMonth string) bool {
	return s.valid(parent, sourceMonth) && old.valid(parent, sourceMonth) && s.From == old.From && s.Boundary >= old.Boundary &&
		s.RegisteredID == old.RegisteredID && s.SourceIdentity == old.SourceIdentity && s.ParentScopeSHA == old.ParentScopeSHA
}

func tokenWorkHistoryScope(ctx context.Context, d *tokenWorkDispatch, req SourcePreparationRequest) (*tokenHistoryWorkScope, error) {
	if d == nil || d.p == nil || !d.p.accountTokenWork || d.receipt != nil || d.job.request.Owner != req.Owner {
		return nil, ErrGenerationInput
	}
	if req.Month <= d.job.request.Month {
		return nil, nil // Legacy version 1 and its serialized bytes stay unchanged.
	}
	c, ok := ctx.Value(historyTokenWorkKey{}).(historyTokenWorkCapability)
	if !ok || c.producer == nil || d.p.historySourceWork != c.producer || c.parent != d.job.request ||
		!c.producer.wallet.funding.accountTokenWork || !c.scope.valid(c.parent, req.Month) || c.scope.SourceIdentity != c.producer.wallet.funding.sourceIdentity {
		return nil, ErrGenerationIntegrity
	}
	copy := c.scope
	return &copy, nil
}

func validTokenWorkGrantVersion(g tokenWorkGrant, allowHistory bool) bool {
	if g.Parent.Owner != g.Source.Owner {
		return false
	}
	if g.Version == 1 {
		return g.HistoryScope == nil && g.Source.Month <= g.Parent.Month
	}
	return allowHistory && g.Version == 2 && g.HistoryScope != nil && g.Source.Month > g.Parent.Month && g.HistoryScope.valid(g.Parent, g.Source.Month)
}

func compatibleTokenWorkGrantHistory(saved, current tokenWorkGrant) bool {
	if saved.Version != current.Version {
		return false
	}
	if current.Version == 1 {
		return saved.HistoryScope == nil && current.HistoryScope == nil
	}
	return saved.HistoryScope != nil && current.HistoryScope != nil && current.HistoryScope.permitsRetained(*saved.HistoryScope, current.Parent, current.Source.Month)
}
