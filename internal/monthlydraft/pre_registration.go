package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Source comparison only, never permission to manufacture a zero bill. The
// later original registration record and no-earlier-activity scope must also
// pass the final candidate audit. Missing observations remain missing.
func futureZeroOrigin(a monitor.MonthlyPreparationAccount, end int64) bool {
	return a.Baseline.At >= end && a.Baseline.BalanceQuota == 0 && a.Baseline.UsedQuota == 0
}

func emptyObservedBeforeOrigin(a monitor.MonthlyPreparationAccount, o monthlysource.UsageMonthAccountReport, end int64) bool {
	if !futureZeroOrigin(a, end) || o.UserID != a.UserID || o.Totals == nil || len(o.Totals.Sources) != 0 ||
		o.MatchingSeconds <= 0 || o.ObservedSeconds != o.MatchingSeconds || len(o.MissingWindows) != 0 {
		return false
	}
	for _, s := range a.MonthStatistics {
		if s.Requests != 0 || s.Tokens != 0 {
			return false
		}
	}
	cursor := end - o.MatchingSeconds
	for _, w := range o.Windows {
		if !w.Matching || len(w.Issues) != 0 || len(w.Rows) != 0 || w.From != cursor || w.To <= w.From || w.To > end {
			return false // Even a zero-price request is genuine activity.
		}
		cursor = w.To
	}
	return cursor == end
}

// Process-local source audit. It cannot be supplied through JSON or a customer
// request, and it is bound to both exact input and historical-scope bytes.
type registrationContext struct {
	input, scope string
	events       map[int64]monitor.MonthlyBundleEvent
}

func (g *Generation) observedAutomaticContext(ctx context.Context, p GenerationPlan, raw []byte, windows []monthlysource.FundingWindowObservation, scope AutomaticScope) (AutomaticSourceContext, error) {
	input, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil {
		return AutomaticSourceContext{}, err
	}
	if p.AutomaticHistorySourceID != "" {
		derived, err := g.auditAutomaticHistoryScope(ctx, p, input)
		if err != nil {
			return AutomaticSourceContext{}, err
		}
		if derived == nil {
			return AutomaticSourceContext{}, ErrGenerationEvidence
		}
		want, err := json.Marshal(derived)
		if err != nil {
			return AutomaticSourceContext{}, err
		}
		got, err := json.Marshal(scope)
		if err != nil || !bytes.Equal(want, got) {
			return AutomaticSourceContext{}, ErrGenerationEvidence
		}
		planRaw, err := json.Marshal(p)
		if err != nil {
			return AutomaticSourceContext{}, err
		}
		w := historyCandidateWitness{input: generationHash(raw), scope: generationHash(got), plan: generationHash(planRaw)}
		ctx = context.WithValue(ctx, historyCandidateWitnessKey{}, w)
	}
	proof, err := g.auditPreRegistration(ctx, p, raw, input, scope)
	if err != nil {
		return AutomaticSourceContext{}, err
	}
	if proof == nil {
		return observedAutomaticContext(ctx, p, raw, windows, scope)
	}
	return observedAutomaticContext(ctx, p, raw, windows, scope, proof)
}

func (g *Generation) auditPreRegistration(ctx context.Context, p GenerationPlan, raw []byte, input monitor.MonthlyPreparationInput, scope AutomaticScope) (*registrationContext, error) {
	month, err := time.ParseInLocation("2006-01", p.Funding.Month, time.FixedZone("Usage-UTC+8", 8*3600))
	if err != nil {
		return nil, ErrGenerationInput
	}
	end := month.AddDate(0, 1, 0).Unix()
	needed := false
	for _, a := range scope.Accounts {
		needed = needed || (a.Registration != nil && a.Registration.At >= end)
	}
	if !needed {
		return nil, ctx.Err() // Preserve the ordinary no-extra-source-read path.
	}
	scopeRaw, err := json.Marshal(scope)
	if err != nil {
		return nil, err
	}
	proof := &registrationContext{input: generationHash(raw), scope: generationHash(scopeRaw), events: map[int64]monitor.MonthlyBundleEvent{}}
	cache := map[string][]byte{}
	bytesRead := 0
	for _, meta := range scope.Accounts {
		r := meta.Registration
		if r == nil || r.At < end {
			continue
		}
		var account *monitor.MonthlyPreparationAccount
		for j := range input.Accounts {
			if input.Accounts[j].UserID == meta.UserID {
				account = &input.Accounts[j]
			}
		}
		if account == nil || !futureZeroOrigin(*account, end) || account.Baseline.At > r.At || r.NoEarlierActivitySource == "" {
			return nil, ErrGenerationEvidence
		}
		id, pointer, found := strings.Cut(r.Source, "#")
		if !found || !validScopeSourceID(id) || !strings.HasPrefix(pointer, "/") {
			return nil, ErrGenerationEvidence
		}
		body, cached := cache[id]
		if !cached {
			source, err := tokenSource(input, id)
			if err != nil {
				return nil, err
			}
			body, err = g.readPrivatePreparationFile(ctx, monthlysource.MonthlyObservationFile{Path: source.Path, SHA256: source.SHA256}, 1<<20)
			if err != nil {
				return nil, err
			}
			bytesRead += len(body)
			if len(cache) >= 32 || bytesRead > monitor.MonthlyPreparationInputLimit {
				return nil, ErrGenerationCapacity
			}
			cache[id] = body
		}
		event, err := registrationFromOriginal(body, pointer, meta.UserID, *r, p.Funding.Rules)
		if err != nil {
			return nil, err
		}
		matched := 0
		for _, m := range account.Movements {
			if m.ID == event.ID && m.UserID == account.UserID && m.At == event.At && m.DeltaQuota == event.DeltaQuota {
				event.Source = m.Source
				matched++
			}
		}
		if matched != 1 {
			return nil, ErrGenerationEvidence
		}
		if _, duplicate := proof.events[meta.UserID]; duplicate {
			return nil, ErrGenerationEvidence
		}
		proof.events[meta.UserID] = event
	}
	return proof, ctx.Err()
}

func registrationFromOriginal(raw []byte, pointer string, user int64, selected monitor.MonthlyBundleRegistration, rules monthlysource.FundingRules) (monitor.MonthlyBundleEvent, error) {
	var empty monitor.MonthlyBundleEvent
	row, err := registrationJSONPointer(raw, pointer)
	if err != nil {
		return empty, err
	}
	fields, err := registrationJSONObject(row)
	if err != nil || len(fields) != 6 {
		return empty, ErrGenerationEvidence
	}
	for _, key := range []string{"id", "user_id", "created_at", "type", "content", "other"} {
		if value, exists := fields[key]; !exists || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return empty, ErrGenerationEvidence
		}
	}
	var log monthlysource.FundingLog
	if json.Unmarshal(row, &log) != nil {
		return empty, ErrGenerationEvidence
	}
	r := monthlysource.NormalizeFundingLog(log, rules)
	if log.UserID != user || log.ID != selected.EventID || log.CreatedAt != selected.At || r.Status != monthlysource.FundingMovement ||
		r.TargetUserID != user || r.DeltaQuota == nil || *r.DeltaQuota <= 0 || r.Kind != "gift" || r.Method != "注册赠送" {
		return empty, ErrGenerationEvidence
	}
	return monitor.MonthlyBundleEvent{ID: log.ID, At: log.CreatedAt, DeltaQuota: *r.DeltaQuota, Kind: r.Kind, Method: r.Method}, nil
}

// A bounded RFC6901 selection from an ORIGINAL retained JSON document. Reject
// duplicate keys along the selected path and in the selected raw source row.
func registrationJSONPointer(raw []byte, pointer string) ([]byte, error) {
	parts := strings.Split(pointer, "/")
	if len(pointer) > 1024 || len(parts) < 2 || len(parts) > 33 || parts[0] != "" {
		return nil, ErrGenerationEvidence
	}
	for _, encoded := range parts[1:] {
		key := strings.ReplaceAll(strings.ReplaceAll(encoded, "~1", "/"), "~0", "~")
		if strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1") != encoded {
			return nil, ErrGenerationEvidence
		}
		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 {
			return nil, ErrGenerationEvidence
		}
		if raw[0] == '{' {
			object, err := registrationJSONObject(raw)
			if err != nil {
				return nil, err
			}
			value, found := object[key]
			if !found {
				return nil, ErrGenerationEvidence
			}
			raw = value
		} else {
			index, err := strconv.Atoi(key)
			var rows []json.RawMessage
			if raw[0] != '[' || err != nil || index < 0 || strconv.Itoa(index) != key || json.Unmarshal(raw, &rows) != nil || index >= len(rows) {
				return nil, ErrGenerationEvidence
			}
			raw = rows[index]
		}
	}
	return raw, nil
}

func registrationJSONObject(raw []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	start, err := d.Token()
	if err != nil || start != json.Delim('{') {
		return nil, ErrGenerationEvidence
	}
	object := map[string]json.RawMessage{}
	for d.More() {
		token, err := d.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return nil, ErrGenerationEvidence
		}
		if _, duplicate := object[key]; duplicate {
			return nil, ErrGenerationEvidence
		}
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return nil, ErrGenerationEvidence
		}
		object[key] = value
	}
	if end, err := d.Token(); err != nil || end != json.Delim('}') {
		return nil, ErrGenerationEvidence
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, ErrGenerationEvidence
	}
	return object, nil
}
