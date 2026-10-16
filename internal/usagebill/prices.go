package usagebill

import (
	"errors"
	"math"
	"net/url"
	"strings"
)

var ErrInvalidPriceCatalogue = errors.New("invalid or overlapping price evidence")

// PriceEvidence is an approved price interval, not a scraper result. Observing
// a current web page does not establish its applicability to earlier requests.
// All ranges are half-open. MaxInput == 0 means no upper bound.
type PriceEvidence struct {
	ID, Model, ServiceTier, SourceURL string
	From, Until, VerifiedAt           int64
	CurrentObservationOnly            bool
	MinInput, MaxInput                int64
	InputUSD, OutputUSD               string // USD per million tokens
	CacheReadUSD, CacheWriteUSD       string // empty means unknown/unsupported, NOT free
}

// PriceCatalogue owns a private copy. Callers cannot mutate a running export's
// pricing by changing their original slice. It has no network or disk access.
type PriceCatalogue struct{ entries []PriceEvidence }

func NewPriceCatalogue(entries []PriceEvidence) (PriceCatalogue, error) {
	if len(entries) == 0 || len(entries) > 512 {
		return PriceCatalogue{}, ErrInvalidPriceCatalogue
	}
	copyEntries := append([]PriceEvidence(nil), entries...)
	ids := make(map[string]bool)
	for i, e := range copyEntries {
		u, err := url.Parse(e.SourceURL)
		if strings.TrimSpace(e.ID) == "" || ids[e.ID] || strings.TrimSpace(e.Model) == "" || strings.TrimSpace(e.ServiceTier) == "" ||
			e.From <= 0 || e.Until <= e.From || e.VerifiedAt <= 0 || (e.CurrentObservationOnly && e.From < e.VerifiedAt) ||
			e.MinInput < 0 || (e.MaxInput != 0 && e.MaxInput <= e.MinInput) ||
			err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" ||
			!priceDecimal.MatchString(e.InputUSD) || !priceDecimal.MatchString(e.OutputUSD) ||
			(e.CacheReadUSD != "" && !priceDecimal.MatchString(e.CacheReadUSD)) ||
			(e.CacheWriteUSD != "" && !priceDecimal.MatchString(e.CacheWriteUSD)) {
			return PriceCatalogue{}, ErrInvalidPriceCatalogue
		}
		ids[e.ID] = true
		for _, previous := range copyEntries[:i] {
			if previous.Model == e.Model && previous.ServiceTier == e.ServiceTier &&
				previous.From < e.Until && e.From < previous.Until &&
				(previous.MaxInput == 0 || e.MinInput < previous.MaxInput) && (e.MaxInput == 0 || previous.MinInput < e.MaxInput) {
				return PriceCatalogue{}, ErrInvalidPriceCatalogue
			}
		}
	}
	return PriceCatalogue{entries: copyEntries}, nil
}

// Match performs exact matching. No model-name aliases, current-price fallback,
// missing-tier default or nearest-period match is safe here.
func (c PriceCatalogue) Match(model, tier string, at, input int64) (PriceEvidence, bool) {
	if input < 0 {
		return PriceEvidence{}, false
	}
	for _, e := range c.entries {
		if model == e.Model && tier == e.ServiceTier && at >= e.From && at < e.Until && input >= e.MinInput && (e.MaxInput == 0 || input < e.MaxInput) {
			return e, true
		}
	}
	return PriceEvidence{}, false
}

// ReferenceOpenAIText uses independently normalized token evidence. It neither
// verifies actual charges nor proves absence of tool/media/subscription costs;
// callers must establish those scopes before offering a full-row comparison.
// Anthropic TTL write prices require a distinct adapter, never this flat rate.
func (c PriceCatalogue) ReferenceOpenAIText(model, tier string, at int64, usage TokenUsage) (Decimal, PriceEvidence, error) {
	if !usage.Complete || usage.Semantic != "openai" {
		return Decimal{}, PriceEvidence{}, ErrIncompletePrice
	}
	counts := []TokenCount{usage.OrdinaryInput, usage.Output, usage.CacheRead, usage.CacheWrite}
	for _, n := range counts {
		if n.State != CountValid || n.Value < 0 {
			return Decimal{}, PriceEvidence{}, ErrIncompletePrice
		}
	}
	input := counts[0].Value
	for _, n := range counts[2:] {
		if n.Value > math.MaxInt64-input {
			return Decimal{}, PriceEvidence{}, ErrIncompletePrice
		}
		input += n.Value
	}
	e, ok := c.Match(model, tier, at, input)
	if !ok {
		return Decimal{}, PriceEvidence{}, ErrIncompletePrice
	}
	prices := []string{e.InputUSD, e.OutputUSD, e.CacheReadUSD, e.CacheWriteUSD}
	components := make([]PriceComponent, 0, 4)
	for i, n := range counts {
		if i >= 2 && prices[i] == "" && n.Value == 0 {
			continue // proven zero use needs no invented cache price
		}
		components = append(components, PriceComponent{Tokens: n.Value, UnitTokens: 1000000, UnitPriceUSD: prices[i]})
	}
	amount, err := OfficialOriginalCNY(components)
	if err != nil {
		return Decimal{}, PriceEvidence{}, err
	}
	return amount, e, nil
}
