// Package usagebill implements the isolated arithmetic for customer text bills.
// It does not read logs, classify settlements, select official prices, or charge
// users. Callers must supply verified wallet charges and normalized price inputs.
package usagebill

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
)

// RuleVersion identifies the fixed, customer-approved bill presentation rule.
// It does not change the currency semantics of existing Monitor/Usage fields.
const RuleVersion = "usage-text-cny-v1"

const (
	quotaPerCNY      = 500000
	officialUSDToCNY = 7
	maxComponents    = 32
)

var (
	// ErrMissingQuota prevents an absent charge from becoming a free request.
	ErrMissingQuota = errors.New("original charge quota is missing")
	// ErrInvalidQuota rejects signed adjustments; refunds need separate handling.
	ErrInvalidQuota = errors.New("wallet consumption quota must be nonnegative")
	// ErrIncompletePrice means no complete official comparison is available.
	ErrIncompletePrice = errors.New("official price components are missing or invalid")
	// ErrInvalidPrecision bounds display formatting, not internal arithmetic.
	ErrInvalidPrecision = errors.New("display precision must be between 0 and 18")
	// ErrRowLimit prevents a summary counter from silently wrapping.
	ErrRowLimit  = errors.New("bill row counter limit exceeded")
	priceDecimal = regexp.MustCompile(`^[0-9]{1,32}(\.[0-9]{1,18})?$`)
)

// Decimal is an immutable, exact, nonnegative rational. Its zero value is zero.
// Copies are safe: methods never mutate the stored rational or expose its pointer.
// Missing values must be represented separately, not by a zero Decimal.
type Decimal struct {
	value *big.Rat
}

func (d Decimal) rational() *big.Rat {
	if d.value == nil {
		return new(big.Rat)
	}
	return new(big.Rat).Set(d.value)
}

// Exact returns a lossless integer or fraction, suitable for arithmetic tests and
// internal snapshots. It is not a customer-facing currency formatter.
func (d Decimal) Exact() string { return d.rational().RatString() }

// Fixed rounds only for display. Never parse its output back to compute totals.
func (d Decimal) Fixed(places int) (string, error) {
	if places < 0 || places > 18 {
		return "", ErrInvalidPrecision
	}
	return d.rational().FloatString(places), nil
}

func add(a, b Decimal) Decimal {
	return Decimal{value: new(big.Rat).Add(a.rational(), b.rational())}
}

// ActualChargeCNY converts original wallet consumption quota without multiplying
// by 7, a site/group multiplier, or an official price. nil and zero are distinct.
func ActualChargeCNY(quota *int64) (Decimal, error) {
	if quota == nil {
		return Decimal{}, ErrMissingQuota
	}
	if *quota < 0 {
		return Decimal{}, ErrInvalidQuota
	}
	return Decimal{value: new(big.Rat).SetFrac(big.NewInt(*quota), big.NewInt(quotaPerCNY))}, nil
}

// PriceComponent contains one already-normalized usage category and its verified
// official USD price per UnitTokens. Cache write variants remain separate entries.
// This type does NOT prove historical price applicability or token semantics.
type PriceComponent struct {
	Tokens       int64
	UnitTokens   int64
	UnitPriceUSD string
}

// OfficialOriginalCNY totals normalized components exactly and applies the fixed
// factor 7 once. Empty inputs and absent prices are not zero-priced comparisons.
// Only plain nonnegative decimal prices are accepted; never floats or fractions.
func OfficialOriginalCNY(components []PriceComponent) (Decimal, error) {
	if len(components) == 0 || len(components) > maxComponents {
		return Decimal{}, ErrIncompletePrice
	}
	usd := new(big.Rat)
	for i, component := range components {
		if component.Tokens < 0 || component.UnitTokens <= 0 || !priceDecimal.MatchString(component.UnitPriceUSD) {
			return Decimal{}, fmt.Errorf("component %d: %w", i, ErrIncompletePrice)
		}
		price, ok := new(big.Rat).SetString(component.UnitPriceUSD)
		if !ok {
			return Decimal{}, fmt.Errorf("component %d: %w", i, ErrIncompletePrice)
		}
		usage := new(big.Rat).SetFrac(big.NewInt(component.Tokens), big.NewInt(component.UnitTokens))
		usd.Add(usd, new(big.Rat).Mul(usage, price))
	}
	return Decimal{value: new(big.Rat).Mul(usd, big.NewRat(officialUSDToCNY, 1))}, nil
}

// DiscountFold returns the numeric Chinese discount fold (0.12 => 1.20 fold).
// A zero original has no discount; a zero actual with positive original is 0 fold.
// Values above 10 are allowed. The boolean distinguishes undefined from true zero.
func DiscountFold(actualCNY, originalCNY Decimal) (Decimal, bool) {
	original := originalCNY.rational()
	if original.Sign() == 0 {
		return Decimal{}, false
	}
	fold := new(big.Rat).Quo(actualCNY.rational(), original)
	return Decimal{value: fold.Mul(fold, big.NewRat(10, 1))}, true
}

// Summary accumulates only the verified wallet text charges supplied by its
// caller. It is single-writer; a future job must own its own instance.
// Missing-price charges remain in the total, never in the matched denominator.
type Summary struct {
	rows            uint64
	matchedRows     uint64
	actual          Decimal
	matchedActual   Decimal
	matchedOriginal Decimal
}

// Add includes an actual charge, and optionally a complete official comparison.
// originalCNY == nil means unavailable. A pointer to zero is a verified zero price.
func (s *Summary) Add(actualCNY Decimal, originalCNY *Decimal) error {
	if s.rows == math.MaxUint64 {
		return ErrRowLimit
	}
	s.rows++
	s.actual = add(s.actual, actualCNY)
	if originalCNY != nil {
		s.matchedRows++
		s.matchedActual = add(s.matchedActual, actualCNY)
		s.matchedOriginal = add(s.matchedOriginal, *originalCNY)
	}
	return nil
}

// Totals is an immutable value snapshot, not a declaration of source completeness.
// Price completeness and source/authorization completeness are separate concerns.
type Totals struct {
	Rows               uint64
	MatchedRows        uint64
	ActualCNY          Decimal
	MatchedActualCNY   Decimal
	MatchedOriginalCNY Decimal
}

// Totals returns the current values without exposing mutable arithmetic pointers.
func (s *Summary) Totals() Totals {
	return Totals{s.rows, s.matchedRows, s.actual, s.matchedActual, s.matchedOriginal}
}

// DiscountFold compares only the same matched subset, never average row discounts.
func (t Totals) DiscountFold() (Decimal, bool) {
	return DiscountFold(t.MatchedActualCNY, t.MatchedOriginalCNY)
}
