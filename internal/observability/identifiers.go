package observability

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	utcTimestampPattern = `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|\+00:00)$`
	hmacHashPattern     = `^[0-9a-f]{64}$`
	hmacKeyIDPattern    = `^[^\s\x00-\x1f\x7f]+$`
)

var (
	utcTimestampRE = regexp.MustCompile(utcTimestampPattern)
	hmacHashRE     = regexp.MustCompile(hmacHashPattern)
	hmacKeyIDRE    = regexp.MustCompile(hmacKeyIDPattern)
)

// ValidateTimestamp accepts RFC 3339 UTC (Z or explicit +00:00). RFC 3339's
// -00:00 means an unknown local offset, not a proven UTC observation.
func ValidateTimestamp(value string) error {
	if !utcTimestampRE.MatchString(value) {
		return fmt.Errorf("observability: timestamp must be RFC 3339 UTC")
	}
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		return fmt.Errorf("observability: invalid RFC 3339 timestamp")
	}
	return nil
}

// HMACReference is Monitor's existing lowercase SHA-256 HMAC representation.
// The contract requires a versioned HMAC and key_id; the digest encoding here
// is the Monitor implementation profile, not a new cross-system algorithm rule.
// Shape validation cannot prove that a producer used HMAC: callers must obtain
// references from trusted HMAC computation, never forward an arbitrary raw ID.
type HMACReference struct {
	Hash  string `json:"hash"`
	KeyID string `json:"key_id"`
}

func (r HMACReference) Validate() error {
	if !hmacHashRE.MatchString(r.Hash) {
		return fmt.Errorf("observability: reference requires a SHA-256 HMAC digest")
	}
	if !hmacKeyIDRE.MatchString(r.KeyID) || strings.TrimSpace(r.KeyID) != r.KeyID {
		return fmt.Errorf("observability: reference requires a non-empty key_id without whitespace")
	}
	return nil
}

// UnmarshalJSON fails closed for accidental raw-ID fields instead of silently
// discarding them. New optional fields in this sensitive reference profile must
// be explicitly reviewed before acceptance.
func (r *HMACReference) UnmarshalJSON(data []byte) error {
	type wireReference HMACReference
	var decoded wireReference
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("observability: invalid HMAC reference object")
	}
	validated := HMACReference(decoded)
	if err := validated.Validate(); err != nil {
		return err
	}
	*r = validated
	return nil
}

func (r HMACReference) MarshalJSON() ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	type wireReference HMACReference
	return json.Marshal(wireReference(r))
}
