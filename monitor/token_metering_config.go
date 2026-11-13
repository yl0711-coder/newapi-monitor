package monitor

import (
	"encoding/json"
	"errors"
	"regexp"
	"strings"
)

const (
	tokenMeteringParserVersion       = "token-facts-v10"
	tokenMeteringWindowSeconds int64 = 300
	tokenMeteringLagSeconds    int64 = 600
	// Lag seals a coverage certificate; it never delays observing committed rows.
	tokenMeteringTailPollSeconds   int64 = 10
	tokenMeteringTailOverlap       int64 = 60
	tokenMeteringRetentionSeconds  int64 = 7 * 86400
	tokenMeteringRepairSeconds     int64 = 72 * 3600
	tokenMeteringPageRows                = 256
	tokenMeteringPageBytes               = 2 << 20
	tokenMeteringSourceExecutionMS       = 3000
	tokenMeteringOtherBytes              = 64 << 10
	tokenMeteringMaxLinkAttempts         = 24
)

func tokenMeteringEffectiveAdapter(s Settings) string {
	// A complete source proves collection coverage, not the producer version,
	// price unit or usage semantics. Only the explicitly configured adapter
	// may supply an adapter contract; an epoch's name is never provenance.
	if s.TokenFactsUsageAdapter == "" {
		return "unknown"
	}
	return s.TokenFactsUsageAdapter
}

var tokenMeteringName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$`)

type tokenMeteringKey struct{ ID, Secret string }

func (s Settings) tokenMeteringKeys() ([]tokenMeteringKey, error) {
	profiles := []tokenMeteringKey{
		{s.NginxEvidenceHMACKeyID, s.NginxEvidenceHMACKey},
		{s.NginxEvidencePreviousHMACKeyID, s.NginxEvidencePreviousHMACKey},
		{s.CloudWatchEvidenceHMACKeyID, s.CloudWatchEvidenceHMACKey},
	}
	seen := map[string]string{}
	result := []tokenMeteringKey{}
	for _, p := range profiles {
		if p.Secret == "" && p.ID == "" {
			continue
		}
		// Default key IDs may exist while a source is unconfigured.
		if p.Secret == "" {
			continue
		}
		if len(p.Secret) < 32 || !nginxEvidenceKeyIDPattern.MatchString(p.ID) {
			return nil, errors.New("token facts HMAC profile invalid")
		}
		if old, ok := seen[p.ID]; ok {
			if old != p.Secret {
				return nil, errors.New("token facts HMAC key_id has incompatible secrets")
			}
			continue
		}
		seen[p.ID] = p.Secret
		result = append(result, p)
	}
	return result, nil
}
func (s Settings) tokenMeteringBindings() (map[string]string, error) {
	out := map[string]string{}
	if strings.TrimSpace(s.TokenFactsSemanticBindings) == "" {
		return out, nil
	}
	if len(s.TokenFactsSemanticBindings) > 4096 || json.Unmarshal([]byte(s.TokenFactsSemanticBindings), &out) != nil {
		return nil, errors.New("token facts semantic bindings invalid")
	}
	for k, v := range out {
		if !tokenMeteringName.MatchString(k) || v != "anthropic_native" && v != "openai_inclusive" {
			return nil, errors.New("token facts semantic binding unsupported")
		}
	}
	return out, nil
}
func validateTokenMeteringSettings(s Settings) error {
	if !s.TokenFactsEnabled {
		return nil
	}
	if err := validateTokenProcessingResources(s); err != nil {
		return err
	}
	if !tokenMeteringName.MatchString(s.TokenFactsSourceEpoch) {
		return errors.New("token facts requires an explicit stable source_epoch")
	}
	if s.TokenFactsRetentionDays != 0 && (s.TokenFactsRetentionDays < 4 || s.TokenFactsRetentionDays > 7) {
		return errors.New("token facts retention must be 4..7 days")
	}
	if s.TokenFactsInitialLookbackSeconds != 0 && (s.TokenFactsInitialLookbackSeconds < 900 || int64(s.TokenFactsInitialLookbackSeconds) > s.tokenMeteringRetentionSeconds() || s.TokenFactsInitialLookbackSeconds%300 != 0) {
		return errors.New("token facts initial lookback must be at least 900 seconds, aligned to five minutes and within retention")
	}
	if s.TokenFactsSourceMode != "" && s.TokenFactsSourceMode != "unverified" && s.TokenFactsSourceMode != "complete" {
		return errors.New("token facts source mode must be unverified or complete")
	}
	if s.TokenFactsUsageAdapter != "" && s.TokenFactsUsageAdapter != "unknown" && s.TokenFactsUsageAdapter != "newapi-rc26" && s.TokenFactsUsageAdapter != "newapi-rc48" {
		return errors.New("token facts usage adapter unsupported")
	}
	if _, err := s.tokenMeteringBindings(); err != nil {
		return err
	}
	keys, err := s.tokenMeteringKeys()
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return errors.New("token facts requires a versioned evidence HMAC key")
	}
	if s.TokenFactsMaxRows < 0 || s.TokenFactsMaxBytes < 0 {
		return errors.New("token facts resource limit invalid")
	}
	return nil
}
func (s Settings) tokenMeteringLimits() (int64, int64) {
	rows, bytes := s.TokenFactsMaxRows, s.TokenFactsMaxBytes
	if rows == 0 {
		rows = 1_000_000
	}
	if bytes == 0 {
		bytes = 512 << 20
	}
	return rows, bytes
}
func (s Settings) tokenMeteringRetentionSeconds() int64 {
	if s.TokenFactsRetentionDays == 0 {
		return tokenMeteringRetentionSeconds
	}
	// Retention also runs when collection is disabled. Never let an invalid
	// legacy setting bypass validation and remove the required four days.
	return int64(max(4, s.TokenFactsRetentionDays)) * 86400
}

// Pin key IDs across restarts. Reusing a version label for another secret must
// stop collection before a source read, rather than silently orphan old links.
func (s Settings) tokenMeteringPinnedKeys(old string) (string, error) {
	profiles := map[string]string{}
	if len(old) > 32768 || old != "" && json.Unmarshal([]byte(old), &profiles) != nil {
		return "", errors.New("token facts HMAC history invalid")
	}
	keys, err := s.tokenMeteringKeys()
	if err != nil {
		return "", err
	}
	for _, key := range keys {
		fingerprint := nginxEvidenceIDHMAC(key.Secret, "token-facts-key-profile-v1", key.ID)
		if prior, exists := profiles[key.ID]; exists && prior != fingerprint {
			return "", errors.New("token facts HMAC version reused")
		}
		profiles[key.ID] = fingerprint
	}
	encoded, err := json.Marshal(profiles)
	return string(encoded), err
}
