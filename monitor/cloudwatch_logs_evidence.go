package monitor

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	cloudWatchEvidenceMaxMessageBytes = 64 << 10
	cloudWatchEvidenceMaxFields       = 128
	cloudWatchEvidenceMaxFieldBytes   = 256 << 10
)

type cloudWatchEvidenceKind string

const (
	cwEvidenceCloudFrontAccess     cloudWatchEvidenceKind = "cloudfront_access"
	cwEvidenceCloudFrontDiagnostic cloudWatchEvidenceKind = "cloudfront_diagnostic"
	cwEvidenceNginxAccess          cloudWatchEvidenceKind = "nginx_access"
	cwEvidenceNginxError           cloudWatchEvidenceKind = "nginx_error"
	cwEvidenceNewAPIError          cloudWatchEvidenceKind = "newapi_error"
	cwEvidenceMasterError          cloudWatchEvidenceKind = "master_error"
	cwEvidenceRDSError             cloudWatchEvidenceKind = "rds_error"
	cwEvidenceRDSSlowQuery         cloudWatchEvidenceKind = "rds_slow_query"
)

type cloudWatchEvidenceParseErrorKind string

const (
	cwParseMalformed       cloudWatchEvidenceParseErrorKind = "malformed"
	cwParseUnsupported     cloudWatchEvidenceParseErrorKind = "unsupported"
	cwParseUnsafe          cloudWatchEvidenceParseErrorKind = "unsafe"
	cwParseSensitiveDenied cloudWatchEvidenceParseErrorKind = "sensitive_denied"
)

type cloudWatchEvidenceParseError struct {
	Kind   cloudWatchEvidenceParseErrorKind
	Source cloudWatchLogSourceID
}

func (e *cloudWatchEvidenceParseError) Error() string {
	return fmt.Sprintf("cloudwatch evidence %s (%s)", e.Kind, e.Source)
}

func newCloudWatchEvidenceParseError(kind cloudWatchEvidenceParseErrorKind, source cloudWatchLogSourceID) error {
	return &cloudWatchEvidenceParseError{Kind: kind, Source: source}
}

type cloudWatchStructuredEvidence struct {
	Source            cloudWatchLogSourceID  `json:"source"`
	Kind              cloudWatchEvidenceKind `json:"kind"`
	EventRef          string                 `json:"event_ref"`
	StreamRef         string                 `json:"stream_ref,omitempty"`
	EventMS           int64                  `json:"event_ms"`
	IngestedMS        int64                  `json:"ingested_ms,omitempty"`
	HMACKeyID         string                 `json:"hmac_key_id"`
	CloudFrontIDHMAC  string                 `json:"cloudfront_id_hmac,omitempty"`
	NginxIDHMAC       string                 `json:"nginx_id_hmac,omitempty"`
	OneAPIIDHMAC      string                 `json:"oneapi_id_hmac,omitempty"`
	ClientIPHMAC      string                 `json:"client_ip_hmac,omitempty"`
	Method            string                 `json:"method,omitempty"`
	Route             string                 `json:"route,omitempty"`
	Host              string                 `json:"host,omitempty"`
	Status            *int                   `json:"status,omitempty"`
	UpstreamStatuses  []int                  `json:"upstream_statuses,omitempty"`
	RequestMS         *int64                 `json:"request_ms,omitempty"`
	UpstreamMS        *int64                 `json:"upstream_ms,omitempty"`
	ConnectMS         *int64                 `json:"connect_ms,omitempty"`
	HeaderMS          *int64                 `json:"header_ms,omitempty"`
	FirstByteMS       *int64                 `json:"first_byte_ms,omitempty"`
	OriginFirstByteMS *int64                 `json:"origin_first_byte_ms,omitempty"`
	OriginLastByteMS  *int64                 `json:"origin_last_byte_ms,omitempty"`
	BytesSent         *int64                 `json:"bytes_sent,omitempty"`
	Completion        string                 `json:"completion,omitempty"`
	EdgeLocation      string                 `json:"edge_location,omitempty"`
	EdgeResult        string                 `json:"edge_result,omitempty"`
	Country           string                 `json:"country,omitempty"`
	ASN               *uint64                `json:"asn,omitempty"`
	UserAgentFamily   string                 `json:"user_agent_family,omitempty"`
	UserAgentVersion  string                 `json:"user_agent_version,omitempty"`
	TLSVersion        string                 `json:"tls_version,omitempty"`
	TLSCipher         string                 `json:"tls_cipher,omitempty"`
	Category          string                 `json:"category,omitempty"`
	Severity          string                 `json:"severity,omitempty"`
	FaultClass        string                 `json:"fault_class,omitempty"`
	Summary           string                 `json:"summary"`
	Model             string                 `json:"model,omitempty"`
	Group             string                 `json:"group,omitempty"`
	UserID            *int64                 `json:"user_id,omitempty"`
	QueryTimeMS       *int64                 `json:"query_time_ms,omitempty"`
	LockTimeMS        *int64                 `json:"lock_time_ms,omitempty"`
	RowsSent          *uint64                `json:"rows_sent,omitempty"`
	RowsExamined      *uint64                `json:"rows_examined,omitempty"`
	DBOperation       string                 `json:"db_operation,omitempty"`
	QueryHMAC         string                 `json:"query_hmac,omitempty"`
}

type cloudWatchEvidenceParseFailure struct {
	EventRef string                           `json:"event_ref"`
	Kind     cloudWatchEvidenceParseErrorKind `json:"kind"`
}

type cloudWatchEvidenceBatch struct {
	Source      cloudWatchLogSourceID            `json:"source"`
	Evidence    []cloudWatchStructuredEvidence   `json:"evidence"`
	Parsed      uint64                           `json:"parsed"`
	ParseFailed uint64                           `json:"parse_failed"`
	Failures    []cloudWatchEvidenceParseFailure `json:"failures,omitempty"`
}

type cloudWatchEvidenceInput struct {
	Source      cloudWatchLogSourceID
	EventID     string
	LogStream   string
	TimestampMS int64
	IngestedMS  int64
	Message     string
	Fields      map[string]string
}

type cloudWatchEvidenceParser struct {
	key            []byte
	keyID          string
	allowSensitive bool
}

func newCloudWatchEvidenceParser(key, keyID string, allowSensitive bool) (*cloudWatchEvidenceParser, error) {
	if len(key) < 32 || !cwValidKeyID(keyID) {
		return nil, newCloudWatchEvidenceParseError(cwParseUnsafe, "")
	}
	return &cloudWatchEvidenceParser{key: append([]byte(nil), key...), keyID: keyID, allowSensitive: allowSensitive}, nil
}

func cwValidKeyID(value string) bool {
	if len(value) < 1 || len(value) > 32 {
		return false
	}
	for i, r := range value {
		letter := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
		digit := r >= '0' && r <= '9'
		if !letter && !digit && !(i > 0 && strings.ContainsRune("._-", r)) {
			return false
		}
	}
	return true
}

func (p *cloudWatchEvidenceParser) hmac(domain, value string) string {
	value = strings.TrimSpace(value)
	if value == "" || value == "-" {
		return ""
	}
	mac := hmac.New(sha256.New, p.key)
	_, _ = mac.Write([]byte(domain))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(value))
	return hex.EncodeToString(mac.Sum(nil))
}

func (p *cloudWatchEvidenceParser) base(in cloudWatchEvidenceInput) cloudWatchStructuredEvidence {
	identity := cwEvidenceIdentity(in)
	streamRef := ""
	if stream, ok := cwBoundedOpaque(in.LogStream, 2048); ok && stream != "" {
		streamRef = p.hmac("cloudwatch-stream", string(in.Source)+"\x00"+stream)
	}
	return cloudWatchStructuredEvidence{Source: in.Source, EventRef: p.hmac("cloudwatch-event", identity), StreamRef: streamRef, EventMS: in.TimestampMS, IngestedMS: in.IngestedMS, HMACKeyID: p.keyID}
}

func cwEvidenceIdentity(in cloudWatchEvidenceInput) string {
	if eventID, ok := cwBoundedOpaque(in.EventID, 2048); ok && eventID != "" {
		return string(in.Source) + "\x00" + eventID
	}
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "%s\x00%d\x00%d\x00", in.Source, in.TimestampMS, in.IngestedMS)
	if stream, ok := cwBoundedOpaque(in.LogStream, 2048); ok {
		_, _ = hash.Write([]byte(stream))
	}
	_, _ = hash.Write([]byte{0})
	if len(in.Message) <= cloudWatchEvidenceMaxMessageBytes {
		_, _ = hash.Write([]byte(in.Message))
	} else {
		_, _ = hash.Write([]byte("oversized-message"))
	}
	if len(in.Fields) <= cloudWatchEvidenceMaxFields {
		keys := make([]string, 0, len(in.Fields))
		for key := range in.Fields {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		remaining := cloudWatchEvidenceMaxFieldBytes
		for _, key := range keys {
			value := in.Fields[key]
			if len(key)+len(value) > remaining {
				_, _ = hash.Write([]byte("\x00oversized-fields"))
				break
			}
			_, _ = hash.Write([]byte{0})
			_, _ = hash.Write([]byte(key))
			_, _ = hash.Write([]byte{0})
			_, _ = hash.Write([]byte(value))
			remaining -= len(key) + len(value)
		}
	} else {
		_, _ = hash.Write([]byte("\x00too-many-fields"))
	}
	return "derived:" + hex.EncodeToString(hash.Sum(nil))
}

func cwBoundedOpaque(raw string, maximum int) (string, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", true
	}
	if len(value) > maximum {
		return "", false
	}
	for _, r := range value {
		if r < 0x21 || r == 0x7f {
			return "", false
		}
	}
	return value, true
}

func cwParseErrorKind(err error) cloudWatchEvidenceParseErrorKind {
	// errors.As 而非类型断言：包装过的解析错误同样必须识别出原本的 kind，
	// 否则被 fmt.Errorf("%w") 包一层就会退化成 malformed。
	var parsed *cloudWatchEvidenceParseError
	if errors.As(err, &parsed) {
		return parsed.Kind
	}
	return cwParseMalformed
}

func (r *cloudWatchLogsRuntime) parseFilterEvidence(parser *cloudWatchEvidenceParser, result cloudWatchFilterResult) cloudWatchEvidenceBatch {
	batch := parser.parseFilterResult(result)
	r.recordEvidenceBatch(batch)
	return batch
}

func (r *cloudWatchLogsRuntime) parseInsightsEvidence(parser *cloudWatchEvidenceParser, result cloudWatchInsightsResult) cloudWatchEvidenceBatch {
	batch := parser.parseInsightsResult(result)
	r.recordEvidenceBatch(batch)
	return batch
}

func (r *cloudWatchLogsRuntime) recordEvidenceBatch(batch cloudWatchEvidenceBatch) {
	if r == nil {
		return
	}
	r.metricsMu.Lock()
	metric := r.metrics[batch.Source]
	metric.Observed = true
	metric.Parsed += batch.Parsed
	metric.ParseFailures += batch.ParseFailed
	if batch.ParseFailed > 0 {
		metric.LastParseFailureUnix = time.Now().Unix()
	}
	r.metrics[batch.Source] = metric
	r.metricsMu.Unlock()
}
