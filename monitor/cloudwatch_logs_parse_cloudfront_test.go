package monitor

import (
	"encoding/json"
	"strings"
	"testing"
)

const cloudWatchParserTestKey = "fixture-cloudwatch-evidence-key-32-bytes-minimum"

func newCloudWatchParserForTest(t *testing.T, sensitive bool) *cloudWatchEvidenceParser {
	t.Helper()
	parser, err := newCloudWatchEvidenceParser(cloudWatchParserTestKey, "fixture-v1", sensitive)
	if err != nil {
		t.Fatal(err)
	}
	return parser
}

func assertCloudWatchEvidenceOmits(t *testing.T, evidence cloudWatchStructuredEvidence, secrets ...string) {
	t.Helper()
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range secrets {
		if secret != "" && strings.Contains(string(encoded), secret) {
			t.Fatalf("structured evidence leaked %q: %s", secret, encoded)
		}
	}
}

func cloudFrontFixture(source cloudWatchLogSourceID) cloudWatchEvidenceInput {
	return cloudWatchEvidenceInput{Source: source, EventID: "event-edge-raw", LogStream: "edge-stream-raw", Message: `{
		"timestamp(ms)":1789363200123,"x-edge-request-id":"edge-request-secret",
		"x-edge-location":"SEA73-P2","cs-method":"POST","x-host-header":"us.nexusapi.link",
		"cs-uri-stem":"/v1/responses?api_key=must-not-survive","sc-status":"504",
		"x-edge-detailed-result-type":"OriginCommError","time-taken":"30.125",
		"time-to-first-byte":"29.500","origin-fbl":"29.400","origin-lbl":"30.000",
		"c-ip":"203.0.113.45","asn":"64500","c-country":"us",
		"cs(User-Agent)":"curl/8.12.1 secret-agent-tail","ssl-protocol":"TLSv1.3",
		"ssl-cipher":"TLS_AES_128_GCM_SHA256"}`}
}

func TestCloudWatchParseCloudFrontCoreUsesOnlyRedactedStructuredFields(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	evidence, err := parser.parse(cloudFrontFixture(cwSourceCloudFrontAccess))
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Kind != cwEvidenceCloudFrontAccess || evidence.EventMS != 1789363200123 || evidence.Method != "POST" || evidence.Route != "/v1/responses" || evidence.Host != "us.nexusapi.link" {
		t.Fatalf("identity fields=%+v", evidence)
	}
	if evidence.Status == nil || *evidence.Status != 504 || evidence.RequestMS == nil || *evidence.RequestMS != 30125 || evidence.FirstByteMS == nil || *evidence.FirstByteMS != 29500 {
		t.Fatalf("timing/status=%+v", evidence)
	}
	if evidence.OriginFirstByteMS == nil || *evidence.OriginFirstByteMS != 29400 || evidence.OriginLastByteMS == nil || *evidence.OriginLastByteMS != 30000 || evidence.FaultClass != "transport_timeout" {
		t.Fatalf("origin/fault=%+v", evidence)
	}
	if evidence.CloudFrontIDHMAC == "" || evidence.EventRef == "" || evidence.StreamRef == "" || len(evidence.CloudFrontIDHMAC) != 64 {
		t.Fatalf("missing HMACs: %+v", evidence)
	}
	withoutHost := cloudFrontFixture(cwSourceCloudFrontAccess)
	withoutHost.Message = strings.Replace(withoutHost.Message, `"x-host-header":"us.nexusapi.link",`, "", 1)
	missingHost, err := parser.parse(withoutHost)
	if err != nil || missingHost.Host != "" {
		t.Fatalf("missing host was not kept unknown: evidence=%+v err=%v", missingHost, err)
	}
	assertCloudWatchEvidenceOmits(t, evidence, "edge-request-secret", "event-edge-raw", "edge-stream-raw", "api_key", "203.0.113.45", "secret-agent-tail")
}

func TestCloudWatchDiagnosticIsDeniedByDefaultAndRedactedWhenAuthorized(t *testing.T) {
	input := cloudFrontFixture(cwSourceCloudFrontDiagnostic)
	if _, err := newCloudWatchParserForTest(t, false).parse(input); cwParseErrorKind(err) != cwParseSensitiveDenied {
		t.Fatalf("ordinary parser accepted diagnostic source: %v", err)
	}
	parser := newCloudWatchParserForTest(t, true)
	evidence, err := parser.parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Kind != cwEvidenceCloudFrontDiagnostic || evidence.ClientIPHMAC == "" || evidence.Country != "US" || evidence.ASN == nil || *evidence.ASN != 64500 {
		t.Fatalf("diagnostic fields=%+v", evidence)
	}
	if evidence.UserAgentFamily != "curl" || evidence.UserAgentVersion != "8.12.1" || evidence.TLSVersion != "TLSv1.3" || evidence.TLSCipher != "TLS_AES_128_GCM_SHA256" {
		t.Fatalf("diagnostic summary=%+v", evidence)
	}
	if evidence.ClientIPHMAC == evidence.CloudFrontIDHMAC {
		t.Fatal("HMAC domains were not separated")
	}
	assertCloudWatchEvidenceOmits(t, evidence, "203.0.113.45", "curl/8.12.1", "edge-request-secret")
}

func TestCloudWatchCloudFrontRejectsMissingContractAndInvalidNumbers(t *testing.T) {
	parser := newCloudWatchParserForTest(t, true)
	for _, message := range []string{`{"sc-status":"200"}`, `{"timestamp(ms)":1,"x-edge-request-id":"x","cs-method":"GET","cs-uri-stem":"/v1/models","sc-status":"700"}`, `{"timestamp(ms)":1,"x-edge-request-id":"x","cs-method":"GET","cs-uri-stem":"/v1/models","sc-status":"200","time-taken":"NaN"}`} {
		_, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceCloudFrontAccess, EventID: "e", Message: message})
		if cwParseErrorKind(err) != cwParseMalformed {
			t.Fatalf("message=%s err=%v", message, err)
		}
	}
}
