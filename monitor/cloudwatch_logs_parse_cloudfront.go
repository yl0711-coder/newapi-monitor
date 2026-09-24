package monitor

import (
	"net"
	"strings"
)

func (p *cloudWatchEvidenceParser) parseCloudFront(in cloudWatchEvidenceInput) (cloudWatchStructuredEvidence, error) {
	fields, err := cwInputFields(in)
	if err != nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	eventMS, err := cwTimestampMS(in, fields, "timestamp(ms)", "")
	if err != nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	requestID, validRequestID := cwOpaqueID(cwField(fields, "x-edge-request-id"))
	method := cwField(fields, "cs-method")
	path := cwField(fields, "cs-uri-stem")
	status, err := cwStatus(cwField(fields, "sc-status"), true)
	if !validRequestID || method == "" || path == "" || err != nil || status == nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	out := p.base(in)
	out.EventMS = eventMS
	out.Method = cwMethod(method)
	out.Route = cwRoute(path)
	out.Status = status
	out.CloudFrontIDHMAC = p.hmac("cloudfront-request-id", requestID)
	out.Host = cwHost(cwField(fields, "x-host-header", "cs(Host)"))
	out.EdgeLocation = cwSafeToken(cwField(fields, "x-edge-location"), 32)
	out.EdgeResult = cwSafeToken(cwField(fields, "x-edge-detailed-result-type", "x-edge-response-result-type", "x-edge-result-type"), 96)
	out.RequestMS, err = cwDurationMS(cwField(fields, "time-taken"))
	if err != nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	out.FirstByteMS, err = cwDurationMS(cwField(fields, "time-to-first-byte"))
	if err != nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	out.OriginFirstByteMS, err = cwDurationMS(cwField(fields, "origin-fbl"))
	if err != nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	out.OriginLastByteMS, err = cwDurationMS(cwField(fields, "origin-lbl"))
	if err != nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	out.FaultClass = cwHTTPFault(*status, out.EdgeResult)
	out.Summary = cwHTTPSummary("CloudFront", *status)
	if in.Source == cwSourceCloudFrontDiagnostic {
		out.Kind = cwEvidenceCloudFrontDiagnostic
		if err := p.addCloudFrontDiagnostic(&out, fields); err != nil {
			return cloudWatchStructuredEvidence{}, err
		}
	} else {
		out.Kind = cwEvidenceCloudFrontAccess
	}
	return out, nil
}

func (p *cloudWatchEvidenceParser) addCloudFrontDiagnostic(out *cloudWatchStructuredEvidence, fields map[string]string) error {
	rawIP := cwField(fields, "c-ip")
	ip := net.ParseIP(strings.TrimSpace(rawIP))
	if ip == nil {
		return newCloudWatchEvidenceParseError(cwParseMalformed, cwSourceCloudFrontDiagnostic)
	}
	out.ClientIPHMAC = p.hmac("client-ip", ip.String())
	asn, present, err := cwParseUint64(cwField(fields, "asn"), 4294967295)
	if err != nil {
		return newCloudWatchEvidenceParseError(cwParseMalformed, cwSourceCloudFrontDiagnostic)
	}
	if present {
		out.ASN = cwUint64Pointer(asn)
	}
	country := strings.ToUpper(cwField(fields, "c-country"))
	if len(country) == 2 && country[0] >= 'A' && country[0] <= 'Z' && country[1] >= 'A' && country[1] <= 'Z' {
		out.Country = country
	}
	out.UserAgentFamily, out.UserAgentVersion = cwUserAgent(cwField(fields, "cs(User-Agent)"))
	out.TLSVersion = cwSafeToken(cwField(fields, "ssl-protocol"), 32)
	out.TLSCipher = cwSafeToken(cwField(fields, "ssl-cipher"), 128)
	if out.ClientIPHMAC == "" {
		return newCloudWatchEvidenceParseError(cwParseMalformed, cwSourceCloudFrontDiagnostic)
	}
	return nil
}
