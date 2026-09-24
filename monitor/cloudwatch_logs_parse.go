package monitor

import (
	"encoding/json"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func validCloudWatchEvidenceInput(in cloudWatchEvidenceInput) bool {
	if in.TimestampMS < 0 || in.IngestedMS < 0 || len(in.Message) > cloudWatchEvidenceMaxMessageBytes || !utf8.ValidString(in.Message) {
		return false
	}
	if _, ok := cwBoundedOpaque(in.EventID, 2048); !ok {
		return false
	}
	if _, ok := cwBoundedOpaque(in.LogStream, 2048); !ok {
		return false
	}
	if len(in.Fields) > cloudWatchEvidenceMaxFields {
		return false
	}
	total := 0
	for key, value := range in.Fields {
		if key == "" || len(key) > 256 || !utf8.ValidString(key) || len(value) > cloudWatchEvidenceMaxMessageBytes || !utf8.ValidString(value) {
			return false
		}
		for _, r := range key {
			if r < 0x20 || r == 0x7f {
				return false
			}
		}
		total += len(key) + len(value)
		if total > cloudWatchEvidenceMaxFieldBytes {
			return false
		}
	}
	return true
}

func (p *cloudWatchEvidenceParser) parse(in cloudWatchEvidenceInput) (cloudWatchStructuredEvidence, error) {
	source, err := cloudWatchLogSourceByID(in.Source)
	if err != nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseUnsupported, in.Source)
	}
	if source.Sensitive && !p.allowSensitive {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseSensitiveDenied, in.Source)
	}
	if !validCloudWatchEvidenceInput(in) {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseUnsafe, in.Source)
	}
	switch in.Source {
	case cwSourceCloudFrontAccess, cwSourceCloudFrontDiagnostic:
		return p.parseCloudFront(in)
	case cwSourceWorkerNginx:
		return p.parseNginx(in)
	case cwSourceWorkerNewAPI, cwSourceMaster:
		return p.parseNewAPI(in)
	case cwSourceRDSError:
		return p.parseRDSError(in)
	case cwSourceRDSSlowQuery:
		return p.parseRDSSlowQuery(in)
	default:
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseUnsupported, in.Source)
	}
}

func (p *cloudWatchEvidenceParser) parseFilterResult(result cloudWatchFilterResult) cloudWatchEvidenceBatch {
	batch := cloudWatchEvidenceBatch{Source: result.Source, Evidence: make([]cloudWatchStructuredEvidence, 0, len(result.Events))}
	for _, event := range result.Events {
		in := cloudWatchEvidenceInput{Source: result.Source, EventID: event.EventID, LogStream: event.LogStream, TimestampMS: event.TimestampMS, IngestedMS: event.IngestedMS, Message: event.Message}
		p.appendEvidence(&batch, in)
	}
	return batch
}

func (p *cloudWatchEvidenceParser) parseInsightsResult(result cloudWatchInsightsResult) cloudWatchEvidenceBatch {
	batch := cloudWatchEvidenceBatch{Source: result.Source, Evidence: make([]cloudWatchStructuredEvidence, 0, len(result.Rows))}
	for _, fields := range result.Rows {
		at, _ := parseCloudWatchResultTime(cwField(fields, "@timestamp"))
		in := cloudWatchEvidenceInput{Source: result.Source, EventID: cwField(fields, "@ptr"), LogStream: cwField(fields, "@logStream"), Message: cwField(fields, "@message"), Fields: fields}
		if !at.IsZero() {
			in.TimestampMS = at.UnixMilli()
		}
		p.appendEvidence(&batch, in)
	}
	return batch
}

func (p *cloudWatchEvidenceParser) appendEvidence(batch *cloudWatchEvidenceBatch, in cloudWatchEvidenceInput) {
	evidence, err := p.parse(in)
	if err == nil {
		batch.Evidence = append(batch.Evidence, evidence)
		batch.Parsed++
		return
	}
	batch.ParseFailed++
	batch.Failures = append(batch.Failures, cloudWatchEvidenceParseFailure{EventRef: p.base(in).EventRef, Kind: cwParseErrorKind(err)})
}

func cwJSONFields(message string) (map[string]string, error) {
	decoder := json.NewDecoder(strings.NewReader(strings.TrimSpace(message)))
	decoder.UseNumber()
	var raw map[string]any
	if decoder.Decode(&raw) != nil || len(raw) == 0 {
		return nil, errors.New("invalid json object")
	}
	var trailing any
	if !errors.Is(decoder.Decode(&trailing), io.EOF) {
		return nil, errors.New("trailing json")
	}
	out := make(map[string]string, len(raw))
	for key, value := range raw {
		switch v := value.(type) {
		case string:
			out[key] = v
		case json.Number:
			out[key] = v.String()
		case bool:
			out[key] = strconv.FormatBool(v)
		case nil:
		default:
			continue
		}
	}
	return out, nil
}

func cwInputFields(in cloudWatchEvidenceInput) (map[string]string, error) {
	if in.Fields != nil {
		return in.Fields, nil
	}
	return cwJSONFields(in.Message)
}

func cwField(fields map[string]string, names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(fields[name]); value != "" {
			return value
		}
	}
	return ""
}

func cwParseInt64(value string, minimum, maximum int64) (int64, bool, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "-" {
		return 0, false, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, false, errors.New("invalid integer")
	}
	return parsed, true, nil
}

func cwParseUint64(value string, maximum uint64) (uint64, bool, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "-" {
		return 0, false, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || parsed > maximum {
		return 0, false, errors.New("invalid unsigned integer")
	}
	return parsed, true, nil
}

func cwParseFloat(value string, minimum, maximum float64) (float64, bool, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "-" {
		return 0, false, nil
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) || parsed < minimum || parsed > maximum {
		return 0, false, errors.New("invalid number")
	}
	return parsed, true, nil
}

func cwDurationMS(value string) (*int64, error) {
	parts := strings.FieldsFunc(strings.TrimSpace(value), func(r rune) bool { return r == ',' || r == ':' })
	var final *int64
	if len(parts) > 8 {
		return nil, errors.New("too many duration values")
	}
	for _, part := range parts {
		seconds, present, err := cwParseFloat(part, 0, 86400)
		if err != nil {
			return nil, err
		}
		if present {
			value := int64(math.Round(seconds * 1000))
			final = &value
		}
	}
	return final, nil
}

func cwStatuses(value string) ([]int, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "-" {
		return nil, nil
	}
	parts := strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ':' })
	if len(parts) < 1 || len(parts) > 8 {
		return nil, errors.New("invalid status sequence")
	}
	out := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "-" {
			continue
		}
		status, err := strconv.Atoi(part)
		if err != nil || status < 100 || status > 599 {
			return nil, errors.New("invalid status")
		}
		out = append(out, status)
	}
	return out, nil
}

func cwStatus(value string, allowZero bool) (*int, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "-" {
		return nil, nil
	}
	status, err := strconv.Atoi(value)
	if err != nil || !(status >= 100 && status <= 599 || allowZero && status == 0) {
		return nil, errors.New("invalid status")
	}
	return &status, nil
}

func cwTimestampMS(in cloudWatchEvidenceInput, fields map[string]string, epochMSField, epochSecondsField string) (int64, error) {
	if raw := cwField(fields, epochMSField); raw != "" {
		value, present, err := cwParseInt64(raw, 1, math.MaxInt64)
		if err != nil {
			return 0, err
		}
		if present {
			return value, nil
		}
	}
	if raw := cwField(fields, epochSecondsField); raw != "" {
		value, present, err := cwParseFloat(raw, 0.001, float64(math.MaxInt64)/1000)
		if err != nil {
			return 0, err
		}
		if present {
			return int64(math.Round(value * 1000)), nil
		}
	}
	if in.TimestampMS > 0 {
		return in.TimestampMS, nil
	}
	if at, ok := parseCloudWatchResultTime(cwField(fields, "@timestamp")); ok {
		return at.UnixMilli(), nil
	}
	return 0, errors.New("missing timestamp")
}

func cwInt64Pointer(value int64) *int64    { return &value }
func cwUint64Pointer(value uint64) *uint64 { return &value }

func cwParseLogTime(value string, layouts ...string) (int64, bool) {
	for _, layout := range layouts {
		if at, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return at.UnixMilli(), true
		}
	}
	return 0, false
}
