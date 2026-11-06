package monitor

// Versioned immutable payload, using the existing bill table. Publication and
// customer readers retain the source and cent view separately. An old binary
// must reject this envelope rather than interpret it as its old cent view.
import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

const monthlyCentBillFormat = "monthly-cent-bill-v1"
const monthlyCentBillPublicationPrefix = monthlyCentBillFormat + ":"
const monthlyCentBillPayloadLimit = 2 << 20

var errMonthlyCentBillStorage = errors.New("invalid retained monthly cent bill")

type monthlyCentBillEnvelope struct {
	Format       string          `json:"format"`
	Source       json.RawMessage `json:"source"`
	Presentation string          `json:"presentation"`
}

type monthlyRetainedCentBill struct {
	Source monthlyStatement
	Plan   monthlyBillCentMonth
}

// The trusted publication transaction must additionally bind the prepared
// series, source evidence, owner and authorization. This codec cannot authorize
// publication and never changes an existing record.
func monthlyEncodeCentBill(source monthlyStatement, plan monthlyBillCentMonth) ([]byte, error) {
	source.Publication = ""
	if !monthlyBillCentMatchesSource(source, plan) {
		return nil, errMonthlyCentBillStorage
	}
	rawSource, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}
	rawPlan, err := json.Marshal(plan)
	if err != nil || len(rawPlan) > monthlyCentBillPayloadLimit {
		return nil, errMonthlyCentBillStorage
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(rawPlan); err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(monthlyCentBillEnvelope{Format: monthlyCentBillFormat, Source: rawSource,
		Presentation: base64.RawStdEncoding.EncodeToString(compressed.Bytes())})
	if err != nil {
		return nil, err
	}
	if len(payload) > monthlyCentBillPayloadLimit {
		return nil, errMonthlyCentBillStorage
	}
	return payload, nil
}

// Canonical round-trip rejects unknown/duplicate keys and noncanonical encodings
// in this internal versioned format. No client-supplied JSON enters this path.
func monthlyCentCanonicalJSON(raw []byte, target any) bool {
	if json.Unmarshal(raw, target) != nil {
		return false
	}
	canonical, err := json.Marshal(target)
	return err == nil && bytes.Equal(canonical, raw)
}

func monthlyReadCentBill(record PortalMonthlyBill) (monthlyRetainedCentBill, error) {
	var empty monthlyRetainedCentBill
	if record.GroupID <= 0 || len(record.Payload) > monthlyCentBillPayloadLimit ||
		monthlyDigest([]byte(record.Payload)) != record.PayloadHash ||
		!strings.HasPrefix(record.PublicationID, monthlyCentBillPublicationPrefix) ||
		len(record.PublicationID) <= len(monthlyCentBillPublicationPrefix) ||
		!validMonthlyPublication(record.MemberManifest) || record.MemberFingerprint == "" {
		return empty, errMonthlyCentBillStorage
	}
	var envelope monthlyCentBillEnvelope
	if !monthlyCentCanonicalJSON([]byte(record.Payload), &envelope) || envelope.Format != monthlyCentBillFormat {
		return empty, errMonthlyCentBillStorage
	}
	var source monthlyStatement
	if !monthlyCentCanonicalJSON(envelope.Source, &source) || source.Month != record.Month || source.Publication != "" {
		return empty, errMonthlyCentBillStorage
	}
	compressed, err := base64.RawStdEncoding.Strict().DecodeString(envelope.Presentation)
	if err != nil || base64.RawStdEncoding.EncodeToString(compressed) != envelope.Presentation {
		return empty, errMonthlyCentBillStorage
	}
	input := bytes.NewReader(compressed)
	reader, err := gzip.NewReader(input)
	if err != nil {
		return empty, errMonthlyCentBillStorage
	}
	reader.Multistream(false)
	rawPlan, readErr := io.ReadAll(io.LimitReader(reader, monthlyCentBillPayloadLimit+1))
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || len(rawPlan) > monthlyCentBillPayloadLimit || input.Len() != 0 {
		return empty, errMonthlyCentBillStorage
	}
	var plan monthlyBillCentMonth
	if !monthlyCentCanonicalJSON(rawPlan, &plan) || plan.GroupID != record.GroupID || !monthlyBillCentMatchesSource(source, plan) {
		return empty, errMonthlyCentBillStorage
	}
	ids := make([]int64, len(source.Members))
	for i, member := range source.Members {
		ids[i] = member.UserID
	}
	if portalMemberFingerprintFromIDs(ids) != record.MemberFingerprint {
		return empty, errMonthlyCentBillStorage
	}
	return monthlyRetainedCentBill{Source: source, Plan: plan}, nil
}
