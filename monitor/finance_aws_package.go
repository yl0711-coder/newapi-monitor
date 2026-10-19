package monitor

// Offline packaging deliberately reuses the production reader. It does not
// fetch evidence, finalize bills, change allocation, or authorize publication.
import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

type FinanceAWSPackageInput struct {
	CURArtifacts                 [][]byte
	Services, RecordTypes, Daily []byte
	FromMonth, ToMonthExclusive  string
	AccountingBasis              string
}

// BuildFinanceAWSPackage returns a self-contained, validated reader input.
// Hashes prove byte identity, not AWS authenticity or ownership. Callers must
// use previously reviewed evidence and preserve its original hashes separately.
func BuildFinanceAWSPackage(in FinanceAWSPackageInput) ([]byte, error) {
	if in.AccountingBasis != financeAWSBillingMonthBasis {
		return nil, fmt.Errorf("explicit accounting basis %q required", financeAWSBillingMonthBasis)
	}
	if len(in.CURArtifacts) == 0 || len(in.CURArtifacts) > financecur.MaxSourceSetArtifacts {
		return nil, fmt.Errorf("requires 1 to 36 individual CUR artifacts")
	}
	total := len(in.Services) + len(in.RecordTypes) + len(in.Daily)
	if total > maxFinanceCURSourceBytes {
		return nil, fmt.Errorf("package inputs exceed size limit")
	}
	var artifacts []financecur.Artifact
	for i, raw := range in.CURArtifacts {
		if len(raw) > maxFinanceCURSourceBytes-total {
			return nil, fmt.Errorf("package inputs exceed size limit")
		}
		total += len(raw)
		source, err := readFinanceCURSource(bytes.NewReader(raw))
		if err != nil {
			return nil, fmt.Errorf("CUR artifact %d: %w", i+1, err)
		}
		if source.legacy == nil {
			return nil, fmt.Errorf("CUR input %d must be an individual artifact", i+1)
		}
		artifacts = append(artifacts, *source.legacy)
	}
	set, err := financecur.NewSourceSet(artifacts)
	if err != nil {
		return nil, err
	}
	bundle := financeAWSEvidenceBundle{SchemaVersion: 1, Kind: "aws_cost_evidence_bundle", AccountingBasis: in.AccountingBasis,
		CUR: set, HistoricalCE: financeCEEvidenceInput{FromMonth: in.FromMonth, ToMonthExclusive: in.ToMonthExclusive,
			Services: in.Services, RecordTypes: in.RecordTypes, Daily: in.Daily}}
	raw, err := json.Marshal(bundle)
	if err != nil {
		return nil, err
	}
	raw = append(raw, '\n')
	if _, err := readFinanceCURSource(bytes.NewReader(raw)); err != nil {
		return nil, fmt.Errorf("runtime reader rejected package: %w", err)
	}
	return raw, nil
}
