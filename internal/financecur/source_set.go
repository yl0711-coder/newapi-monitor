package financecur

// SourceSet is an offline handoff of independently verified monthly sources.
// It does not allocate money, merge day buckets, or approve publication. In
// particular, adjacent UTC months may both contribute to one Shanghai day.
import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
)

const SourceSetSchemaVersion = 1
const MaxSourceSetArtifacts = 36
const maxSourceSetBytes = 16 << 20

type SourceSet struct {
	SchemaVersion int        `json:"schema_version"`
	Kind          string     `json:"kind"`
	Artifacts     []Artifact `json:"artifacts"`
}

type SourceInterval struct {
	FromUnix int64 `json:"from_unix"`
	ToUnix   int64 `json:"to_unix"`
}

type SourceSetReview struct {
	SourceSetID string      `json:"source_set_id"`
	Currency    string      `json:"currency"`
	TimeZone    string      `json:"time_zone"`
	Statements  []Statement `json:"statements"`
	// Gaps cover only the span between the first and last supplied periods.
	// Earlier/later missing history must be checked against the requested range.
	Gaps []SourceInterval `json:"gaps"`
}

func NewSourceSet(artifacts []Artifact) (SourceSet, error) {
	set := SourceSet{SchemaVersion: SourceSetSchemaVersion, Kind: "cur_source_set", Artifacts: append([]Artifact(nil), artifacts...)}
	if _, err := ReviewSourceSet(set); err != nil {
		return SourceSet{}, err
	}
	sort.Slice(set.Artifacts, func(i, j int) bool {
		return set.Artifacts[i].Statement.BillingPeriodStart < set.Artifacts[j].Statement.BillingPeriodStart
	})
	return set, nil
}

// ReviewSourceSet never silently chooses one revision of the same month.
// Callers must replace that month's artifact explicitly before validation.
func ReviewSourceSet(set SourceSet) (SourceSetReview, error) {
	if set.SchemaVersion != SourceSetSchemaVersion || set.Kind != "cur_source_set" {
		return SourceSetReview{}, errors.New("unsupported CUR source set")
	}
	if len(set.Artifacts) == 0 || len(set.Artifacts) > MaxSourceSetArtifacts {
		return SourceSetReview{}, errors.New("CUR source set must contain 1 to 36 artifacts")
	}
	review := SourceSetReview{Gaps: []SourceInterval{}}
	for i, artifact := range set.Artifacts {
		if err := VerifyArtifact(artifact); err != nil {
			return SourceSetReview{}, fmt.Errorf("CUR source %d: %w", i, err)
		}
		s := artifact.Statement
		if i == 0 {
			review.Currency, review.TimeZone = s.Currency, s.TimeZone
		} else if s.Currency != review.Currency || s.TimeZone != review.TimeZone {
			return SourceSetReview{}, errors.New("CUR sources have different currencies or timezones")
		}
		review.Statements = append(review.Statements, s)
	}
	sort.Slice(review.Statements, func(i, j int) bool {
		return review.Statements[i].BillingPeriodStart < review.Statements[j].BillingPeriodStart
	})
	ids := make([]string, 0, len(review.Statements))
	for i, s := range review.Statements {
		if i > 0 {
			previous := review.Statements[i-1]
			if s.BillingPeriodStart < previous.BillingPeriodEnd {
				return SourceSetReview{}, errors.New("CUR billing periods overlap or contain multiple revisions")
			}
			if s.BillingPeriodStart > previous.BillingPeriodEnd {
				review.Gaps = append(review.Gaps, SourceInterval{previous.BillingPeriodEnd, s.BillingPeriodStart})
			}
		}
		ids = append(ids, s.StatementID)
	}
	encoded, err := json.Marshal(struct {
		Version int      `json:"version"`
		IDs     []string `json:"statement_ids"`
	}{SourceSetSchemaVersion, ids})
	if err != nil {
		return SourceSetReview{}, err
	}
	digest := sha256.Sum256(encoded)
	review.SourceSetID = hex.EncodeToString(digest[:])
	return review, nil
}

// MissingPeriods reports absent billing files, not source completeness inside
// a present file. Draft/finalized and allocation flags remain per statement.
func (review SourceSetReview) MissingPeriods(from, to int64) ([]SourceInterval, error) {
	if from <= 0 || from >= to {
		return nil, errors.New("invalid CUR requested range")
	}
	missing := []SourceInterval{}
	cursor := from
	for _, s := range review.Statements {
		if s.BillingPeriodEnd <= cursor || s.BillingPeriodStart >= to {
			continue
		}
		if s.BillingPeriodStart > cursor {
			missing = append(missing, SourceInterval{cursor, s.BillingPeriodStart})
		}
		cursor = min(to, s.BillingPeriodEnd)
	}
	if cursor < to {
		missing = append(missing, SourceInterval{cursor, to})
	}
	return missing, nil
}

func ReadSourceSet(reader io.Reader) (SourceSet, error) {
	if reader == nil {
		return SourceSet{}, errors.New("CUR source set reader is nil")
	}
	limited := &io.LimitedReader{R: reader, N: maxSourceSetBytes + 1}
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	var set SourceSet
	if err := decoder.Decode(&set); err != nil {
		return SourceSet{}, fmt.Errorf("decode CUR source set: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return SourceSet{}, errors.New("CUR source set contains invalid trailing data")
	}
	if limited.N == 0 {
		return SourceSet{}, errors.New("CUR source set exceeds size limit")
	}
	if _, err := ReviewSourceSet(set); err != nil {
		return SourceSet{}, err
	}
	return set, nil
}

func EncodeSourceSet(writer io.Writer, set SourceSet) error {
	if writer == nil {
		return errors.New("CUR source set writer is nil")
	}
	if _, err := ReviewSourceSet(set); err != nil {
		return err
	}
	encoded, err := json.Marshal(set)
	if err != nil {
		return err
	}
	if len(encoded)+1 > maxSourceSetBytes {
		return errors.New("CUR source set exceeds size limit")
	}
	encoded = append(encoded, '\n')
	n, err := writer.Write(encoded)
	if err == nil && n != len(encoded) {
		return io.ErrShortWrite
	}
	return err
}
