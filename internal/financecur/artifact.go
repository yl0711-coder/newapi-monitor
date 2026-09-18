package financecur

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const ArtifactSchemaVersion = 2
const maxArtifactBytes = 4 << 20

// Artifact is the only CUR object intended to cross into Monitor. It excludes
// source paths, AWS credentials, raw resource identifiers, and allocation
// rules; those remain in the private review workspace and are committed only
// by their hashes.
type Artifact struct {
	SchemaVersion int       `json:"schema_version"`
	Statement     Statement `json:"statement"`
	Timeline      Timeline  `json:"timeline"`
}

func NewArtifact(statement Statement, timeline Timeline) (Artifact, error) {
	artifact := Artifact{SchemaVersion: ArtifactSchemaVersion, Statement: statement, Timeline: timeline}
	if err := VerifyArtifact(artifact); err != nil {
		return Artifact{}, err
	}
	return artifact, nil
}

func VerifyArtifact(artifact Artifact) error {
	if artifact.SchemaVersion != ArtifactSchemaVersion {
		return fmt.Errorf("unsupported CUR artifact schema version %d", artifact.SchemaVersion)
	}
	return VerifyStatement(artifact.Statement, artifact.Timeline)
}

func ReadArtifact(reader io.Reader) (Artifact, error) {
	if reader == nil {
		return Artifact{}, errors.New("CUR artifact reader is nil")
	}
	decoder := json.NewDecoder(io.LimitReader(reader, maxArtifactBytes+1))
	decoder.DisallowUnknownFields()
	var artifact Artifact
	if err := decoder.Decode(&artifact); err != nil {
		return Artifact{}, fmt.Errorf("decode CUR artifact: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Artifact{}, errors.New("CUR artifact contains trailing JSON")
		}
		return Artifact{}, fmt.Errorf("decode CUR artifact trailer: %w", err)
	}
	if err := VerifyArtifact(artifact); err != nil {
		return Artifact{}, err
	}
	return artifact, nil
}

func EncodeArtifact(writer io.Writer, artifact Artifact) error {
	if writer == nil {
		return errors.New("CUR artifact writer is nil")
	}
	if err := VerifyArtifact(artifact); err != nil {
		return err
	}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(artifact); err != nil {
		return fmt.Errorf("encode CUR artifact: %w", err)
	}
	return nil
}
