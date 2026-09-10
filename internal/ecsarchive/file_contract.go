package ecsarchive

import (
	"errors"
	"regexp"
)

const (
	NewAPIFileContract = "newapi-files-v1"
	FinalMaxFiles      = 32
	FinalMaxBytes      = 256 << 20
)

var newAPIFileName = regexp.MustCompile(`^oneapi-[A-Za-z0-9_-]{1,112}\.log$`)

// FinalFileNameAllowed is deliberately not an arbitrary user-supplied glob.
// Empty contract retains the original signed single-file wire format.
func FinalFileNameAllowed(contract, lane, name string) bool {
	if contract == "" {
		want := map[string]string{"access": "access.jsonl", "evidence": "access.jsonl", "error": "error.log", "reject": "new-api.log"}[lane]
		return want != "" && name == want
	}
	if contract != NewAPIFileContract {
		return false
	}
	switch lane {
	case "access", "evidence":
		return name == "nexusapi_access.jsonl"
	case "error":
		return name == "error.log"
	case "reject":
		return newAPIFileName.MatchString(name)
	}
	return false
}

// ValidateFinalFiles bounds the total work and rejects duplicate identities,
// aliases and noncanonical ordering before any completeness claim is accepted.
func ValidateFinalFiles(contract, lane string, files []FinalFile) error {
	limit := 1
	if contract == NewAPIFileContract && lane == "reject" {
		limit = FinalMaxFiles
	}
	if len(files) == 0 || len(files) > limit {
		return errors.New("invalid final file count")
	}
	seen := map[[2]uint64]bool{}
	var total int64
	for i, f := range files {
		identity := [2]uint64{f.Device, f.Inode}
		if !FinalFileNameAllowed(contract, lane, f.Name) || f.Inode == 0 || seen[identity] || (i > 0 && files[i-1].Name >= f.Name) || f.Size < 0 || f.Size > FinalMaxBytes || f.Offset != f.Size || !hashPattern.MatchString(f.SHA256) {
			return errors.New("invalid final file boundary")
		}
		seen[identity] = true
		total += f.Size
		if total > FinalMaxBytes {
			return errors.New("final inventory exceeds total byte budget")
		}
	}
	return nil
}
