package financegiftexport

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
)

// ReadLargeHourPlan bounds an explicit local plan before any source connection.
func ReadLargeHourPlan(path string) (LargeHourPlan, string, error) {
	var plan LargeHourPlan
	info, err := os.Lstat(path)
	if err != nil {
		return plan, "", err
	}
	if !info.Mode().IsRegular() || info.Size() > giftScopeExportBytes {
		return plan, "", errors.New("read plan must be a bounded regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return plan, "", err
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return plan, "", errors.New("read plan changed while opening")
	}
	data, err := io.ReadAll(io.LimitReader(f, giftScopeExportBytes+1))
	if err != nil || len(data) > giftScopeExportBytes {
		return plan, "", errors.New("read plan exceeds byte budget or is unreadable")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&plan); err != nil {
		return plan, "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return plan, "", errors.New("read plan has trailing content")
	}
	digest, err := LargeHourConfirmation(plan)
	return plan, digest, err
}

// WriteLargeHourPlan stores sensitive ledger metadata privately and never
// overwrites an existing path. CLI stdout only needs the hash and row count.
func WriteLargeHourPlan(path string, plan LargeHourPlan) (string, error) {
	digest, err := LargeHourConfirmation(plan)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	if err := publishGiftScopeEvidence(path, data); err != nil {
		return "", err
	}
	return digest, nil
}
