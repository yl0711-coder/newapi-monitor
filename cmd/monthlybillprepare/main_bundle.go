package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func readBundleLocal(path string) ([]byte, error) {
	return readPreparationLocal(path, monitor.MonthlyPreparationInputLimit)
}

func readPreparationLocal(path string, limit int64) ([]byte, error) {
	f, err := openLocalRegular(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > limit {
		return nil, fmt.Errorf("本机制单输入超过 %d 字节上限", limit)
	}
	after, err := f.Stat()
	if err != nil {
		return nil, err
	}
	current, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !current.Mode().IsRegular() || !os.SameFile(after, current) || int64(len(raw)) != before.Size() || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return nil, errors.New("制单输入在读取期间发生变化")
	}
	return raw, nil
}

// Write completely and sync before making the final name visible. Link is
// exclusive even if another process creates the requested output meanwhile;
// rename would overwrite it. Only this call's temporary file is removed.
func writeBundleExclusive(path string, raw []byte) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return errors.New("输出必须为绝对本机路径")
	}
	if len(raw) > 32<<20 {
		return errors.New("候选包超过 32MiB，请分批制单")
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".monthly-candidate-*")
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()
	if _, err := f.Write(raw); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Link(f.Name(), path)
}

func prepareBundle(inputPath, contextPath, outputPath string, now time.Time) error {
	if !filepath.IsAbs(outputPath) || filepath.Clean(outputPath) != outputPath {
		return errors.New("输出必须为绝对本机路径")
	}
	if _, err := os.Lstat(outputPath); !os.IsNotExist(err) {
		return errors.New("输出路径已存在或无法检查；不会覆盖")
	}
	inputRaw, err := readBundleLocal(inputPath)
	if err != nil {
		return err
	}
	contextRaw, err := readBundleLocal(contextPath)
	if err != nil {
		return err
	}
	input, err := monitor.DecodeMonthlyPreparationInput(inputRaw)
	if err != nil {
		return err
	}
	verify := func() error {
		for _, s := range input.Sources {
			if err := verifySource(s); err != nil {
				return fmt.Errorf("来源 %s: %w", s.ID, err)
			}
		}
		return nil
	}
	if err := verify(); err != nil {
		return err
	}
	bundle, err := monitor.PrepareMonthlyCandidateBundle(inputRaw, contextRaw, now)
	if err != nil {
		return err
	}
	// Recheck retained files after calculation too. The resulting hashes bind
	// exactly the bytes used; they are not an attestation of source completeness.
	if err := verify(); err != nil {
		return err
	}
	for _, item := range []struct {
		path string
		raw  []byte
	}{{inputPath, inputRaw}, {contextPath, contextRaw}} {
		current, err := readBundleLocal(item.path)
		if err != nil {
			return err
		}
		if !bytes.Equal(current, item.raw) {
			return errors.New("制单输入在核对期间发生变化；未生成候选包")
		}
	}
	bundle.SourceIntegrity = "retained_files_sha256_verified_not_fact_attestation"
	bundle.Reconciliation.SourceIntegrity = bundle.SourceIntegrity
	raw, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return err
	}
	return writeBundleExclusive(outputPath, append(raw, '\n'))
}
