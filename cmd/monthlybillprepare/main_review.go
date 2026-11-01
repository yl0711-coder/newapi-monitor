package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func prepareReviewed(inputPath, contextPath, bundlePath, reviewPath, outputPath string, now time.Time) error {
	if !filepath.IsAbs(outputPath) || filepath.Clean(outputPath) != outputPath {
		return errors.New("输出必须为绝对本机路径")
	}
	if _, err := os.Lstat(outputPath); !os.IsNotExist(err) {
		return errors.New("输出路径已存在或无法检查；不会覆盖")
	}
	inputs := []struct {
		path  string
		limit int64
		raw   []byte
	}{
		{path: inputPath, limit: monitor.MonthlyPreparationInputLimit},
		{path: contextPath, limit: monitor.MonthlyPreparationInputLimit},
		{path: bundlePath, limit: monitor.MonthlyCandidateBundleLimit},
		{path: reviewPath, limit: monitor.MonthlyReviewRequestLimit},
	}
	for i := range inputs {
		raw, err := readPreparationLocal(inputs[i].path, inputs[i].limit)
		if err != nil {
			return err
		}
		inputs[i].raw = raw
	}
	input, err := monitor.DecodeMonthlyPreparationInput(inputs[0].raw)
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
	evidence, err := monitor.ReviewMonthlyCandidate(inputs[0].raw, inputs[1].raw, inputs[2].raw, inputs[3].raw, now)
	if err != nil {
		return err
	}
	if err := verify(); err != nil {
		return err
	}
	for _, input := range inputs {
		current, err := readPreparationLocal(input.path, input.limit)
		if err != nil {
			return err
		}
		if !bytes.Equal(current, input.raw) {
			return errors.New("来源或复核文件在生成期间发生变化；未输出导入文件")
		}
	}
	return writeBundleExclusive(outputPath, evidence)
}
