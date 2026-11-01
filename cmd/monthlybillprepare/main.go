// monthlybillprepare checks retained local evidence and writes a reconciliation
// draft, candidate bundle or explicitly reviewed import file. It has no
// database, network, import execution or publication configuration.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func openLocalRegular(path string) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("必须使用绝对本机文件路径")
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, errors.New("仅支持普通文件，不支持目录、管道或符号链接")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	after, err := f.Stat()
	if err != nil || !os.SameFile(before, after) || !after.Mode().IsRegular() {
		_ = f.Close()
		return nil, errors.New("本机来源文件在打开期间变化")
	}
	return f, nil
}

func verifySource(s monitor.MonthlyPreparationSource) error {
	f, err := openLocalRegular(s.Path)
	if err != nil {
		return err
	}
	defer f.Close()
	before, err := f.Stat()
	if err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return err
	}
	after, err := f.Stat()
	if err != nil {
		return err
	}
	currentPath, err := os.Lstat(s.Path)
	if err != nil {
		return err
	}
	if !currentPath.Mode().IsRegular() || !os.SameFile(after, currentPath) {
		return errors.New("来源路径在读取期间被替换")
	}
	if n != before.Size() || n != after.Size() || !before.ModTime().Equal(after.ModTime()) || hex.EncodeToString(h.Sum(nil)) != s.SHA256 {
		return errors.New("来源文件 SHA-256 不一致或读取期间发生变化")
	}
	return nil
}

func prepare(inputPath, outputPath string, now time.Time) (err error) {
	if !filepath.IsAbs(outputPath) || filepath.Clean(outputPath) != outputPath {
		return errors.New("输出必须为绝对本机路径")
	}
	if _, statErr := os.Lstat(outputPath); !os.IsNotExist(statErr) {
		return errors.New("输出路径已存在或无法检查；不会覆盖")
	}
	f, err := openLocalRegular(inputPath)
	if err != nil {
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(f, monitor.MonthlyPreparationInputLimit+1))
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	input, err := monitor.DecodeMonthlyPreparationInput(raw)
	if err != nil {
		return err
	}
	for _, s := range input.Sources {
		if err := verifySource(s); err != nil {
			return fmt.Errorf("来源 %s: %w", s.ID, err)
		}
	}
	report, err := monitor.PrepareMonthlyReconciliation(raw, now)
	if err != nil {
		return err
	}
	report.SourceIntegrity = "retained_files_sha256_verified_not_fact_attestation"
	body, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	out, err := os.OpenFile(outputPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() {
		_ = out.Close()
		if err != nil {
			_ = os.Remove(outputPath)
		} // only the incomplete file created by this call
	}()
	if _, err = out.Write(append(body, '\n')); err != nil {
		return err
	}
	if err = out.Sync(); err != nil {
		return err
	}
	err = out.Close()
	return err
}

func run(args []string, out io.Writer, now time.Time) error {
	fs := flag.NewFlagSet("monthlybillprepare", flag.ContinueOnError)
	fs.SetOutput(out)
	input := fs.String("input", "", "absolute local normalized evidence JSON")
	output := fs.String("out", "", "new absolute local report/evidence path; never overwritten")
	bundleContext := fs.String("bundle-context", "", "optional absolute local scope/event context; produces candidates, never publishes")
	reviewBundle := fs.String("review-bundle", "", "exact local candidate bundle to review; requires -review-request")
	reviewRequest := fs.String("review-request", "", "explicit local approval for ONE customer/month; never writes a database")
	history := fs.Bool("historical-preview", false, "preview an explicit historical allocation; never produces importable evidence")
	resolution := fs.Bool("resolution-plan", false, "classify per-account/month evidence issues and next actions; local only")
	historyHash := fs.String("history-input-sha256", "", "exact retained input digest for historical preview")
	historyUser := fs.Int64("history-user", 0, "one historical preview account")
	historyAnchor := fs.Int64("history-anchor", 0, "retained exclusive balance cutoff, Unix seconds")
	historyMonth := fs.String("history-month", "", "explicit proposed allocation month, YYYY-MM")
	historyGap := fs.Int64("history-gap-quota", 0, "explicit positive residual; not a tolerance")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("不接受额外参数")
	}
	var err error
	message := "本机核对草稿已生成；未写数据库、未发布账单。请逐账号查看结果，文件生成成功不代表每个月已对齐。"
	historyArgs := *historyHash != "" || *historyUser != 0 || *historyAnchor != 0 || *historyMonth != "" || *historyGap != 0
	if historyArgs && !*history {
		return errors.New("历史预览参数须显式指定 -historical-preview")
	}
	if *resolution {
		if *history || historyArgs || *bundleContext != "" || *reviewBundle != "" || *reviewRequest != "" {
			return errors.New("处理方案不得与历史预览、候选或发布审核混用")
		}
		err = prepareResolutionPlan(*input, *output, now)
		message = "本机逐账号处理方案已生成；仅供内部核验，不执行重试、调账或发布。"
	} else if *history {
		if *bundleContext != "" || *reviewBundle != "" || *reviewRequest != "" {
			return errors.New("历史预览不得与候选或发布审核混用")
		}
		err = prepareHistoricalPreview(*input, *output, monitor.MonthlyHistoricalSettlementRequest{InputSHA256: *historyHash, UserID: *historyUser, AnchorAt: *historyAnchor, SettlementMonth: *historyMonth, ExpectedGapQuota: *historyGap}, now)
		message = "历史归期预览已生成；请检查 status 和 reason。预览不代表核验通过，不可直接发布；未写数据库。"
	} else if *reviewBundle != "" || *reviewRequest != "" {
		if *reviewBundle == "" || *reviewRequest == "" || *bundleContext == "" {
			return errors.New("复核转换须同时提供 -bundle-context、-review-bundle、-review-request")
		}
		err = prepareReviewed(*input, *bundleContext, *reviewBundle, *reviewRequest, *output, now)
		message = "选定客户月份的导入文件已生成；未写数据库、未发布账单。请保留源文件和复核记录，再用原导入命令默认只读核验。"
	} else if *bundleContext == "" {
		err = prepare(*input, *output, now)
	} else {
		err = prepareBundle(*input, *bundleContext, *output, now)
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, message)
	return err
}

func main() {
	if err := run(os.Args[1:], os.Stdout, time.Now()); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
