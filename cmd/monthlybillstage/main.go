// monthlybillstage is an offline operator command, not a customer API. It
// connects only to explicit local files; it never loads production .env files.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlydraft"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	cancel()
	if err != nil {
		// Source decoding errors may contain private values. Keep public console
		// diagnostics categorical; never print raw SQL/input or credentials.
		fmt.Fprintln(os.Stderr, safeError(err))
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "prepare-retained" {
		return runPrepareRetained(ctx, args[1:], out, stderr)
	}
	if len(args) > 0 && args[0] == "plan-collection" {
		return runCollectionPlan(ctx, args[1:], out, stderr)
	}
	if len(args) > 0 && args[0] == "prepare" {
		return runPrepare(ctx, args[1:], out, stderr)
	}
	if len(args) > 0 && args[0] == "handoff" {
		return runHandoff(ctx, args[1:], out, stderr)
	}
	if len(args) > 0 && args[0] == "init" {
		return runInit(ctx, args[1:], out, stderr)
	}
	fs := flag.NewFlagSet("monthlybillstage", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var cfg monthlydraft.DeploymentConfig
	fs.StringVar(&cfg.ReceiverPath, "receiver", "", "existing private task receiver (absolute path)")
	fs.StringVar(&cfg.FundingPath, "funding", "", "retained funding database (read-only)")
	fs.StringVar(&cfg.AuthorityPath, "authority", "", "Usage permission database (read-only)")
	fs.StringVar(&cfg.EvidenceRoot, "evidence-root", "", "private retained evidence directory")
	group := fs.Int64("group-id", 0, "exact Monitor organization ID, not user ID")
	month := fs.String("month", "", "one closed Beijing calendar month YYYY-MM")
	planPath := fs.String("plan", "", "canonical machine-produced plan JSON")
	preparationPath := fs.String("preparation", "", "normalized preparation JSON")
	expected := fs.String("expected-head", "", "required explicit selection head; empty only for first version")
	apply := fs.Bool("apply", false, "retain and select input only; default read-only validation")
	if err := fs.Parse(args); err != nil {
		return err
	}
	headProvided := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "expected-head" {
			headProvided = true
		}
	})
	if fs.NArg() != 0 || !headProvided || *group <= 0 || *month == "" {
		return monthlydraft.ErrGenerationInput
	}
	plan, err := readInput(*planPath, 2<<20)
	if err != nil {
		return err
	}
	preparation, err := readInput(*preparationPath, monitor.MonthlyPreparationInputLimit)
	if err != nil {
		return err
	}
	cfg.Rollout = monthlydraft.GenerationRollout{Enabled: true, Targets: []monthlydraft.GenerationTarget{{GroupID: *group, Month: *month}}}
	result, stageErr := monthlydraft.PrepareGenerationInput(ctx, cfg, plan, preparation, *expected, *apply)
	if stageErr != nil && !result.Applied {
		return stageErr
	}
	// If a commit succeeded but cleanup failed, still emit its applied receipt.
	// A lost stdout receipt must be resolved by inspecting the receiver, not by
	// retrying with an invented/blank head.
	return errors.Join(stageErr, json.NewEncoder(out).Encode(result))
}

func runInit(ctx context.Context, args []string, out, stderr io.Writer) error {
	fs := flag.NewFlagSet("monthlybillstage init", flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("receiver", "", "NEW private receiver path; existing files are never overwritten")
	apply := fs.Bool("apply", false, "create empty receiver; default only checks path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return monthlydraft.ErrGenerationInput
	}
	result, err := monthlydraft.InitializeGenerationReceiverFile(ctx, *path, *apply)
	if err != nil && !result.Applied {
		return err
	}
	return errors.Join(err, json.NewEncoder(out).Encode(result))
}

func readInput(path string, limit int64) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, monthlydraft.ErrGenerationInput
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Size() <= 0 || before.Size() > limit {
		return nil, monthlydraft.ErrGenerationInput
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, monthlydraft.ErrGenerationInput
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) || !opened.Mode().IsRegular() {
		return nil, monthlydraft.ErrGenerationInput
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, monthlydraft.ErrGenerationInput
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(opened, after) || after.Size() != int64(len(raw)) || int64(len(raw)) > limit || !after.ModTime().Equal(opened.ModTime()) {
		return nil, monthlydraft.ErrGenerationInput
	}
	return raw, nil
}

func safeError(err error) string {
	for _, item := range []struct {
		target  error
		message string
	}{
		{monthlydraft.ErrGenerationBusy, "任务库正在使用或检查忙碌；停止并排空生成服务后再试，未自动抢锁"},
		{monthlydraft.ErrGenerationSelectionConflict, "输入选择版本已变化；重新核对 expected-head，不要强制覆盖"},
		{monthlydraft.ErrGenerationChanged, "组织权限或成员范围已变化，需重新准备输入"},
		{monthlydraft.ErrGenerationIntegrity, "来源或接收库完整性检查失败，未自动修复"},
		{monthlydraft.ErrGenerationEvidence, "任务未完成、证据或逐账号对账未通过；未批准或发布账单"},
		{monthlydraft.ErrGenerationCapacity, "数据超过本次接收容量限制，需人工检查"},
		{context.Canceled, "操作已取消；如已输出写入或导出回执，以回执及本机目标核对结果为准"},
		{context.DeadlineExceeded, "操作超时；请检查接收库当前选择后再重试"},
	} {
		if errors.Is(err, item.target) {
			return item.message
		}
	}
	return "输入或本机操作失败；检查文件、范围和回执，不自动修改来源或发布账单"
}
