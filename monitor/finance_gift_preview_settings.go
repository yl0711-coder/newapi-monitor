package monitor

import (
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
)

func validateFinanceGiftPreviewSettings(s Settings) error {
	if s.FinanceGiftHandoffLiveExecutionEnabled && (s.LocalSnapshotOnly || s.FinanceGiftHandoffLocalExecutionEnabled || !s.FinanceGiftHandoffApprovalEnabled) {
		return errors.New("在线交接内核需要独立启用授权，不能与本地快照执行混用")
	}
	if s.FinanceGiftHandoffLocalExecutionEnabled && (!s.LocalSnapshotOnly || !s.FinanceGiftHandoffApprovalEnabled || s.ProdDSN != "" || s.NewAPIBaseURL != "") {
		return errors.New("交接执行仅允许无外部来源的本地隔离快照，且必须先启用授权")
	}
	if s.FinanceGiftHandoffApprovalEnabled && (s.FinanceGiftHandoffPreviewDir == "" || s.FinanceGiftHandoffPreviewSHA256 == "" || s.LocalAuthBypass || strings.TrimSpace(s.SessionSecret) == "") {
		return errors.New("交接授权需要固定预检配置和会话密钥，不允许免登录模式")
	}
	if s.FinanceGiftHandoffApprovalEnabled && (!filepath.IsAbs(s.StorePath) || !filepath.IsAbs(s.UsageFactsStorePath) || filepath.Clean(s.StorePath) == filepath.Clean(s.UsageFactsStorePath)) {
		return errors.New("交接授权需要主库和独立事实库的明确绝对路径")
	}
	if s.FinanceGiftHandoffPreviewDir == "" && s.FinanceGiftHandoffPreviewSHA256 == "" {
		return nil
	}
	digest, err := hex.DecodeString(s.FinanceGiftHandoffPreviewSHA256)
	if err != nil || len(digest) != 32 || strings.ToLower(s.FinanceGiftHandoffPreviewSHA256) != s.FinanceGiftHandoffPreviewSHA256 {
		return errors.New("交接只读预检需要固定的计划 SHA256")
	}
	dir := s.FinanceGiftHandoffPreviewDir
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || filepath.Dir(dir) == dir {
		return errors.New("交接只读预检需要独立任务目录的绝对路径")
	}
	if !s.FinanceEnabled || !s.FinanceFactsReadIsolationEnabled || strings.TrimSpace(s.UsageFactsHistorySourceEpoch) == "" {
		return errors.New("交接只读预检需要经营核算、独立只读事实连接和明确的来源 epoch")
	}
	return nil
}
