package main

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlydraft"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

type monthlyServiceConfig struct {
	monthlydraft.DeploymentConfig
	mode, scopeSource, displaySeriesIndex string
	preparationConfig, preparationSHA     string
	coldDirectory                         string
}

var monthlyScopeSourceID = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
var monthlyPreparationSHA = regexp.MustCompile(`^[0-9a-f]{64}$`)

func monthlyGenerationConfig(getenv func(string) string, s monitor.Settings) (monthlyServiceConfig, error) {
	var cfg monthlyServiceConfig
	switch getenv("MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED") {
	case "", "false":
		return cfg, nil // disabling never opens/validates stale paths
	case "true":
	default:
		return cfg, errors.New("MONITOR_PORTAL_MONTHLY_GENERATION_ENABLED must be true or false")
	}
	if s.PortalAddr == "" || !s.PortalMonthlyStatementEnabled {
		return cfg, errors.New("monthly generation requires the Usage listener and monthly statements")
	}
	cfg.mode = getenv("MONITOR_PORTAL_MONTHLY_GENERATION_MODE")
	if cfg.mode == "simple" {
		// Simple generator: read replica + verified usage facts, published via
		// the existing validated path. Other monthly generation settings are
		// ignored so switching back to a previous mode needs no other edits.
		return cfg, nil
	}
	cfg.scopeSource = getenv("MONITOR_PORTAL_MONTHLY_GENERATION_SCOPE_SOURCE")
	cfg.displaySeriesIndex = getenv("MONITOR_PORTAL_MONTHLY_GENERATION_DISPLAY_SERIES_INDEX")
	if cfg.displaySeriesIndex != "" && cfg.mode != "automatic-series" {
		return cfg, errors.New("monthly display series index requires explicit automatic-series mode")
	}
	switch cfg.mode {
	case "", "draft":
		cfg.mode = "draft"
		if cfg.scopeSource != "" {
			return cfg, errors.New("monthly generation scope source requires explicit automatic mode")
		}
	case "automatic", "automatic-series":
		if !monthlyScopeSourceID.MatchString(cfg.scopeSource) {
			return cfg, errors.New("automatic monthly generation requires a canonical scope source ID")
		}
		if cfg.mode == "automatic-series" && (!monthlyScopeSourceID.MatchString(cfg.displaySeriesIndex) || strings.HasPrefix(cfg.displaySeriesIndex, ".")) {
			return cfg, errors.New("automatic-series mode requires a private range index basename")
		}
	default:
		return cfg, errors.New("monthly generation mode must be draft, automatic or automatic-series")
	}
	raw := getenv("MONITOR_PORTAL_MONTHLY_GENERATION_TARGETS")
	cfg.Rollout.Policy = getenv("MONITOR_PORTAL_MONTHLY_GENERATION_POLICY")
	registered := cfg.Rollout.Policy == monthlydraft.GenerationPolicyUsageRegistered
	if cfg.Rollout.Policy != "" && cfg.Rollout.Policy != "exact" && !registered {
		return cfg, errors.New("monthly generation policy must be exact or usage-registered")
	}
	if registered && raw != "" {
		return cfg, errors.New("usage-registered policy cannot combine with exact group:month targets")
	}
	if !registered && (len(raw) == 0 || len(raw) > 2048) {
		return cfg, errors.New("monthly generation requires explicit group:YYYY-MM targets")
	}
	cfg.Rollout.Enabled = true
	parts := strings.Split(raw, ",")
	if registered {
		parts = nil
	}
	for _, part := range parts {
		pair := strings.Split(strings.TrimSpace(part), ":")
		if len(pair) != 2 {
			return cfg, errors.New("invalid monthly generation target")
		}
		id, err := strconv.ParseInt(pair[0], 10, 64)
		if err != nil || id <= 0 || strconv.FormatInt(id, 10) != pair[0] {
			return cfg, errors.New("invalid monthly generation group ID")
		}
		cfg.Rollout.Targets = append(cfg.Rollout.Targets, monthlydraft.GenerationTarget{GroupID: id, Month: pair[1]})
	}
	cfg.ReceiverPath = getenv("MONITOR_PORTAL_MONTHLY_GENERATION_RECEIVER")
	cfg.FundingPath = getenv("MONITOR_PORTAL_MONTHLY_GENERATION_FUNDING")
	cfg.EvidenceRoot = getenv("MONITOR_PORTAL_MONTHLY_GENERATION_EVIDENCE")
	cfg.PreparationInbox = getenv("MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_INBOX")
	if cfg.PreparationInbox != "" && cfg.mode != "automatic" && cfg.mode != "automatic-series" {
		return cfg, errors.New("monthly preparation inbox requires explicit retained automatic mode")
	}
	cfg.preparationConfig = getenv("MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_CONFIG")
	cfg.preparationSHA = getenv("MONITOR_PORTAL_MONTHLY_GENERATION_PREPARATION_SHA256")
	if cfg.preparationConfig != "" || cfg.preparationSHA != "" {
		if cfg.mode != "automatic-series" || cfg.PreparationInbox != "" || s.LocalSnapshotOnly ||
			!monthlyScopeSourceID.MatchString(cfg.preparationConfig) || strings.HasPrefix(cfg.preparationConfig, ".") ||
			!monthlyPreparationSHA.MatchString(cfg.preparationSHA) {
			return cfg, errors.New("captured monthly preparation requires explicit automatic-series mode, private config basename and SHA256; cannot combine with inbox or local snapshot mode")
		}
	}
	if registered && (cfg.mode != "automatic-series" || cfg.preparationConfig == "" || cfg.preparationSHA == "" || cfg.PreparationInbox != "" || s.LocalSnapshotOnly) {
		return cfg, errors.New("usage-registered eligibility requires explicit captured automatic-series preparation and real registration source")
	}
	cfg.coldDirectory = getenv("MONITOR_PORTAL_MONTHLY_GENERATION_COLD_DIRECTORY")
	if cfg.coldDirectory != "" {
		if cfg.mode != "automatic-series" || cfg.preparationConfig == "" || cfg.preparationSHA == "" || cfg.PreparationInbox != "" || s.LocalSnapshotOnly ||
			len(cfg.coldDirectory) > 4096 || strings.TrimSpace(cfg.coldDirectory) != cfg.coldDirectory || !filepath.IsAbs(cfg.coldDirectory) || filepath.Clean(cfg.coldDirectory) != cfg.coldDirectory || cfg.coldDirectory == string(filepath.Separator) {
			return cfg, errors.New("monthly cold maintenance requires explicit captured automatic-series preparation and a canonical private absolute directory")
		}
	}
	var err error
	cfg.AuthorityPath, err = filepath.Abs(s.StorePath)
	return cfg, err
}

// The service owns publication through its existing store connection. The
// generation deployment owns only its separate source/receiver connections.
// Opening either mode starts no workers and publishes nothing.
func openMonthlyGeneration(ctx context.Context, cfg monthlyServiceConfig, m *monitor.Monitor) (*monthlydraft.Deployment, error) {
	if cfg.Rollout.Enabled && cfg.Rollout.Policy == monthlydraft.GenerationPolicyUsageRegistered && (cfg.mode != "automatic-series" || cfg.preparationConfig == "" || cfg.preparationSHA == "" || cfg.PreparationInbox != "") {
		return nil, monthlydraft.ErrGenerationInput
	}
	if cfg.Rollout.Enabled && cfg.coldDirectory != "" && (cfg.mode != "automatic-series" || cfg.preparationConfig == "" || cfg.preparationSHA == "" || cfg.PreparationInbox != "") {
		return nil, monthlydraft.ErrGenerationInput
	}
	d, err := openMonthlyGenerationDeployment(ctx, cfg, m)
	if err != nil || d == nil || cfg.preparationConfig == "" {
		return d, err
	}
	if err := d.EnableCapturedSourcePreparation(ctx, cfg.scopeSource, monthlysource.MonthlyObservationFile{
		Path: filepath.Join(cfg.EvidenceRoot, cfg.preparationConfig), SHA256: cfg.preparationSHA,
	}); err != nil {
		closeMonthlyGeneration(d)
		return nil, err
	}
	if cfg.coldDirectory != "" {
		if err := d.EnablePublishedCapacityMaintenance(ctx, cfg.coldDirectory); err != nil {
			closeMonthlyGeneration(d)
			return nil, err
		}
	}
	return d, nil
}

func openMonthlyGenerationDeployment(ctx context.Context, cfg monthlyServiceConfig, m *monitor.Monitor) (*monthlydraft.Deployment, error) {
	if !cfg.Rollout.Enabled {
		return nil, nil
	}
	switch cfg.mode {
	case "", "draft":
		if cfg.scopeSource != "" {
			return nil, errors.New("monthly generation scope source requires explicit automatic mode")
		}
		return monthlydraft.OpenDeployment(ctx, cfg.DeploymentConfig)
	case "automatic":
		if m == nil || !monthlyScopeSourceID.MatchString(cfg.scopeSource) {
			return nil, errors.New("automatic monthly generation dependencies are missing")
		}
		return monthlydraft.OpenRetainedAutomaticDeployment(ctx, cfg.DeploymentConfig, cfg.scopeSource,
			func(ctx context.Context, req monthlytasks.Request, raw []byte) error {
				_, err := m.PublishMonthlyBillForAuthorization(ctx, raw, monitor.MonthlyBillExpectedAuthorization{
					GroupID: req.Owner.GroupID, AuthVersion: req.Owner.AuthVersion,
					MemberFingerprint: req.Owner.MemberFingerprint, MemberManifest: req.Owner.MemberManifest,
				})
				return err
			})
	case "automatic-series":
		if m == nil || !monthlyScopeSourceID.MatchString(cfg.scopeSource) {
			return nil, errors.New("automatic series generation dependencies are missing")
		}
		return monthlydraft.OpenRetainedAutomaticDisplaySeriesDeployment(ctx, cfg.DeploymentConfig, cfg.scopeSource, cfg.displaySeriesIndex,
			func(ctx context.Context, req monthlytasks.Request, raw []byte, prepared *monitor.PreparedMonthlyDisplayPublication) error {
				_, err := m.PublishMonthlyBillWithDisplaySeries(ctx, raw, monitor.MonthlyBillExpectedAuthorization{GroupID: req.Owner.GroupID, AuthVersion: req.Owner.AuthVersion, MemberFingerprint: req.Owner.MemberFingerprint, MemberManifest: req.Owner.MemberManifest}, prepared)
				return err
			})
	default:
		return nil, errors.New("invalid monthly generation mode")
	}
}

// Never close databases after an incomplete drain. The supervisor may still
// terminate the process, but we do not race library Close against active work.
func closeMonthlyGeneration(d *monthlydraft.Deployment) {
	if d == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err := d.Close(ctx)
	cancel()
	if err != nil {
		slog.Error("月度账单生成仍在退出，保持数据库连接等待", "err", err)
		if err := d.Close(context.Background()); err != nil {
			slog.Error("关闭月度账单生成资源失败", "err", err)
		}
	}
}
