package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

type AutomaticDisplaySeriesIndex struct {
	Version int                           `json:"version"`
	Ranges  []AutomaticDisplaySeriesRange `json:"ranges"`
}

// Opt-in service composition. Reads one bounded private server range index;
// starts no work, creates no schemas, and never reads customer-selected range
// parameters. Registered mode permits an empty index; its actual source identity
// and per-request bounds must be installed by the validated capture/calendar path.
func OpenRetainedAutomaticDisplaySeriesDeployment(ctx context.Context, cfg DeploymentConfig, sourceID, indexName string, publish AutomaticDisplaySeriesPublisher) (*Deployment, error) {
	if cfg.Rollout.Enabled && (!validScopeSourceID(sourceID) || !preparationInboxName(indexName) || publish == nil) {
		return nil, ErrGenerationInput
	}
	return openDeploymentWithAutomatic(ctx, cfg, false, &automaticDeploymentDependencies{sourceID: sourceID, seriesIndex: indexName, publishSeries: publish})
}

func readAutomaticDisplaySeriesIndex(ctx context.Context, g *Generation, name string, cfg GenerationRollout) ([]AutomaticDisplaySeriesRange, error) {
	if !preparationInboxName(name) {
		return nil, ErrGenerationInput
	}
	info, err := g.root.Lstat(name)
	if err != nil || !privateAutomaticDisplayFile(info) {
		return nil, ErrGenerationIntegrity
	}
	raw, err := readArchiveFile(ctx, g.root, name, 64<<10)
	if err != nil {
		return nil, err
	}
	var index AutomaticDisplaySeriesIndex
	if json.Unmarshal(raw, &index) != nil || index.Version != 1 || index.Ranges == nil {
		return nil, ErrGenerationInput
	}
	canonical, err := json.Marshal(index)
	if err != nil || !bytes.Equal(bytes.TrimSpace(raw), canonical) {
		return nil, ErrGenerationInput
	}
	allowed := map[int64]bool{}
	for _, target := range cfg.Targets {
		allowed[target.GroupID] = true
	}
	byGroup := map[int64]AutomaticDisplaySeriesRange{}
	for _, scope := range index.Ranges {
		if (cfg.Policy != GenerationPolicyUsageRegistered && !allowed[scope.GroupID]) || byGroup[scope.GroupID].GroupID != 0 {
			return nil, ErrGenerationInput
		}
		byGroup[scope.GroupID] = scope
	}
	for _, target := range cfg.Targets {
		scope, ok := byGroup[target.GroupID]
		if !ok || target.Month < scope.From || target.Month > scope.Through {
			return nil, ErrGenerationInput
		}
	}
	return index.Ranges, ctx.Err()
}

func newAutomaticDisplaySeriesRuntime(g *Generation, auth *gorm.DB, cfg GenerationRollout, metadata AutomaticContext, ranges []AutomaticDisplaySeriesRange, publish AutomaticDisplaySeriesPublisher) (*LocalRuntime, error) {
	scope, err := rolloutScope(cfg, time.Now())
	if err != nil || !cfg.Enabled {
		return nil, err
	}
	w, err := newAutomaticDisplaySeriesLocalWorker(g, metadata, ranges, publish, cfg.Policy == GenerationPolicyUsageRegistered, auth)
	if err != nil {
		return nil, err
	}
	r, err := newRuntimeWithWorker(g, auth, w)
	if err != nil {
		return nil, err
	}
	r.scope, w.scope = scope, scope
	return r, nil
}

var _ monitor.MonthlyGenerationService = (*Deployment)(nil)
