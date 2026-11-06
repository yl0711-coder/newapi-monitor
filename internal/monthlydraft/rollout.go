package monthlydraft

import (
	"context"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"gorm.io/gorm"
)

// GenerationTarget is an exact server-side rollout boundary, not authority or
// customer input. Existing session/member/source checks still apply inside it.
type GenerationTarget struct {
	GroupID int64
	Month   string
}

const GenerationPolicyUsageRegistered = "usage-registered"

// Zero/default remains disabled or exact-target rollout. Usage-registered is
// an explicit eligibility policy, NOT evidence or permission to publish money.
type GenerationRollout struct {
	Enabled bool
	Targets []GenerationTarget
	Policy  string
}

type generationScope struct {
	exact      map[GenerationTarget]struct{}
	registered bool
	now        func() time.Time
	// Installed from the real Usage+registration adapter before Start. Never
	// supplied by HTTP or inferred from a selected financial input.
	authorize func(context.Context, monthlytasks.Request) error
}

func (s *generationScope) allows(gid int64, month string) bool {
	if s == nil {
		return true // legacy explicit local-only constructor
	}
	if s.registered {
		if gid <= 0 || s.now == nil {
			return false
		}
		zone := time.FixedZone("Usage-UTC+8", 8*3600)
		start, err := time.ParseInLocation("2006-01", month, zone)
		return err == nil && start.Format("2006-01") == month && start.Year() >= 2020 && !start.AddDate(0, 1, 0).After(s.now())
	}
	_, ok := s.exact[GenerationTarget{GroupID: gid, Month: month}]
	return ok
}

func rolloutScope(cfg GenerationRollout, now time.Time) (*generationScope, error) {
	if !cfg.Enabled {
		if len(cfg.Targets) != 0 || cfg.Policy != "" {
			return nil, ErrGenerationInput
		}
		return nil, nil
	}
	if cfg.Policy == GenerationPolicyUsageRegistered {
		if len(cfg.Targets) != 0 {
			return nil, ErrGenerationInput
		}
		return &generationScope{registered: true, now: time.Now}, nil
	}
	if cfg.Policy != "" && cfg.Policy != "exact" {
		return nil, ErrGenerationInput
	}
	if len(cfg.Targets) == 0 || len(cfg.Targets) > 32 {
		return nil, ErrGenerationInput
	}
	scope := &generationScope{exact: make(map[GenerationTarget]struct{}, len(cfg.Targets))}
	zone := time.FixedZone("Asia/Shanghai", 8*60*60)
	for _, target := range cfg.Targets {
		start, err := time.ParseInLocation("2006-01", target.Month, zone)
		if target.GroupID <= 0 || err != nil || start.Format("2006-01") != target.Month || start.Year() < 2020 || start.AddDate(0, 1, 0).After(now) {
			return nil, ErrGenerationInput
		}
		if _, exists := scope.exact[target]; exists {
			return nil, ErrGenerationInput
		}
		scope.exact[target] = struct{}{}
	}
	return scope, nil
}

// NewControlledLocalRuntime does nothing when disabled (nil, nil), even with
// nil dependencies. Exact mode requires an immutable target allowlist. The
// registered policy cannot Start until its real registration gate is installed.
// Scope changes require stopping/draining the old instance and constructing a
// new one; callers must not run old and new rollout instances concurrently.
func NewControlledLocalRuntime(g *Generation, authorization *gorm.DB, cfg GenerationRollout) (*LocalRuntime, error) {
	scope, err := rolloutScope(cfg, time.Now())
	if err != nil || !cfg.Enabled {
		return nil, err
	}
	r, err := NewLocalRuntime(g, authorization)
	if err != nil {
		return nil, err
	}
	r.scope, r.worker.scope = scope, scope
	return r, nil
}
