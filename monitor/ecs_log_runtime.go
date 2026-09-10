package monitor

import (
	"context"
	"log/slog"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
)

const ecsLogDiscoveryInterval = 30 * time.Second

// Separate from CloudWatch resource sampling. This loop performs control-plane
// reads only; it never changes ECS desiredCount, task definitions or traffic.
func (m *Monitor) startECSLogDiscovery(parent context.Context) {
	if !ecsLogRuntimeEnabled(m.cfg) || m.cfg.LocalSnapshotOnly {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	go func() {
		select {
		case <-m.shutdownSignal():
			cancel()
		case <-ctx.Done():
		}
	}()
	ticker := time.NewTicker(ecsLogDiscoveryInterval)
	defer ticker.Stop()
	var client *ecs.Client
	for {
		if ctx.Err() != nil {
			return
		}
		if client == nil {
			setup, done := context.WithTimeout(ctx, 8*time.Second)
			cfg, err := awsconfig.LoadDefaultConfig(setup, awsconfig.WithRegion(m.cfg.AWSRegion), awsconfig.WithRetryMaxAttempts(2))
			done()
			if err != nil {
				slog.Warn("ECS log discovery client unavailable", "err", err)
				for _, p := range m.ecsLogPolicies {
					if recordErr := m.recordECSLogDiscoveryFailure(ctx, p.ServiceARN, time.Now().Unix()); recordErr != nil {
						slog.Warn("ECS log discovery failure could not be recorded", "err", recordErr)
					}
				}
			} else {
				client = ecs.NewFromConfig(cfg)
			}
		}
		if client != nil {
			for _, p := range m.ecsLogPolicies {
				if ctx.Err() != nil {
					return
				}
				scan, done := context.WithTimeout(ctx, 8*time.Second)
				err := m.reconcileECSLogService(scan, client, p, time.Now().Unix())
				done()
				if err != nil {
					slog.Warn("ECS log discovery incomplete; prior sources preserved", "service", p.ServiceARN, "err", err)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
