package monitor

import (
	"context"
	"errors"
	"log/slog"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

const ecsArchivePollInterval = 10 * time.Second

func ecsArchiveConfig(s Settings) ecsarchive.Config {
	return ecsarchive.Config{Bucket: s.ECSArchiveBucket, Prefix: s.ECSArchivePrefix, Region: s.AWSRegion, Account: s.ECSArchiveAccount}
}
func validateECSArchiveSettings(s Settings) error {
	if !s.ECSArchiveEnabled {
		return nil
	}
	if !ecsLogRuntimeEnabled(s) || s.LocalSnapshotOnly || s.LocalAuthBypass {
		return errors.New("archive recovery requires an authenticated ECS receiver")
	}
	if err := ecsArchiveConfig(s).Validate(); err != nil {
		return err
	}
	if s.ECSArchivePrefix != s.ECSLogScope+"/" {
		return errors.New("archive prefix must be dedicated to the authenticated ECS scope")
	}
	if s.NginxRetentionDays > 0 && s.NginxRetentionDays < 2 {
		return errors.New("archive replay requires at least 48 hours of nginx receipts")
	}
	policies, err := parseECSLogPolicies(s)
	if err != nil {
		return err
	}
	for _, p := range policies {
		parts := ecsLogARNPattern.FindStringSubmatch(p.ServiceARN)
		if len(parts) == 0 || parts[2] != s.ECSArchiveAccount {
			return errors.New("archive owner must match authorized ECS account")
		}
	}
	return nil
}

// Read-only AWS calls and writes to Monitor's own recovery ledger only.
func (m *Monitor) startECSArchiveRecovery(parent context.Context) {
	if !m.cfg.ECSArchiveEnabled || validateECSArchiveSettings(m.cfg) != nil {
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
	ticker := time.NewTicker(ecsArchivePollInterval)
	defer ticker.Stop()
	r := newECSArchiveReplayer(m, m.cfg.ECSArchivePrefix)
	var reader ecsarchive.Reader
	for ctx.Err() == nil {
		if reader == nil {
			setup, done := context.WithTimeout(ctx, 8*time.Second)
			cfg, err := awsconfig.LoadDefaultConfig(setup, awsconfig.WithRegion(m.cfg.AWSRegion), awsconfig.WithRetryMaxAttempts(2))
			done()
			if err == nil {
				reader, err = ecsarchive.NewS3(ecsArchiveConfig(m.cfg), cfg)
			}
			if err != nil {
				slog.Warn("ECS archive reader initialization failed; replay pending")
			}
		}
		if reader != nil {
			poll, done := context.WithTimeout(ctx, 20*time.Second)
			err := r.scan(poll, reader, ecsArchiveConfig(m.cfg).Account+"/"+ecsArchiveConfig(m.cfg).Region+"/"+ecsArchiveConfig(m.cfg).Bucket+"/"+m.cfg.ECSArchivePrefix, time.Now())
			done()
			if err != nil {
				slog.Warn("ECS archive recovery incomplete; original objects retained", "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
