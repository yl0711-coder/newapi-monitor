// ecslogagent wraps an existing collector; no deployment happens by building it.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/yl0711-coder/newapi-monitor/internal/ecslogagent"
)

func main() {
	if err := run(); err != nil {
		slog.Error("ECS collector agent stopped; persistent state retained", "err", err)
		os.Exit(1)
	}
}

func run() error {
	c := ecslogagent.Config{Scope: os.Getenv("ECSLOG_SCOPE"), Kind: os.Getenv("ECSLOG_KIND"), ServiceARN: os.Getenv("ECSLOG_SERVICE_ARN"), Container: os.Getenv("ECSLOG_PRODUCER_CONTAINER"), MonitorURL: os.Getenv("ECSLOG_MONITOR_URL"), RegisterURL: os.Getenv("ECSLOG_REGISTER_URL"), Audience: os.Getenv("ECSLOG_AUDIENCE"), StateRoot: os.Getenv("ECSLOG_STATE_ROOT")}
	c.FinalLogRoot = os.Getenv("ECSLOG_FINAL_LOG_ROOT")
	c.FinalFileContract = os.Getenv("ECSLOG_FINAL_FILE_CONTRACT")
	if raw := os.Getenv("ECSLOG_DEFERRED_ARCHIVE_ACK"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("invalid ECSLOG_DEFERRED_ARCHIVE_ACK: %w", err)
		}
		c.DeferredArchiveACK = value
	}
	if raw := os.Getenv("ECSLOG_ARCHIVE_CLOSURE"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("invalid ECSLOG_ARCHIVE_CLOSURE: %w", err)
		}
		c.ArchiveClosure = value
	}
	if err := c.Validate(); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	meta, err := ecslogagent.WaitMetadata(ctx, os.Getenv("ECS_CONTAINER_METADATA_URI_V4"), c.Container)
	if err != nil {
		return err
	}
	setup, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	cfg, err := awsconfig.LoadDefaultConfig(setup)
	if err != nil {
		return err
	}
	agent, err := ecslogagent.New(c, meta, cfg.Credentials, nil)
	if err != nil {
		return err
	}
	defer agent.Close()
	if c.FinalLogRoot != "" {
		agent.ConfigureProducerStopCheck(func(ctx context.Context) error {
			return ecslogagent.CheckProducerStopped(ctx, os.Getenv("ECS_CONTAINER_METADATA_URI_V4"), c.Container, meta)
		})
	}
	if err := configureArchive(ctx, agent, c, meta, cfg); err != nil {
		return err
	}
	argv := os.Args[1:]
	if len(argv) > 0 && argv[0] == "--" {
		argv = argv[1:]
	}
	slog.Info("isolated ECS collector agent started", "node", agent.Node(), "kind", c.Kind)
	return agent.RunChild(ctx, argv)
}
