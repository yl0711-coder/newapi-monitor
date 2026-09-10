package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
	"github.com/yl0711-coder/newapi-monitor/internal/ecslogagent"
)

func configureArchive(ctx context.Context, agent *ecslogagent.Agent, c ecslogagent.Config, meta ecslogagent.Metadata, cfg aws.Config) error {
	enabled := os.Getenv("ECSLOG_ARCHIVE_ENABLED")
	if enabled == "" || enabled == "false" {
		return nil
	}
	if enabled != "true" {
		return errors.New("invalid ECSLOG_ARCHIVE_ENABLED value")
	}
	parts := strings.Split(c.ServiceARN, ":")
	if len(parts) != 6 {
		return errors.New("invalid archive service identity")
	}
	archiveCfg := ecsarchive.Config{Bucket: os.Getenv("ECSLOG_ARCHIVE_BUCKET"), Prefix: os.Getenv("ECSLOG_ARCHIVE_PREFIX"), Account: parts[4], Region: parts[3]}
	if err := archiveCfg.Validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	cfg.HTTPClient = &http.Client{Transport: transport, Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	client := sts.NewFromConfig(cfg, func(o *sts.Options) {
		o.Region = archiveCfg.Region
		o.BaseEndpoint = aws.String("https://sts." + archiveCfg.Region + ".amazonaws.com")
		o.RetryMaxAttempts = 2
	})
	identity, err := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil || identity == nil {
		return errors.New("archive task identity verification unavailable")
	}
	if aws.ToString(identity.Account) != archiveCfg.Account || !strings.HasPrefix(aws.ToString(identity.Arn), "arn:aws:sts::"+archiveCfg.Account+":assumed-role/") {
		return errors.New("archive requires same-account task role")
	}
	owner := aws.ToString(identity.UserId)
	if task := ecsarchive.OwnerTask(owner); task == "" || !strings.HasSuffix(meta.TaskARN, "/"+task) || !strings.HasSuffix(aws.ToString(identity.Arn), "/"+task) {
		return errors.New("archive caller session is not this ECS task")
	}
	store, err := ecsarchive.NewS3(archiveCfg, cfg)
	if err != nil {
		return err
	}
	return agent.ConfigureArchive(store, archiveCfg.Prefix, owner)
}
