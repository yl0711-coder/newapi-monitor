// Package ecslogagent provides the default-off, isolated ECS identity adapter.
// It transports frozen collector payloads; it never parses/recounts log records.
package ecslogagent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

type Config struct {
	Scope, Kind, ServiceARN, Container, MonitorURL, RegisterURL, Audience, StateRoot string
	DeferredArchiveACK                                                               bool   `json:",omitempty"`
	ArchiveClosure                                                                   bool   `json:",omitempty"`
	FinalLogRoot                                                                     string `json:",omitempty"`
	FinalFileContract                                                                string `json:",omitempty"`
}

type Metadata struct{ TaskARN, RuntimeID string }

var servicePattern = regexp.MustCompile(`^arn:aws:ecs:([a-z]{2}-[a-z]+-[0-9]):([0-9]{12}):service/([A-Za-z0-9_-]+)/([A-Za-z0-9_-]+)$`)
var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
var taskIDPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)
var audiencePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:./_-]{7,127}$`)

func (c Config) Validate() error {
	if c.FinalFileContract != "" && (c.FinalFileContract != ecsarchive.NewAPIFileContract || c.FinalLogRoot == "") {
		return errors.New("unsupported final file contract or missing log root")
	}
	if c.FinalLogRoot != "" && (!c.ArchiveClosure || !filepath.IsAbs(c.FinalLogRoot) || filepath.Clean(c.FinalLogRoot) != c.FinalLogRoot || c.FinalLogRoot == "/") {
		return errors.New("final boundary requires closure and a dedicated absolute log directory")
	}
	if c.ArchiveClosure && !c.DeferredArchiveACK {
		return errors.New("collector closure requires deferred archive delivery")
	}
	p := servicePattern.FindStringSubmatch(c.ServiceARN)
	if c.Scope != "isolated" || (c.Kind != "nginx" && c.Kind != "reject") || len(p) == 0 || !namePattern.MatchString(c.Container) || !audiencePattern.MatchString(c.Audience) || !filepath.IsAbs(c.StateRoot) || filepath.Clean(c.StateRoot) == "/" {
		return errors.New("invalid isolated ECS agent configuration")
	}
	monitor, err := strictHTTPS(c.MonitorURL)
	if err != nil || (monitor.Path != "" && monitor.Path != "/") {
		return errors.New("Monitor URL must be a fixed HTTPS origin")
	}
	registration, err := strictHTTPS(c.RegisterURL)
	if err != nil || !strings.HasSuffix(registration.Hostname(), ".execute-api."+p[1]+".amazonaws.com") || registration.Port() != "" || registration.Path != "/isolated/register" {
		return errors.New("registration requires the regional AWS_IAM Gateway isolated/register endpoint")
	}
	return nil
}

func strictHTTPS(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" {
		return nil, errors.New("invalid HTTPS endpoint")
	}
	return u, nil
}

func (c Config) lanes() []string {
	if c.Kind == "reject" {
		return []string{"reject"}
	}
	return []string{"access", "error", "evidence"}
}

func (c Config) validateMetadata(meta Metadata) error {
	p := servicePattern.FindStringSubmatch(c.ServiceARN)
	if len(p) == 0 {
		return errors.New("invalid service")
	}
	prefix := "arn:aws:ecs:" + p[1] + ":" + p[2] + ":task/" + p[3] + "/"
	if !strings.HasPrefix(meta.TaskARN, prefix) || !taskIDPattern.MatchString(strings.TrimPrefix(meta.TaskARN, prefix)) || meta.RuntimeID == "" || len(meta.RuntimeID) > 256 {
		return errors.New("task metadata does not match configured service scope")
	}
	return nil
}

func sourceNode(meta Metadata, container string) string {
	b, _ := json.Marshal([]string{"ecs-log-source-v1", meta.TaskARN, container, meta.RuntimeID})
	h := sha256.Sum256(b)
	return "ecs-" + hex.EncodeToString(h[:24])
}

func localLane(path string) string {
	switch path {
	case "/internal/nginx":
		return "access"
	case "/internal/nginx-errors":
		return "error"
	case "/internal/nginx-evidence/v1":
		return "evidence"
	case "/internal/rejections/v2":
		return "reject"
	}
	return ""
}
