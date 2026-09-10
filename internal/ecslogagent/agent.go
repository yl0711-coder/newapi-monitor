package ecslogagent

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

type Agent struct {
	cfg              Config
	meta             Metadata
	node, dir, token string
	key              ed25519.PrivateKey
	lock             *os.File
	client           *http.Client
	credentials      aws.CredentialsProvider
	mu               sync.RWMutex
	leases           map[string]int64
	inflight         atomic.Int32
	now              func() time.Time
	archive          ecsarchive.Writer
	archivePrefix    string
	archiveOwner     string
	archiveMu        sync.Mutex
	producerStopped  func(context.Context) error
}

// Credentials are supplied by the SDK task-role chain in the command. Tests
// inject synthetic credentials and a local-only transport, not real AWS keys.
func New(c Config, meta Metadata, credentials aws.CredentialsProvider, client *http.Client) (*Agent, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if err := c.validateMetadata(meta); err != nil {
		return nil, err
	}
	if credentials == nil {
		return nil, errors.New("task credentials provider required")
	}
	identity, dir, lock, err := openIdentity(c, meta)
	if err != nil {
		return nil, err
	}
	key, _ := base64.StdEncoding.DecodeString(identity.PrivateKey)
	if client == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		client = &http.Client{Transport: transport}
	}
	cl := *client
	cl.Timeout = 10 * time.Second
	cl.CheckRedirect = noRedirect
	return &Agent{cfg: c, meta: meta, node: sourceNode(meta, c.Container), dir: dir, token: identity.LocalToken, key: key, lock: lock, client: &cl, credentials: credentials, leases: map[string]int64{}, now: time.Now}, nil
}

func (a *Agent) Close() error     { a.client.CloseIdleConnections(); return a.lock.Close() }
func (a *Agent) Node() string     { return a.node }
func (a *Agent) StateDir() string { return a.dir }

func (a *Agent) laneAllowed(lane string) bool {
	for _, allowed := range a.cfg.lanes() {
		if lane == allowed {
			return true
		}
	}
	return false
}
func (a *Agent) lease(lane string) int64 { a.mu.RLock(); defer a.mu.RUnlock(); return a.leases[lane] }

func (a *Agent) Renew(ctx context.Context, lane string) error {
	if !a.laneAllowed(lane) {
		return errors.New("unconfigured lane")
	}
	now := a.now()
	body, _ := json.Marshal(map[string]string{"service_arn": a.cfg.ServiceARN, "task_arn": a.meta.TaskARN, "container": a.cfg.Container, "runtime_id": a.meta.RuntimeID, "lane": lane, "public_key": base64.StdEncoding.EncodeToString(a.key.Public().(ed25519.PublicKey))})
	req, err := http.NewRequestWithContext(ctx, "POST", a.cfg.RegisterURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	credentials, err := a.credentials.Retrieve(ctx)
	if err != nil {
		return errors.New("task credentials unavailable")
	}
	if credentials.SessionToken == "" {
		return errors.New("temporary task credentials required")
	}
	h := sha256.Sum256(body)
	region := servicePattern.FindStringSubmatch(a.cfg.ServiceARN)[1]
	if err := v4.NewSigner().SignHTTP(ctx, credentials, req, hex.EncodeToString(h[:]), "execute-api", region, now); err != nil {
		return errors.New("IAM registration signing failed")
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return errors.New("IAM registration unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return errors.New("IAM registration rejected; retain source state")
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 16385))
	if err != nil || len(b) > 16384 {
		return errors.New("registration response unreadable")
	}
	var ack struct {
		OK             bool   `json:"ok"`
		Version        int    `json:"version"`
		Node           string `json:"node"`
		Lane           string `json:"lane"`
		Audience       string `json:"audience"`
		LeaseUntil     int64  `json:"lease_until"`
		ArchiveClosure bool   `json:"archive_closure_v2"`
		FinalBoundary  bool   `json:"final_boundary_v1"`
		NewAPIFiles    bool   `json:"final_newapi_files_v1"`
	}
	if json.Unmarshal(b, &ack) != nil || !ack.OK || ack.Version != 1 || ack.Node != a.node || ack.Lane != lane || ack.Audience != a.cfg.Audience || ack.LeaseUntil < now.Unix()+30 || ack.LeaseUntil > now.Unix()+16*60 {
		return errors.New("registration ACK does not match this source")
	}
	if a.cfg.ArchiveClosure && !ack.ArchiveClosure {
		return errors.New("receiver does not support archive closure; upgrade receiver first")
	}
	if a.cfg.FinalLogRoot != "" && !ack.FinalBoundary {
		return errors.New("receiver does not support final source boundaries")
	}
	if a.cfg.FinalFileContract != "" && !ack.NewAPIFiles {
		return errors.New("receiver does not support NewAPI file boundaries; upgrade receiver and bridge first")
	}
	a.mu.Lock()
	a.leases[lane] = max(a.leases[lane], ack.LeaseUntil)
	a.mu.Unlock()
	return nil
}

func signingMessage(audience, node, lane, path, at string, body []byte) []byte {
	h := sha256.Sum256(body)
	return []byte("monitor-ecs-log-v1\n" + audience + "\n" + node + "\n" + lane + "\n" + path + "\n" + at + "\n" + hex.EncodeToString(h[:]))
}

func (a *Agent) send(ctx context.Context, lane, path string, body []byte) (*http.Response, error) {
	now := a.now().Unix()
	if !a.laneAllowed(lane) || a.lease(lane) <= now {
		return nil, errors.New("source lease unavailable; retry unchanged batch")
	}
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(a.cfg.MonitorURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	at := strconv.FormatInt(now, 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Monitor-ECS-Node", a.node)
	req.Header.Set("X-Monitor-ECS-Time", at)
	req.Header.Set("X-Monitor-ECS-Signature", base64.StdEncoding.EncodeToString(ed25519.Sign(a.key, signingMessage(a.cfg.Audience, a.node, lane, path, at, body))))
	return a.client.Do(req)
}

func (a *Agent) Heartbeat(ctx context.Context, lane string) error {
	body, _ := json.Marshal(map[string]string{"node": a.node})
	resp, err := a.send(ctx, lane, "/internal/ecs/v1/heartbeat/"+lane, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if err != nil || len(b) > 4096 {
		return errors.New("heartbeat response unreadable")
	}
	var ack struct {
		OK   bool   `json:"ok"`
		Node string `json:"node"`
		Lane string `json:"lane"`
		At   int64  `json:"heartbeat_at"`
	}
	if resp.StatusCode != 200 || json.Unmarshal(b, &ack) != nil || !ack.OK || ack.Node != a.node || ack.Lane != lane || ack.At < a.now().Unix()-120 || ack.At > a.now().Unix()+120 {
		return errors.New("heartbeat not acknowledged")
	}
	return nil
}
