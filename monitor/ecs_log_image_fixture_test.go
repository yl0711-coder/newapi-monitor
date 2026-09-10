package monitor

// This fixture is compiled ONLY into a Go test binary. It runs in a local
// network-none Docker namespace, never in the Monitor release executable.
import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

const imageFixtureTask = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type imageProducer struct {
	Name        string
	DockerID    string `json:"DockerId"`
	KnownStatus string
	ExitCode    *int
}

type imageReceiverFixture struct {
	mu        sync.Mutex
	m         *Monitor
	producers map[string]imageProducer
	objects   []ecsarchive.Object
	byKey     map[string]ecsarchive.Object
}

func TestECSLogImageReceiverFixture(t *testing.T) {
	if os.Getenv("MONITOR_TEST_ECS_IMAGE_FIXTURE") != "receiver" {
		t.Skip("only the isolated Docker image acceptance runner may start this fixture")
	}
	m := ownershipFixture(t)
	m.cfg.ECSArchiveEnabled = true
	m.ecsLogPolicies[0].Containers = map[string][]string{"nginx": {"access", "error", "evidence"}, "new-api": {"reject"}}
	f := &imageReceiverFixture{m: m, producers: map[string]imageProducer{}, byKey: map[string]ecsarchive.Object{}}
	m.ecsLogVerify = func(ctx context.Context, in ecsLogRegistration, p ECSLogPolicy) (ECSLogSource, error) {
		f.mu.Lock()
		producer := f.producers[in.Container]
		f.mu.Unlock()
		if producer.KnownStatus != "RUNNING" || in.RuntimeID != producer.DockerID || !strings.HasSuffix(in.TaskARN, "/"+imageFixtureTask) {
			return ECSLogSource{}, errors.New("fixture task not active")
		}
		return verifyECSLogTask(ctx, validECSLogTaskFixture(in), in, p)
	}
	for _, dir := range []string{"/logs-nginx", "/logs-reject", "/state-nginx", "/state-reject", "/fixture-shared"} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(dir, 100, 101); err != nil {
			t.Fatal(err)
		}
	}
	cert := imageFixtureCertificate(t)
	servers := []*http.Server{
		{Addr: ":443", Handler: http.HandlerFunc(f.https), TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}},
		{Addr: ":80", Handler: http.HandlerFunc(f.metadata)},
		{Addr: "127.0.0.1:8080", Handler: http.HandlerFunc(f.control)},
	}
	failed := make(chan error, len(servers))
	for i, server := range servers {
		server.ReadHeaderTimeout = 3 * time.Second
		server.ReadTimeout, server.WriteTimeout = 30*time.Second, 30*time.Second
		t.Cleanup(func() { _ = server.Close() })
		go func() {
			if i == 0 {
				failed <- server.ListenAndServeTLS("", "")
			} else {
				failed <- server.ListenAndServe()
			}
		}()
	}
	select {
	case err := <-failed:
		t.Fatal(err)
	case <-time.After(4 * time.Minute):
		t.Fatal("local image fixture lifetime exceeded")
	}
}

func imageFixtureCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "local image fixture only"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, DNSNames: []string{
			"monitor.fixture.invalid", "fixture.execute-api.us-west-2.amazonaws.com", "sts.us-west-2.amazonaws.com", "fixture-image-archive.s3.us-west-2.amazonaws.com",
		}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("/fixture-shared/ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0644); err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func (f *imageReceiverFixture) metadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" || r.URL.Path != "/v4/fixture-image/task" {
		http.NotFound(w, r)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	rows := make([]imageProducer, 0, len(f.producers))
	for _, p := range f.producers {
		rows = append(rows, p)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"TaskARN": testECSRegistration(imageFixtureTask, "").TaskARN, "Containers": rows})
}

// Host orchestration sets STOPPED only AFTER docker wait confirms the actual
// producer exited. This is a simulated control plane, not evidence of AWS IAM.
func (f *imageReceiverFixture) control(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/status" && r.Method == "GET" {
		f.mu.Lock()
		defer f.mu.Unlock()
		lanes := map[string]bool{}
		for _, o := range f.objects {
			if e, err := ecsarchive.Decode(o.Body); err == nil {
				lanes[e.Lane] = true
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"lanes": len(lanes), "objects": len(f.objects)})
		return
	}
	if r.URL.Path == "/verify" && r.Method == "POST" {
		result, err := f.verifyArchive(r.URL.Query().Get("expect"))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = json.NewEncoder(w).Encode(result)
		return
	}
	var p imageProducer
	if r.Method != "POST" || r.URL.Path != "/producer" || json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&p) != nil || (p.Name != "nginx" && p.Name != "new-api") || len(p.DockerID) != 64 || (p.KnownStatus != "RUNNING" && p.KnownStatus != "STOPPED") {
		http.Error(w, "invalid fixture control", 400)
		return
	}
	f.mu.Lock()
	previous := f.producers[p.Name]
	if previous.DockerID != "" && previous.DockerID != p.DockerID || p.KnownStatus == "STOPPED" && p.ExitCode == nil {
		f.mu.Unlock()
		http.Error(w, "fixture identity mismatch", 400)
		return
	}
	f.producers[p.Name] = p
	f.mu.Unlock()
	if p.KnownStatus == "STOPPED" {
		code := "ServiceSchedulerInitiated"
		if *p.ExitCode != 0 {
			code = "EssentialContainerExited"
		}
		if err := f.m.storeDB.Model(&ECSLogSource{}).Where("container = ? AND runtime_id = ?", p.Name, p.DockerID).Updates(map[string]any{"stopped_at": time.Now().Unix(), "stop_code": code, "producer_exit_code": aws.Int32(int32(*p.ExitCode))}).Error; err != nil {
			http.Error(w, "fixture stop write failed", 500)
			return
		}
	}
	_, _ = w.Write([]byte(`{"ok":true}`))
}
