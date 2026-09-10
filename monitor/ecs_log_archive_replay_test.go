package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

// Separate directory stands in for external storage. Destroying the entire
// collector state tree cannot remove these objects. No AWS access in tests.
type archiveDiskFixture struct {
	root string
	fail bool
	last ecsarchive.Object
}

func (s *archiveDiskFixture) Binding() string { return s.root }

func (s *archiveDiskFixture) Put(_ context.Context, key string, raw []byte) error {
	if s.fail {
		return errors.New("archive unavailable")
	}
	path := filepath.Join(s.root, key)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if errors.Is(err, os.ErrExist) {
		return ecsarchive.ErrExists
	}
	if err != nil {
		return err
	}
	_, err = f.Write(raw)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	s.last = ecsarchive.Object{Key: key, ETag: "fixture-etag", Modified: time.Now(), Body: append([]byte(nil), raw...)}
	return nil
}
func (s *archiveDiskFixture) Get(_ context.Context, key string) (ecsarchive.Object, error) {
	raw, err := os.ReadFile(filepath.Join(s.root, key))
	if err != nil {
		return ecsarchive.Object{}, err
	}
	o := s.last
	o.Key = key
	o.Body = raw
	return o, nil
}

func TestECSLogArchiveTaskDestroyedFourLanes(t *testing.T) {
	for _, lane := range []string{"access", "error", "evidence", "reject"} {
		t.Run(lane, func(t *testing.T) {
			m := newECSLogTestMonitor(t)
			kind, path := "nginx", "/internal/nginx"
			switch lane {
			case "error":
				path = "/internal/nginx-errors"
			case "evidence":
				path = "/internal/nginx-evidence/v1"
			case "reject":
				kind, path = "reject", "/internal/rejections/v2"
			}
			agent, cfg, _, _ := ecsAgentFixture(t, m, kind, strings.Repeat("a", 32), nil)
			archive := &archiveDiskFixture{root: t.TempDir()}
			if err := agent.ConfigureArchive(archive, "isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("a", 32)); err != nil {
				t.Fatal(err)
			}
			if err := agent.Renew(context.Background(), lane); err != nil {
				t.Fatal(err)
			}
			// Registration is complete; data endpoint now goes offline BEFORE
			// any live commit. The agent's cloned client delegates to this hook.
			// The agent clones the client, so mutate the fixture routing by making
			// its authoritative receiver reject ingestion, not by swapping clients.
			m.cfg.ECSLogScope = "offline-fixture"
			client, token := privateCollectorClient(t, agent)
			body := ecsLaneTestBody(t, agent.Node(), lane, 1)
			for retry := 0; retry < 2; retry++ {
				req, _ := http.NewRequest("POST", "http://monitor.example"+path, bytes.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+token)
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != 503 {
					t.Fatalf("offline status %d", resp.StatusCode)
				}
			}
			if archive.last.Key == "" {
				t.Fatal("batch was not externally archived")
			}
			if err := agent.Close(); err != nil {
				t.Fatal(err)
			}
			// Only a validated test-created directory is destroyed.
			if !strings.HasPrefix(cfg.StateRoot, "/tmp/eai-") {
				t.Fatal("unexpected fixture root")
			}
			if err := os.RemoveAll(cfg.StateRoot); err != nil {
				t.Fatal(err)
			}
			m.cfg.ECSLogScope = "isolated"
			at := archive.last.Modified.Unix()
			if err := m.storeDB.Model(&ECSLogSource{}).Where("node = ? AND lane = ?", agent.Node(), lane).Updates(map[string]any{"stopped_at": at, "lease_until": at + 1}).Error; err != nil {
				t.Fatal(err)
			}
			// Recover 2 minutes later, without task directory or agent private key.
			o, err := archive.Get(context.Background(), archive.last.Key)
			if err != nil {
				t.Fatal(err)
			}
			replayer := newECSArchiveReplayer(m, "isolated/")
			for retry := 0; retry < 2; retry++ {
				r := replayer.replay(context.Background(), o, o.Modified.Add(2*time.Minute))
				if !r.Accepted || r.Status != 200 || r.Duplicate != (retry == 1) {
					t.Fatalf("recovery retry %d: %+v", retry, r)
				}
			}
			var source ECSLogSource
			if err := m.storeDB.First(&source, "node = ? AND lane = ?", agent.Node(), lane).Error; err != nil {
				t.Fatal(err)
			}
			if source.LastReport != 0 || source.LastHeartbeat != 0 || source.StoppedAt != at {
				t.Fatalf("replay resurrected source: %+v", source)
			}
		})
	}
}

func TestECSLogArchiveSecurityAndRealConflict(t *testing.T) {
	m := newECSLogTestMonitor(t)
	s, key := registerECSTestSource(t, m, strings.Repeat("b", 32), "reject")
	seal := func(body []byte) ecsarchive.Object {
		e, err := ecsarchive.Seal(m.cfg.ECSLogAudience, s.Node, s.Lane, body, key)
		if err != nil {
			t.Fatal(err)
		}
		k, err := ecsarchive.ObjectKey("isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("b", 32), e)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(e)
		return ecsarchive.Object{Key: k, ETag: "fixture", Modified: time.Now(), Body: raw}
	}
	o := seal(ecsLaneTestBody(t, s.Node, s.Lane, 1))
	r := newECSArchiveReplayer(m, "isolated/")
	if got := r.replay(context.Background(), o, time.Now()); !got.Accepted {
		t.Fatalf("first %+v", got)
	}
	if got := r.replay(context.Background(), seal(ecsLaneTestBody(t, s.Node, s.Lane, 2)), time.Now()); got.Status != 409 || got.Accepted {
		t.Fatalf("real conflict hidden %+v", got)
	}
	for name, mutate := range map[string]func(*ecsarchive.Object){
		"cross-task": func(o *ecsarchive.Object) {
			o.Key = strings.Replace(o.Key, strings.Repeat("b", 32), strings.Repeat("c", 32), 1)
		},
		"tampered": func(o *ecsarchive.Object) {
			o.Body = bytes.Replace(o.Body, []byte(`"signature":"`), []byte(`"signature":"a`), 1)
		},
		"late-upload":      func(o *ecsarchive.Object) { o.Modified = time.Now().Add(time.Hour) },
		"pre-registration": func(o *ecsarchive.Object) { o.Modified = time.Unix(s.FirstSeen-1, 0) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := o
			mutate(&bad)
			if got := r.replay(context.Background(), bad, time.Now()); got.Accepted || got.Status == 200 {
				t.Fatalf("untrusted object accepted %+v", got)
			}
		})
	}
	if got := r.replay(context.Background(), o, time.Now().Add(25*time.Hour)); got.Status != 410 {
		t.Fatalf("expired object %+v", got)
	}
	if err := m.storeDB.Model(&s).Update("revoked", true).Error; err != nil {
		t.Fatal(err)
	}
	if got := r.replay(context.Background(), o, time.Now()); got.Status != 403 {
		t.Fatalf("revoked %+v", got)
	}
}
