package ecslogagent

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
)

const maxPayload = 2 << 20

// Only the private Unix listener uses this handler. It is not a general HTTP
// proxy: fixed origin, four fixed paths, bound node, lane policy, no redirects,
// no caller-supplied outbound headers or credentials.
func (a *Agent) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	origin, _ := url.Parse(a.cfg.MonitorURL)
	lane := localLane(r.URL.Path)
	if r.Method != "POST" || r.Host != origin.Host || r.URL.RawQuery != "" || r.URL.RawPath != "" || !a.laneAllowed(lane) {
		http.Error(w, "invalid local collector destination", 400)
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+a.token)) != 1 {
		http.Error(w, "private collector authorization required", 401)
		return
	}
	n := a.inflight.Add(1)
	defer a.inflight.Add(-1)
	if n > 8 {
		http.Error(w, "agent busy; retain batch", 429)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxPayload+1))
	if err != nil || len(body) > maxPayload {
		http.Error(w, "bounded payload required", 413)
		return
	}
	var envelope struct {
		Node string `json:"node"`
	}
	if json.Unmarshal(body, &envelope) != nil || envelope.Node != a.node {
		http.Error(w, "source mismatch; preserve batch", 400)
		return
	}
	if a.cfg.DeferredArchiveACK && (a.archive == nil || r.Header.Get("X-Monitor-Archive-Ack") != "1") {
		http.Error(w, "external archive and compatible collector required", 503)
		return
	}
	if err := a.archiveFrozen(r.Context(), lane, body); err != nil {
		http.Error(w, "external archive unavailable; retain unchanged batch", 503)
		return
	}
	if a.cfg.DeferredArchiveACK {
		a.writeArchiveReceipt(w, lane, body)
		return
	}
	resp, err := a.send(r.Context(), lane, "/internal/ecs/v1/ingest/"+lane, body)
	if err != nil {
		http.Error(w, "signed delivery unavailable; retain batch", 503)
		return
	}
	defer resp.Body.Close()
	ack, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil || len(ack) > 65536 {
		http.Error(w, "ACK unavailable; retain batch", 502)
		return
	}
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		http.Error(w, "Monitor redirect refused; retain batch", 502)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(ack)
}

func (a *Agent) SocketPath() string { return filepath.Join(a.dir, "agent.sock") }

func (a *Agent) Listen() (net.Listener, error) {
	path := a.SocketPath()
	if len(path) > 100 {
		return nil, errors.New("agent socket path too long; use a short private state root")
	}
	// The source lock is held. Only remove a stale socket, never an ordinary
	// file/symlink. The Unix listener removes its own socket when closed.
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, &os.PathError{Op: "listen", Path: path, Err: os.ErrExist}
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0600); err != nil {
		_ = listener.Close()
		return nil, err
	}
	return listener, nil
}
