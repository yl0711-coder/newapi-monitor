package monitor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
)

// TestNewapiAuthRC26LiveSessionCleanup is opt-in because it requires a real,
// disposable RC26 instance. CI skips it; release validation supplies the three
// MONITOR_TEST_RC26_* variables and must never point them at production.
func TestNewapiAuthRC26LiveSessionCleanup(t *testing.T) {
	base := strings.TrimRight(os.Getenv("MONITOR_TEST_RC26_URL"), "/")
	username := os.Getenv("MONITOR_TEST_RC26_USERNAME")
	password := os.Getenv("MONITOR_TEST_RC26_PASSWORD")
	if base == "" || username == "" || password == "" {
		t.Skip("set MONITOR_TEST_RC26_URL, MONITOR_TEST_RC26_USERNAME and MONITOR_TEST_RC26_PASSWORD")
	}
	if !strings.HasPrefix(base, "http://127.0.0.1:") && !strings.HasPrefix(base, "http://localhost:") {
		t.Fatalf("live RC26 integration target must be loopback, got %q", base)
	}

	client := &http.Client{Timeout: newAPIAuthRequestTimeout}
	control, err := rc26IntegrationLogin(client, base, username, password)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := revokeTemporaryNewAPISession(client, base, control.Data.AccessToken, control.Data.Session.SID); err != nil {
			t.Errorf("revoke control session: %v", err)
		}
	}()

	before, err := rc26IntegrationSessions(client, base, control.Data.AccessToken)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 1 || before[0].SID != control.Data.Session.SID {
		t.Fatalf("unexpected baseline sessions: count=%d", len(before))
	}

	m := &Monitor{cfg: Settings{NewAPIBaseURL: base}}
	role, name, err := m.newapiAuth(username, password)
	if err != nil || role != roleRoot || name == "" {
		t.Fatalf("monitor RC26 auth: role=%d name=%q err=%v", role, name, err)
	}

	after, err := rc26IntegrationSessions(client, base, control.Data.AccessToken)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) || len(after) != 1 || after[0].SID != control.Data.Session.SID {
		t.Fatalf("monitor login leaked or revoked the wrong session: before=%d after=%d", len(before), len(after))
	}
	for _, session := range after {
		if session.UserAgent == newAPIAuthUserAgent {
			t.Fatal("monitor temporary session remains active after successful authentication")
		}
	}
}

func rc26IntegrationLogin(client *http.Client, base, username, password string) (newAPILoginResponse, error) {
	payload, err := json.Marshal(map[string]string{"username": username, "password": password})
	if err != nil {
		return newAPILoginResponse{}, err
	}
	req, err := http.NewRequest(http.MethodPost, base+"/api/user/login", bytes.NewReader(payload))
	if err != nil {
		return newAPILoginResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "NexusAPI-Monitor/rc26-integration-control")
	resp, err := client.Do(req)
	if err != nil {
		return newAPILoginResponse{}, err
	}
	defer resp.Body.Close()
	var result newAPILoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return result, err
	}
	if resp.StatusCode != http.StatusOK || !result.Success || result.Data.AccessToken == "" || result.Data.Session.SID == "" {
		return result, fmt.Errorf("RC26 control login failed: HTTP %d success=%t", resp.StatusCode, result.Success)
	}
	return result, nil
}

type rc26IntegrationSession struct {
	SID       string `json:"sid"`
	UserAgent string `json:"user_agent"`
}

func rc26IntegrationSessions(client *http.Client, base, accessToken string) ([]rc26IntegrationSession, error) {
	req, err := http.NewRequest(http.MethodGet, base+"/api/user/sessions", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result struct {
		Success bool                     `json:"success"`
		Data    []rc26IntegrationSession `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK || !result.Success {
		return nil, fmt.Errorf("list RC26 sessions failed: HTTP %d success=%t", resp.StatusCode, result.Success)
	}
	return result.Data, nil
}
