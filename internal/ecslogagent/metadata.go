package ecslogagent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var errMetadataNotReady = errors.New("producer container not ready")
var errMetadataUnavailable = errors.New("ECS metadata unavailable")

// Metadata is a candidate identity only. Monitor independently verifies it
// using the signed IAM caller and ECS control plane before granting a lease.
func ReadMetadata(ctx context.Context, uri, container string) (Metadata, error) {
	b, err := readTaskMetadata(ctx, uri)
	if err != nil {
		return Metadata{}, err
	}
	return parseMetadata(b, container)
}

// CheckProducerStopped reads only the fixed link-local endpoint. Monitor also
// verifies the stopped incarnation and exit code through the AWS control plane.
func CheckProducerStopped(ctx context.Context, uri, container string, expected Metadata) error {
	b, err := readTaskMetadata(ctx, uri)
	if err != nil {
		return err
	}
	return checkStoppedMetadata(b, container, expected)
}

func checkStoppedMetadata(body []byte, container string, expected Metadata) error {
	meta, err := parseMetadata(body, container)
	if err != nil || meta != expected {
		return errors.New("final producer identity mismatch")
	}
	var task struct {
		Containers []struct {
			Name, KnownStatus string
			ExitCode          *int
		}
	}
	if json.Unmarshal(body, &task) != nil {
		return errors.New("invalid stop metadata")
	}
	for _, c := range task.Containers {
		if c.Name == container && c.KnownStatus == "STOPPED" && c.ExitCode != nil && *c.ExitCode == 0 {
			return nil
		}
	}
	return errors.New("producer has not verifiably exited normally")
}

func readTaskMetadata(ctx context.Context, uri string) ([]byte, error) {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "http" || u.Host != "169.254.170.2" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || !strings.HasPrefix(u.Path, "/v4/") || len(strings.TrimPrefix(u.Path, "/v4/")) < 8 {
		return nil, errors.New("ECS v4 link-local metadata endpoint required")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: noRedirect}
	req, err := http.NewRequestWithContext(ctx, "GET", strings.TrimRight(uri, "/")+"/task", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errMetadataUnavailable
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			return nil, errMetadataUnavailable
		}
		return nil, errors.New("ECS metadata rejected")
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if err != nil || len(b) > 1<<20 {
		return nil, errors.New("ECS metadata exceeds budget")
	}
	return b, nil
}

func parseMetadata(body []byte, container string) (Metadata, error) {
	var task struct {
		TaskARN    string
		Containers []struct {
			Name     string
			DockerID string `json:"DockerId"`
		}
	}
	if json.Unmarshal(body, &task) != nil || len(task.Containers) > 100 {
		return Metadata{}, errors.New("invalid task metadata")
	}
	var result Metadata
	matches := 0
	for _, c := range task.Containers {
		if c.Name == container {
			matches++
			if matches > 1 {
				return Metadata{}, errors.New("ambiguous producer container")
			}
			result = Metadata{task.TaskARN, c.DockerID}
		}
	}
	if result.TaskARN == "" || result.RuntimeID == "" {
		return Metadata{}, errMetadataNotReady
	}
	return result, nil
}

func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
