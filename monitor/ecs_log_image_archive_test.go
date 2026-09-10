package monitor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/ecsarchive"
)

func (f *imageReceiverFixture) https(w http.ResponseWriter, r *http.Request) {
	switch r.Host {
	case "monitor.fixture.invalid":
		f.m.ecsAcceptanceHandler().ServeHTTP(w, r)
	case "fixture.execute-api.us-west-2.amazonaws.com":
		if r.Method != "POST" || r.URL.Path != "/isolated/register" || !strings.Contains(r.Header.Get("Authorization"), "Credential=fixture-id/") {
			http.Error(w, "fixture registration only", 403)
			return
		}
		var in ecsLogRegistration
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&in) != nil || in.CallerARN != "" {
			http.Error(w, "invalid fixture registration", 400)
			return
		}
		in.CallerARN = testECSRegistration(imageFixtureTask, "").CallerARN
		body, _ := json.Marshal(in)
		forward := r.Clone(r.Context())
		forward.URL.Path = "/internal/ecs/v1/register"
		forward.Header.Set("Authorization", "Bearer "+f.m.cfg.ECSLogBridgeToken)
		forward.Body = io.NopCloser(bytes.NewReader(body))
		f.m.ecsAcceptanceHandler().ServeHTTP(w, forward)
	case "sts.us-west-2.amazonaws.com":
		w.Header().Set("Content-Type", "text/xml")
		_, _ = fmt.Fprintf(w, `<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><GetCallerIdentityResult><Arn>%s</Arn><UserId>AROAABCDEFGHIJKLMNOPQ:%s</UserId><Account>123456789012</Account></GetCallerIdentityResult></GetCallerIdentityResponse>`, testECSRegistration(imageFixtureTask, "").CallerARN, imageFixtureTask)
	case "fixture-image-archive.s3.us-west-2.amazonaws.com":
		f.s3(w, r)
	default:
		http.Error(w, "unexpected fixture destination", 403)
	}
}

func (f *imageReceiverFixture) s3(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/")
	if _, _, _, _, err := ecsarchive.ParseKey("isolated/", key); err != nil || r.Header.Get("X-Amz-Expected-Bucket-Owner") != "123456789012" {
		http.Error(w, "invalid archive fixture destination", 400)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	previous, exists := f.byKey[key]
	if r.Method == "GET" && exists {
		w.Header().Set("ETag", previous.ETag)
		w.Header().Set("Last-Modified", previous.Modified.Format(http.TimeFormat))
		w.Header().Set("Content-Length", fmt.Sprint(len(previous.Body)))
		_, _ = w.Write(previous.Body)
		return
	}
	if r.Method != "PUT" {
		http.NotFound(w, r)
		return
	}
	if exists {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(412)
		_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code></Error>`))
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, ecsarchive.MaxObject))
	sum := sha256.Sum256(body)
	checksum := base64.StdEncoding.EncodeToString(sum[:])
	if err != nil || len(f.objects) >= 1000 || checksum != r.Header.Get("X-Amz-Checksum-Sha256") || r.Header.Get("If-None-Match") != "*" || r.Header.Get("X-Amz-Server-Side-Encryption") != "AES256" {
		http.Error(w, "invalid bounded archive write", 400)
		return
	}
	if _, err := ecsarchive.Decode(body); err != nil {
		http.Error(w, "invalid archive envelope", 400)
		return
	}
	o := ecsarchive.Object{Key: key, Body: body, ETag: `"` + hex.EncodeToString(sum[:]) + `"`, Modified: time.Now().UTC()}
	f.byKey[key], f.objects = o, append(f.objects, o)
	w.Header().Set("ETag", o.ETag)
	w.Header().Set("X-Amz-Checksum-Sha256", checksum)
}

func (f *imageReceiverFixture) imageCounts() (map[string]int64, error) {
	counts := map[string]int64{}
	for lane, model := range map[string]any{"access": &NginxMinuteSample{}, "error": &NginxErrorMinuteSample{}, "reject": &RejectionSample{}} {
		var n int64
		if err := f.m.storeDB.Model(model).Select("COALESCE(SUM(count),0)").Scan(&n).Error; err != nil {
			return nil, err
		}
		counts[lane] = n
	}
	var n int64
	if err := f.m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Count(&n).Error; err != nil {
		return nil, err
	}
	counts["evidence"] = n
	return counts, nil
}

func (f *imageReceiverFixture) verifyArchive(expect string) (map[string]any, error) {
	if expect != "complete" && expect != "gap" {
		return nil, fmt.Errorf("explicit expected result required")
	}
	f.mu.Lock()
	objects := append([]ecsarchive.Object(nil), f.objects...)
	f.mu.Unlock()
	if len(objects) == 0 {
		return nil, fmt.Errorf("no image archives")
	}
	replay := newECSArchiveReplayer(f.m, "isolated/")
	for _, object := range objects {
		if result := replay.replay(context.Background(), object, time.Now()); !result.Accepted {
			return nil, fmt.Errorf("image archive replay failed: %+v", result)
		}
	}
	before, err := f.imageCounts()
	if err != nil {
		return nil, err
	}
	for _, object := range objects {
		if result := replay.replay(context.Background(), object, time.Now()); !result.Accepted || !result.Duplicate {
			return nil, fmt.Errorf("image duplicate replay failed: %+v", result)
		}
	}
	after, err := f.imageCounts()
	if err != nil {
		return nil, err
	}
	for lane, n := range before {
		if after[lane] != n {
			return nil, fmt.Errorf("duplicate image archive changed %s", lane)
		}
	}
	var sources []ECSLogSource
	if err := f.m.storeDB.Find(&sources).Error; err != nil {
		return nil, fmt.Errorf("image sources query failed: %w", err)
	}
	if len(sources) != 4 {
		return nil, fmt.Errorf("image sources incomplete: %d", len(sources))
	}
	if err := projectArchiveDelivery(f.m.storeDB, sources); err != nil {
		return nil, err
	}
	verified := 0
	for _, source := range sources {
		if err := f.m.checkECSLogOwnership(context.Background(), source); err != nil {
			return nil, err
		}
		if source.FinalBoundaryStatus == "retained_files_verified" {
			verified++
		}
	}
	if expect == "complete" {
		if verified != 4 || after["access"] != 2 || after["error"] != 2 || after["evidence"] != 2 || after["reject"] != 5 {
			return nil, fmt.Errorf("image tail not complete: verified=%d counts=%v", verified, after)
		}
	} else if verified != 0 {
		return nil, fmt.Errorf("bad shutdown order falsely complete: %d", verified)
	}
	return map[string]any{"passed": true, "production_ready": false, "expected": expect, "counts": after, "verified_lanes": verified, "objects": len(objects), "duplicate_counts_unchanged": true}, nil
}
