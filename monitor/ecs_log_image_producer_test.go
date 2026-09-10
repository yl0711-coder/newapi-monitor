package monitor

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestECSLogImageProducerFixture(t *testing.T) {
	kind := os.Getenv("MONITOR_TEST_ECS_IMAGE_FIXTURE")
	if kind != "nginx" && kind != "new-api" {
		t.Skip("only the isolated Docker image acceptance runner may start this producer")
	}
	// Install the handler before making any observable files. TERM produces
	// one last append, exactly as a graceful log writer shutdown can do.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM)
	defer signal.Stop(stop)
	write := func(final bool) {
		now := time.Now().UTC()
		names := []string{"nexusapi_access.jsonl", "error.log"}
		lines := []string{
			fmt.Sprintf(`{"log_schema":2,"msec":"%d.250","request_method":"POST","uri":"/v1/responses","status":"200","request_time":"0.350","upstream_status":"200","upstream_response_time":"0.300","upstream_connect_time":"0.025","upstream_header_time":"0.125","bytes_sent":"1024","nginx_request_id":"image-%t","oneapi_request_id":"image-%t","request_completion":"OK"}`+"\n", now.Unix(), final, final),
			now.Format("2006/01/02 15:04:05") + " [error] 1#1: *1 upstream timed out (110: Connection timed out) while reading response header from upstream\n",
		}
		if kind == "new-api" {
			names = []string{"oneapi-20000101.log", "oneapi-20000102.log"}
			if final {
				names = append(names, "oneapi-20000103.log")
			}
			line := "[ERR] " + now.Format("2006/01/02 - 15:04:05") + " | image | user 7 | No available channel for model fixture under group fixture-group (distributor)\n"
			lines = []string{line, line, line}
		}
		for i, name := range names {
			f, err := os.OpenFile(filepath.Join("/logs", name), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				t.Fatal(err)
			}
			_, writeErr := f.WriteString(lines[i])
			syncErr, closeErr := f.Sync(), f.Close()
			if writeErr != nil || syncErr != nil || closeErr != nil {
				t.Fatal("image producer write failed", writeErr, syncErr, closeErr)
			}
		}
		fmt.Printf("IMAGE_PRODUCER kind=%s final=%t files=%d\n", kind, final, len(names))
	}
	write(false)
	select {
	case <-stop:
		write(true)
	case <-time.After(3 * time.Minute):
		t.Fatal("image producer lifetime exceeded")
	}
}
