package ecslogagent

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWaitMetadataProducerStartsLater(t *testing.T) {
	calls := 0
	want := Metadata{TaskARN: "fixture-task", RuntimeID: "fixture-runtime"}
	meta, err := waitMetadata(context.Background(), time.Millisecond, func(context.Context) (Metadata, error) {
		calls++
		switch calls {
		case 1:
			return Metadata{}, errMetadataNotReady
		case 2:
			return Metadata{}, errMetadataUnavailable
		default:
			return want, nil
		}
	})
	if err != nil || meta != want || calls != 3 {
		t.Fatalf("startup wait failed: %+v %d %v", meta, calls, err)
	}
}

func TestWaitMetadataRejectsInvalidIdentityAndHonorsCancellation(t *testing.T) {
	calls := 0
	permanent := errors.New("invalid identity")
	if _, err := waitMetadata(context.Background(), time.Millisecond, func(context.Context) (Metadata, error) { calls++; return Metadata{}, permanent }); !errors.Is(err, permanent) || calls != 1 {
		t.Fatal("permanent error retried")
	}
	ctx, cancel := context.WithCancel(context.Background())
	_, err := waitMetadata(ctx, time.Hour, func(context.Context) (Metadata, error) { cancel(); return Metadata{}, errMetadataNotReady })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("startup ignored cancellation: %v", err)
	}
	if _, err := WaitMetadata(context.Background(), "http://127.0.0.1/v4/invalid", "nginx"); err == nil {
		t.Fatal("unsafe endpoint accepted")
	}
	for _, body := range []string{
		`{"TaskARN":"fixture","Containers":[{"Name":"nginx","DockerId":""},{"Name":"nginx","DockerId":"runtime"}]}`,
		`{"TaskARN":"fixture","Containers":[{"Name":"nginx","DockerId":"runtime"},{"Name":"nginx","DockerId":""}]}`,
	} {
		if _, err := parseMetadata([]byte(body), "nginx"); err == nil || errors.Is(err, errMetadataNotReady) {
			t.Fatal("ambiguous producer treated as retryable startup")
		}
	}
}
