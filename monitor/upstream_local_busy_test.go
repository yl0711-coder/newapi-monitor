package monitor

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

// An upstream that runs on SQLite may report "database is locked" itself.
// That is its failure: it must be recorded, not mistaken for our store being
// busy (which pauses silently and retries every few seconds).
func TestUpstreamDatabaseLockedIsNotLocalBusy(t *testing.T) {
	remote := []error{
		&upstreamHTTPError{Status: http.StatusInternalServerError, Message: "database is locked"},
		fmt.Errorf("读取使用日志：%w", &upstreamHTTPError{Status: 500, Message: "database is locked (5) (SQLITE_BUSY)"}),
		decodeNewAPIData([]byte(`{"success":false,"message":"database is locked"}`), &struct{}{}),
		decodeSub2APIEnvelopeForTest(t),
		&tokenForceAPIError{Status: 500, Message: "database is busy"},
	}
	for i, err := range remote {
		if err == nil {
			t.Fatalf("case %d: no error", i)
		}
		if isUpstreamUsageLocalStoreBusy(err) {
			t.Fatalf("case %d: upstream error %q classified as local busy", i, err)
		}
		if _, paused := upstreamPauseRetryAt(err, 1000); paused {
			t.Fatalf("case %d: upstream error %q pauses instead of being recorded", i, err)
		}
	}
	// Local errors that lost their type through %v wrapping still count.
	if !isUpstreamUsageLocalStoreBusy(errors.New("保存同步结果：database is locked (5) (SQLITE_BUSY)")) {
		t.Fatal("local busy text no longer recognized")
	}
}

func decodeSub2APIEnvelopeForTest(t *testing.T) error {
	t.Helper()
	return decodeSub2APIData([]byte(`{"code":500,"message":"database is locked"}`), &struct{}{})
}
