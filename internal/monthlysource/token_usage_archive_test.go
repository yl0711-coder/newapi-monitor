package monthlysource

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestTokenUsageArchiveRebuildCanonicalAndRejectsAssertions(t *testing.T) {
	req, pairs, now := tokenMonthFixture()
	archive := TokenUsageMonthArchive{Version: 1, Basis: TokenUsageArchiveBasis, SelectionSHA256: strings.Repeat("c", 64), SourceIdentity: req.SourceIdentity, Rules: req.Rules, UserID: req.UserIDs[0], Month: req.Month, Pairs: pairs}
	raw, _ := json.Marshal(archive)
	decoded, report, err := DecodeTokenUsageMonthArchive(context.Background(), raw, now)
	expected, expectedErr := SummarizeTokenUsageMonth(context.Background(), req, pairs, now)
	if err != nil || expectedErr != nil || !reflect.DeepEqual(archive, decoded) || !reflect.DeepEqual(report, expected) {
		t.Fatal("original archive report differs", err, expectedErr)
	}
	for _, bad := range [][]byte{
		bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":2`), 1),
		bytes.Replace(raw, []byte(TokenUsageArchiveBasis), []byte("approved"), 1),
		bytes.Replace(raw, []byte(`"selection_sha256":"`+strings.Repeat("c", 64)+`"`), []byte(`"selection_sha256":"invalid"`), 1),
		append(append([]byte{}, raw[:len(raw)-1]...), []byte(`,"approved":true}`)...),
		append(append([]byte{}, raw...), byte(' ')),
		bytes.Replace(raw, []byte(`"user_id":201`), []byte(`"user_id":202`), 1),
		bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
	} {
		if _, _, err := DecodeTokenUsageMonthArchive(context.Background(), bad, now); err == nil {
			t.Fatal("invalid or extra archive assertions accepted")
		}
	}
}
