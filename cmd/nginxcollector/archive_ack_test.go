package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestArchiveReceiptRequiresPrivateTransportAndExactBatch(t *testing.T) {
	body := []byte(`{"batch_id":"fixture"}`)
	sum := sha256.Sum256(body)
	valid := map[string]any{"version": 1, "state": "durably_archived", "node": "ecs-fixture", "lane": "access", "batch_id": "fixture", "body_sha256": hex.EncodeToString(sum[:])}
	c := config{ecsSocket: "/private/fixture.sock"}
	check := func(c config, raw []byte) error {
		return validateArchiveReceipt(c, "access", "ecs-fixture", "fixture", body, raw)
	}
	raw, _ := json.Marshal(valid)
	if err := check(c, raw); err != nil {
		t.Fatal(err)
	}
	if check(config{}, raw) == nil {
		t.Fatal("Lightsail accepted archive receipt")
	}
	for _, field := range []string{"version", "state", "node", "lane", "batch_id", "body_sha256"} {
		t.Run(field, func(t *testing.T) {
			copy := make(map[string]any)
			for k, v := range valid {
				copy[k] = v
			}
			copy[field] = "wrong"
			raw, _ := json.Marshal(copy)
			if check(c, raw) == nil {
				t.Fatal("mismatched receipt accepted")
			}
		})
	}
	for _, bad := range [][]byte{[]byte(`{"ok":true}`), []byte(`null`), append(raw, 'x'), []byte(strings.Repeat(" ", 4097))} {
		if check(c, bad) == nil {
			t.Fatal("invalid receipt accepted")
		}
	}
}
