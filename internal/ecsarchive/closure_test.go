package ecsarchive

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"strings"
	"testing"
)

func TestClosureSignatureDomainAndCanonicalBody(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	e, err := SealClosure("fixture-audience", "ecs-"+strings.Repeat("a", 48), "reject", strings.Repeat("b", 64), key)
	if err != nil || !e.Verify(pub) {
		t.Fatal("closure verification", err)
	}
	raw, _ := json.Marshal(e)
	d, err := Decode(raw)
	if err != nil || !d.Verify(pub) {
		t.Fatal("closure decode", err)
	}
	for _, mutate := range []func(*Envelope){
		func(e *Envelope) { e.Version = 1; e.Kind = "" },
		func(e *Envelope) { e.Kind = "different" },
		func(e *Envelope) { e.Previous = strings.Repeat("c", 64) },
		func(e *Envelope) { e.Lane = "access" },
		func(e *Envelope) { e.Body = append(e.Body, ' '); e.Hash = digest(e.Body) },
	} {
		changed := e
		mutate(&changed)
		if changed.Verify(pub) {
			t.Fatal("tampered closure accepted")
		}
	}
	legacy, err := Seal("fixture-audience", e.Node, e.Lane, ClosureBody(e.Node), key)
	if err != nil || legacy.Kind != "" || legacy.Version != 1 {
		t.Fatal("legacy envelope changed", err)
	}
}
