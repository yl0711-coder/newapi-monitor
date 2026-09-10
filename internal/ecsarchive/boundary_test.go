package ecsarchive

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"strings"
	"testing"
)

func TestFinalBoundaryIsCanonicalBoundedAndSigned(t *testing.T) {
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	node := "ecs-" + strings.Repeat("a", 48)
	good := FinalBoundary{ObservedAt: 123, Files: []FinalFile{{Name: "new-api.log", Inode: 1, Size: 20, Offset: 20, SHA256: strings.Repeat("b", 64)}}}
	e, err := SealBoundaryClosure("fixture-audience", node, "reject", "", &good, key)
	if err != nil || !e.Verify(pub) {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(e)
	if _, err := Decode(raw); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*FinalBoundary){
		func(b *FinalBoundary) { b.ObservedAt = 0 },
		func(b *FinalBoundary) { b.Files = nil },
		func(b *FinalBoundary) { b.Files = append(b.Files, b.Files[0]) },
		func(b *FinalBoundary) { b.Files[0].Offset-- },
		func(b *FinalBoundary) { b.Files[0].Size = -1; b.Files[0].Offset = -1 },
		func(b *FinalBoundary) { b.Files[0].Inode = 0 },
		func(b *FinalBoundary) { b.Files[0].Name = "../secret" },
		func(b *FinalBoundary) { b.Files[0].SHA256 = "invalid" },
	} {
		b := good
		b.Files = append([]FinalFile(nil), good.Files...)
		mutate(&b)
		if _, err := SealBoundaryClosure("fixture-audience", node, "reject", "", &b, key); err == nil {
			t.Fatalf("invalid boundary accepted %+v", b)
		}
	}
	e.Body = BoundaryBody(node, nil)
	e.Hash = digest(e.Body)
	if e.Verify(pub) {
		t.Fatal("removing final proof retained signature validity")
	}
}
