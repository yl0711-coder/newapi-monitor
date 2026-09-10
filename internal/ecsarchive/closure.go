package ecsarchive

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
)

// CollectorClosure closes only the collector's frozen batch chain. It is NOT
// proof that a business producer stopped, that every file was seen, or that an
// entire request interval is complete. V1 signatures/bytes remain unchanged.
const CollectorClosure = "collector-closure"

func ClosureBody(node string) []byte {
	return BoundaryBody(node, nil)
}

func SealClosure(audience, node, lane, previous string, key ed25519.PrivateKey) (Envelope, error) {
	return SealBoundaryClosure(audience, node, lane, previous, nil, key)
}

func SealBoundaryClosure(audience, node, lane, previous string, boundary *FinalBoundary, key ed25519.PrivateKey) (Envelope, error) {
	body := BoundaryBody(node, boundary)
	e := Envelope{Version: 2, Kind: CollectorClosure, Audience: audience, Node: node,
		Lane: lane, Body: body, Hash: digest(body), Previous: previous}
	if !e.valid() || len(key) != ed25519.PrivateKeySize {
		return Envelope{}, errors.New("invalid collector closure")
	}
	e.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(key, e.message()))
	return e, nil
}
