// Package ecsarchive preserves frozen protocol bodies outside the ECS task.
// Archive signatures are never accepted by the live ingestion HTTP routes.
package ecsarchive

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
)

const MaxBody = 2 << 20
const MaxObject = 3 << 20
const PageSize = 100

var nodePattern = regexp.MustCompile(`^ecs-[a-f0-9]{48}$`)
var hashPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var ownerPattern = regexp.MustCompile(`^AROA[A-Z0-9]{12,60}:[a-f0-9]{32}$`)
var prefixPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,62}/$`)
var bucketPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{1,61}[a-z0-9]$`)
var accountPattern = regexp.MustCompile(`^[0-9]{12}$`)
var regionPattern = regexp.MustCompile(`^[a-z]{2}-[a-z]+-[0-9]$`)

type Config struct{ Bucket, Prefix, Region, Account string }

func (c Config) Validate() error {
	if !bucketPattern.MatchString(c.Bucket) || !prefixPattern.MatchString(c.Prefix) || !accountPattern.MatchString(c.Account) || !regionPattern.MatchString(c.Region) {
		return errors.New("archive requires an explicit private regional bucket, account and dedicated single-segment prefix")
	}
	return nil
}

func Lane(lane string) bool {
	return lane == "access" || lane == "error" || lane == "evidence" || lane == "reject"
}

// Body is base64-encoded by JSON, preserving the exact bytes (RawMessage would
// compact whitespace and corrupt existing byte-sensitive receipt hashes).
type Envelope struct {
	Version   int    `json:"version"`
	Kind      string `json:"kind,omitempty"`
	Audience  string `json:"audience"`
	Node      string `json:"node"`
	Lane      string `json:"lane"`
	Body      []byte `json:"body"`
	Hash      string `json:"body_sha256"`
	Signature string `json:"signature"`
	Previous  string `json:"previous_body_sha256"`
}

func digest(body []byte) string { sum := sha256.Sum256(body); return hex.EncodeToString(sum[:]) }
func (e Envelope) message() []byte {
	if e.Version == 2 {
		return []byte("monitor-ecs-archive-v2\n" + e.Kind + "\n" + e.Audience + "\n" + e.Node + "\n" + e.Lane + "\n" + e.Hash + "\n" + e.Previous)
	}
	return []byte("monitor-ecs-archive-v1\n" + e.Audience + "\n" + e.Node + "\n" + e.Lane + "\n" + e.Hash + "\n" + e.Previous)
}

func Seal(audience, node, lane string, body []byte, key ed25519.PrivateKey) (Envelope, error) {
	return SealAfter(audience, node, lane, body, "", key)
}

func SealAfter(audience, node, lane string, body []byte, previous string, key ed25519.PrivateKey) (Envelope, error) {
	e := Envelope{Version: 1, Audience: audience, Node: node, Lane: lane, Body: append([]byte(nil), body...), Hash: digest(body), Previous: previous}
	if !e.valid() || len(key) != ed25519.PrivateKeySize {
		return Envelope{}, errors.New("invalid frozen archive source/body")
	}
	e.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(key, e.message()))
	return e, nil
}

func (e Envelope) valid() bool {
	if e.Previous != "" && (!hashPattern.MatchString(e.Previous) || e.Previous == e.Hash) {
		return false
	}
	var payload struct {
		Node    string `json:"node"`
		BatchID string `json:"batch_id"`
	}
	versionOK := e.Version == 1 && e.Kind == ""
	if e.Version == 2 && e.Kind == CollectorClosure {
		_, closureErr := ReadClosure(e.Body, e.Node, e.Lane)
		versionOK = closureErr == nil
	}
	return versionOK && len(e.Audience) >= 8 && len(e.Audience) <= 128 && !strings.ContainsAny(e.Audience, "\r\n") && nodePattern.MatchString(e.Node) && Lane(e.Lane) && len(e.Body) > 0 && len(e.Body) <= MaxBody && e.Hash == digest(e.Body) && json.Unmarshal(e.Body, &payload) == nil && payload.Node == e.Node && payload.BatchID != "" && len(payload.BatchID) <= 128
}

func (e Envelope) Verify(key ed25519.PublicKey) bool {
	sig, err := base64.StdEncoding.DecodeString(e.Signature)
	return e.valid() && err == nil && len(key) == ed25519.PublicKeySize && ed25519.Verify(key, e.message(), sig)
}

func Decode(raw []byte) (Envelope, error) {
	var e Envelope
	if len(raw) > MaxObject {
		return e, errors.New("archive object exceeds budget")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	var tail any
	if d.Decode(&e) != nil || !errors.Is(d.Decode(&tail), io.EOF) || !e.valid() {
		return Envelope{}, errors.New("invalid archive envelope")
	}
	return e, nil
}

func OwnerTask(owner string) string {
	if !ownerPattern.MatchString(owner) {
		return ""
	}
	return strings.Split(owner, ":")[1]
}

func ObjectKey(prefix, owner string, e Envelope) (string, error) {
	if !prefixPattern.MatchString(prefix) || OwnerTask(owner) == "" || !e.valid() {
		return "", errors.New("invalid archive key identity")
	}
	return prefix + owner + "/" + e.Node + "/" + e.Lane + "/" + e.Hash + ".json", nil
}

// ParseKey is structural only. Trust also requires verified S3 provenance,
// source registration/public key and the task-role-scoped bucket policy.
func ParseKey(prefix, key string) (owner, node, lane, hash string, err error) {
	if !prefixPattern.MatchString(prefix) || !strings.HasPrefix(key, prefix) {
		err = errors.New("archive key outside configured prefix")
		return
	}
	p := strings.Split(strings.TrimPrefix(key, prefix), "/")
	if len(p) != 4 || OwnerTask(p[0]) == "" || !nodePattern.MatchString(p[1]) || !Lane(p[2]) || !strings.HasSuffix(p[3], ".json") || !hashPattern.MatchString(strings.TrimSuffix(p[3], ".json")) {
		err = errors.New("invalid archive key")
		return
	}
	return p[0], p[1], p[2], strings.TrimSuffix(p[3], ".json"), nil
}
