package ecsarchive

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func testEnvelope(t *testing.T) Envelope {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	node := "ecs-" + strings.Repeat("a", 48)
	body := []byte("{\n  \"node\":\"" + node + "\", \"batch_id\":\"fixture-batch-1\"\n}\n")
	e, err := Seal("fixture-audience", node, "reject", body, key)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(raw)
	if err != nil || !bytes.Equal(decoded.Body, body) || !decoded.Verify(key.Public().(ed25519.PublicKey)) {
		t.Fatalf("raw bytes/signature changed: %v", err)
	}
	return e
}

func TestEnvelopeBytesSignatureAndKeyIsolation(t *testing.T) {
	e := testEnvelope(t)
	key, err := ObjectKey("isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("b", 32), e)
	if err != nil {
		t.Fatal(err)
	}
	owner, node, lane, hash, err := ParseKey("isolated/", key)
	if err != nil || OwnerTask(owner) != strings.Repeat("b", 32) || node != e.Node || lane != e.Lane || hash != e.Hash {
		t.Fatal("key roundtrip mismatch")
	}
	for _, bad := range []string{"../" + key, strings.Replace(key, "isolated/", "other/", 1), strings.Replace(key, "/reject/", "/arbitrary/", 1)} {
		if _, _, _, _, err := ParseKey("isolated/", bad); err == nil {
			t.Fatal("unsafe key accepted")
		}
	}
	_, otherKey, _ := ed25519.GenerateKey(rand.Reader)
	if e.Verify(otherKey.Public().(ed25519.PublicKey)) {
		t.Fatal("wrong signer accepted")
	}
	e.Body = append(e.Body, ' ')
	if e.Verify(otherKey.Public().(ed25519.PublicKey)) {
		t.Fatal("tampered bytes accepted")
	}
}

type s3Fixture struct {
	put    *s3.PutObjectInput
	get    *s3.GetObjectInput
	list   *s3.ListObjectsV2Input
	exists bool
	raw    []byte
	badAck bool
}

func (f *s3Fixture) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.put = in
	if f.exists {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed"}
	}
	checksum := in.ChecksumSHA256
	if f.badAck {
		checksum = nil
	}
	return &s3.PutObjectOutput{ETag: aws.String("fixture-etag"), ChecksumSHA256: checksum}, nil
}
func (f *s3Fixture) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	f.get = in
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(f.raw)), ContentLength: aws.Int64(int64(len(f.raw))), LastModified: aws.Time(time.Now()), ETag: aws.String("fixture-etag")}, nil
}
func (f *s3Fixture) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.list = in
	return &s3.ListObjectsV2Output{}, nil
}

func TestS3ConditionalOwnerEncryptedBoundedContract(t *testing.T) {
	e := testEnvelope(t)
	key, _ := ObjectKey("isolated/", "AROAABCDEFGHIJKLMNOPQ:"+strings.Repeat("b", 32), e)
	raw, _ := json.Marshal(e)
	fake := &s3Fixture{raw: raw}
	store := &S3{cfg: Config{Bucket: "fixture-bucket", Prefix: "isolated/", Account: "123456789012", Region: "us-west-2"}, client: fake}
	ctx := context.Background()
	if err := store.Put(ctx, key, raw); err != nil {
		t.Fatal(err)
	}
	p := fake.put
	if aws.ToString(p.IfNoneMatch) != "*" || aws.ToString(p.ExpectedBucketOwner) != store.cfg.Account || p.ServerSideEncryption != types.ServerSideEncryptionAes256 || aws.ToString(p.ChecksumSHA256) == "" {
		t.Fatal("missing immutable/owner/checksum/encryption constraint")
	}
	fake.exists = true
	if err := store.Put(ctx, key, raw); !errors.Is(err, ErrExists) {
		t.Fatalf("create-only collision %v", err)
	}
	fake.exists = false
	fake.badAck = true
	if store.Put(ctx, key, raw) == nil {
		t.Fatal("unconfirmed persistence acknowledged")
	}
	if _, err := store.Get(ctx, key); err != nil {
		t.Fatal(err)
	}
	if aws.ToString(fake.get.ExpectedBucketOwner) != store.cfg.Account {
		t.Fatal("read lacks owner constraint")
	}
	if _, err := store.List(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if aws.ToString(fake.list.ExpectedBucketOwner) != store.cfg.Account || aws.ToString(fake.list.Prefix) != "isolated/" || aws.ToInt32(fake.list.MaxKeys) != PageSize {
		t.Fatal("unbounded/unscoped list")
	}
	if store.Put(ctx, "wrong-prefix", raw) == nil {
		t.Fatal("unscoped write")
	}
	if _, err := store.List(ctx, strings.Repeat("x", 4097)); err == nil {
		t.Fatal("unbounded cursor")
	}
	for _, c := range []Config{{}, {Bucket: "fixture-bucket", Prefix: "../", Account: "123456789012", Region: "us-west-2"}} {
		if _, err := NewS3(c, aws.Config{}); err == nil {
			t.Fatal("unsafe store config")
		}
	}
}
