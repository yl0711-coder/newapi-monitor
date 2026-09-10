package ecsarchive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type s3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

type S3 struct {
	cfg    Config
	client s3API
}

func (s *S3) Binding() string {
	return s.cfg.Account + "/" + s.cfg.Region + "/" + s.cfg.Bucket + "/" + s.cfg.Prefix
}

func NewS3(config Config, cfg aws.Config) (*S3, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	cfg.HTTPClient = &http.Client{Transport: transport, Timeout: 8 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = config.Region
		// Do not honor custom endpoints from ambient AWS environment settings.
		o.BaseEndpoint = aws.String("https://s3." + config.Region + ".amazonaws.com")
		o.EndpointResolverV2 = s3.NewDefaultEndpointResolverV2()
		o.RetryMaxAttempts = 2
	})
	return &S3{config, client}, nil
}

func (s *S3) checkKey(key string) error { _, _, _, _, err := ParseKey(s.cfg.Prefix, key); return err }

func (s *S3) Put(ctx context.Context, key string, body []byte) error {
	if s.checkKey(key) != nil || len(body) == 0 || len(body) > MaxObject {
		return errors.New("invalid bounded archive object")
	}
	sum := sha256.Sum256(body)
	checksum := base64.StdEncoding.EncodeToString(sum[:])
	out, err := s.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))), ContentType: aws.String("application/json"), IfNoneMatch: aws.String("*"), ExpectedBucketOwner: aws.String(s.cfg.Account), ServerSideEncryption: types.ServerSideEncryptionAes256, ChecksumSHA256: aws.String(checksum)})
	var api smithy.APIError
	if errors.As(err, &api) && api.ErrorCode() == "PreconditionFailed" {
		return ErrExists
	}
	if err != nil {
		return errors.New("S3 archive write unavailable")
	}
	if out == nil || aws.ToString(out.ETag) == "" || aws.ToString(out.ChecksumSHA256) != checksum {
		return errors.New("S3 archive acknowledgement incomplete")
	}
	return nil
}

func (s *S3) Get(ctx context.Context, key string) (Object, error) {
	if err := s.checkKey(key); err != nil {
		return Object{}, err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.cfg.Bucket), Key: aws.String(key), ExpectedBucketOwner: aws.String(s.cfg.Account)})
	if err != nil {
		return Object{}, errors.New("S3 archive read unavailable")
	}
	if out == nil || out.Body == nil {
		return Object{}, errors.New("empty archive response")
	}
	defer out.Body.Close()
	if out.LastModified == nil || out.LastModified.IsZero() || aws.ToString(out.ETag) == "" || out.ContentLength == nil || *out.ContentLength < 1 || *out.ContentLength > MaxObject {
		return Object{}, errors.New("invalid archive metadata")
	}
	body, err := io.ReadAll(io.LimitReader(out.Body, MaxObject+1))
	if err != nil || len(body) > MaxObject || int64(len(body)) != *out.ContentLength {
		return Object{}, errors.New("archive body truncated or oversized")
	}
	return Object{Key: key, ETag: *out.ETag, Modified: *out.LastModified, Body: body}, nil
}

func (s *S3) List(ctx context.Context, token string) (Page, error) {
	if len(token) > 4096 {
		return Page{}, errors.New("archive cursor exceeds budget")
	}
	in := &s3.ListObjectsV2Input{Bucket: aws.String(s.cfg.Bucket), Prefix: aws.String(s.cfg.Prefix), ExpectedBucketOwner: aws.String(s.cfg.Account), MaxKeys: aws.Int32(PageSize)}
	if token != "" {
		in.ContinuationToken = aws.String(token)
	}
	out, err := s.client.ListObjectsV2(ctx, in)
	if err != nil || out == nil {
		return Page{}, errors.New("S3 archive listing unavailable")
	}
	if len(out.Contents) > PageSize {
		return Page{}, errors.New("archive list exceeds page budget")
	}
	page := Page{Entries: make([]Entry, 0, len(out.Contents))}
	for _, o := range out.Contents {
		if !strings.HasPrefix(aws.ToString(o.Key), s.cfg.Prefix) || o.LastModified == nil || o.Size == nil || *o.Size < 0 {
			return Page{}, errors.New("archive listing metadata incomplete")
		}
		page.Entries = append(page.Entries, Entry{Key: aws.ToString(o.Key), ETag: aws.ToString(o.ETag), Modified: *o.LastModified, Size: *o.Size})
	}
	if aws.ToBool(out.IsTruncated) {
		page.Next = aws.ToString(out.NextContinuationToken)
		if page.Next == "" || page.Next == token || len(page.Next) > 4096 {
			return Page{}, errors.New("archive pagination did not progress")
		}
	}
	return page, nil
}
