// Package s3store wraps the AWS SDK S3 client for the write service.
package s3store

import (
	"bytes"
	"context"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"write-service/config"
)

// Client wraps an S3 client bound to a single bucket.
type Client struct {
	bucket string
	s3     *s3.Client
}

// New dials S3/MinIO for the given config and ensures the bucket exists.
func New(ctx context.Context, cfg *config.Config) (*Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.S3Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("s3store: load config: %w", err)
	}

	cli := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		//nolint:staticcheck
		o.EndpointResolver = s3.EndpointResolverFunc(
			func(_ string, _ s3.EndpointResolverOptions) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               cfg.S3Endpoint,
					HostnameImmutable: true,
					SigningRegion:     cfg.S3Region,
				}, nil
			},
		)
		o.UsePathStyle = true
	})

	c := &Client{bucket: cfg.S3Bucket, s3: cli}
	if err := c.ensureBucket(ctx); err != nil {
		return nil, err
	}

	log.Printf("[s3store] ready — bucket=%s endpoint=%s", cfg.S3Bucket, cfg.S3Endpoint)
	return c, nil
}

// PutObject uploads data to key with the given content type and metadata.
func (c *Client) PutObject(ctx context.Context, key, contentType string, data []byte, meta map[string]string) error {
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
		Metadata:    meta,
	})
	if err != nil {
		return fmt.Errorf("s3store: put %q: %w", key, err)
	}
	return nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func (c *Client) ensureBucket(ctx context.Context) error {
	_, err := c.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(c.bucket)})
	if err == nil {
		return nil
	}

	log.Printf("[s3store] bucket %q not found — creating", c.bucket)
	if _, err := c.s3.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(c.bucket),
	}); err != nil {
		return fmt.Errorf("s3store: create bucket %q: %w", c.bucket, err)
	}
	return nil
}