package indexer

import (
	"context"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Poller lists new NDJSON segments across all configured S3 buckets.
// Each bucket is polled concurrently, resuming from the per-bucket cursor.
type Poller struct {
	s3  *s3.Client
	cfg *BuilderConfig
}

// NewPoller creates a Poller with a shared S3 client.
func NewPoller(cfg *BuilderConfig) (*Poller, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.S3Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, ""),
		),
	)
	if err != nil {
		return nil, err
	}

	cli := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		//nolint:staticcheck // matches existing write-service pattern
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

	return &Poller{s3: cli, cfg: cfg}, nil
}

// Poll lists new segments from all 6 buckets concurrently, starting after each
// bucket's cursor. Returns a globally sorted slice (timestamp, writer, segment).
func (p *Poller) Poll(ctx context.Context, cursors *CursorState) ([]Segment, error) {
	type result struct {
		segs []Segment
		err  error
	}

	results := make([]result, len(p.cfg.Buckets))
	var wg sync.WaitGroup

	for i, bucket := range p.cfg.Buckets {
		wg.Add(1)
		go func(idx int, bkt string) {
			defer wg.Done()
			segs, err := p.pollBucket(ctx, bkt, cursors.Get(bkt))
			results[idx] = result{segs: segs, err: err}
		}(i, bucket)
	}
	wg.Wait()

	var all []Segment
	for i, r := range results {
		if r.err != nil {
			log.Printf("[poller] bucket %s error: %v", p.cfg.Buckets[i], r.err)
			continue
		}
		all = append(all, r.segs...)
	}

	SortSegments(all)
	return all, nil
}

// pollBucket pages through ListObjectsV2 for a single bucket.
func (p *Poller) pollBucket(ctx context.Context, bucket, cursor string) ([]Segment, error) {
	var segments []Segment
	var contToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(p.cfg.SegmentPrefix),
			MaxKeys: aws.Int32(1000),
		}
		if contToken != nil {
			input.ContinuationToken = contToken
		} else if cursor != "" {
			input.StartAfter = aws.String(cursor)
		}

		resp, err := p.s3.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, err
		}

		for _, obj := range resp.Contents {
			if obj.Key == nil {
				continue
			}
			var size int64
			if obj.Size != nil {
				size = *obj.Size
			}
			seg, err := ParseSegmentKey(bucket, *obj.Key, size)
			if err != nil {
				log.Printf("[poller] skip invalid key %s/%s: %v", bucket, *obj.Key, err)
				continue
			}
			segments = append(segments, seg)
		}

		if resp.NextContinuationToken == nil {
			break
		}
		contToken = resp.NextContinuationToken
	}

	return segments, nil
}

// S3Client returns the underlying S3 client for use by the processor and builder.
func (p *Poller) S3Client() *s3.Client {
	return p.s3
}

// EnsureBucket creates a bucket if it does not exist.
func (p *Poller) EnsureBucket(ctx context.Context, bucket string) error {
	_, err := p.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return nil
	}
	log.Printf("[poller] creating bucket %s", bucket)
	_, err = p.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	return err
}
