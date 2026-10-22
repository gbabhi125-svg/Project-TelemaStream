package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/telemastream/processing-engine/internal/model"
)

// ColdStore writes raw readings to object storage, partitioned by date,
// standing in for the Parquet-on-S3 cold path in the Lambda architecture.
// Readings are newline-delimited JSON, gzip-compressed; swapping the
// encoder for a columnar Parquet writer (e.g. github.com/xitongsys/parquet-go)
// is the direct productionization step and does not change this type's
// interface.
type ColdStore struct {
	client *minio.Client
	bucket string
}

func NewColdStore(endpoint, accessKey, secretKey, bucket string) (*ColdStore, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}
	return &ColdStore{client: client, bucket: bucket}, nil
}

func (c *ColdStore) EnsureBucket(ctx context.Context) error {
	exists, err := c.client.BucketExists(ctx, c.bucket)
	if err != nil {
		return err
	}
	if !exists {
		return c.client.MakeBucket(ctx, c.bucket, minio.MakeBucketOptions{})
	}
	return nil
}

// WriteBatch uploads one gzip object per flush, keyed by the date/hour
// partition convention the brief calls for: /year=YYYY/month=MM/day=DD/.
func (c *ColdStore) WriteBatch(ctx context.Context, readings []model.Reading) error {
	if len(readings) == 0 {
		return nil
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	enc := json.NewEncoder(gz)
	for _, r := range readings {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}
	if err := gz.Close(); err != nil {
		return err
	}

	now := time.Now().UTC()
	key := fmt.Sprintf("year=%04d/month=%02d/day=%02d/batch-%d.json.gz",
		now.Year(), now.Month(), now.Day(), now.UnixNano())

	_, err := c.client.PutObject(ctx, c.bucket, key, &buf, int64(buf.Len()), minio.PutObjectOptions{
		ContentType:     "application/gzip",
		ContentEncoding: "gzip",
	})
	return err
}
