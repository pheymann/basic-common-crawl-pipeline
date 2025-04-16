package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/aleph-alpha/case-study/basic-common-crawl-pipeline/golang/common"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MiniOStorage struct {
	client *minio.Client
	bucket string
}

func NewMiniOStorage(endpoint, accessKey, secretKey, bucket string) (*MiniOStorage, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to check bucket '%s' exists: %w", bucket, err)
	}
	if !exists {
		return nil, fmt.Errorf("bucket %s does not exist", bucket)
	}

	return &MiniOStorage{client: client, bucket: bucket}, nil
}

func (s *MiniOStorage) SaveDocument(doc common.Document) error {
	// Create SHA-256 hash of the SurtURL to limit the size of the key
	hash := sha256.Sum256([]byte(doc.URL.SurtURL))
	// Encode the hash as a hex string so we don't create issues like nested folders
	key := hex.EncodeToString(hash[:])

	jsonDoc, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to marshal document: %w", err)
	}

	_, err = s.client.PutObject(context.Background(), s.bucket, key, bytes.NewReader(jsonDoc), int64(len(jsonDoc)), minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to save document: %w", err)
	}

	return nil
}
