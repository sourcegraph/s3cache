// Package s3cache provides an implementation of httpcache.Cache that stores and
// retrieves data using Amazon S3.
package s3cache

import (
	"crypto/md5"
	"encoding/hex"
	"github.com/sqs/s3"
	"github.com/sqs/s3/s3util"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

// Cache objects store and retrieve data using Amazon S3.
type Cache struct {
	// Config is the Amazon S3 configuration.
	Config s3util.Config

	// BucketURL is the URL to the bucket on Amazon S3, which includes the
	// bucket name and the AWS region. Example:
	// "https://s3-us-west-2.amazonaws.com/mybucket".
	BucketURL string
}

func (c *Cache) Get(key string) (resp []byte, ok bool) {
	rdr, err := s3util.Open(c.url(key), &c.Config)
	if err != nil {
		return []byte{}, false
	}
	defer rdr.Close()
	resp, err = ioutil.ReadAll(rdr)
	return resp, err == nil
}

func (c *Cache) Set(key string, resp []byte) {
	w, err := s3util.Create(c.url(key), nil, &c.Config)
	if err != nil {
		return
	}
	w.Write(resp)
	defer w.Close()
}

func (c *Cache) Delete(key string) {
	rdr, err := s3util.Delete(c.url(key), &c.Config)
	if err != nil {
		return
	}
	defer rdr.Close()
}

func (c *Cache) url(key string) string {
	key = cacheKeyToObjectKey(key)
	if strings.HasSuffix(c.BucketURL, "/") {
		return c.BucketURL + key
	}
	return c.BucketURL + "/" + key
}

func cacheKeyToObjectKey(key string) string {
	h := md5.New()
	io.WriteString(h, key)
	return hex.EncodeToString(h.Sum(nil))
}

// New returns a new Cache with underlying storage in Amazon S3. The bucketURL
// is the full URL to the bucket on Amazon S3, including the bucket name and AWS
// region (e.g., "https://s3-us-west-2.amazonaws.com/mybucket").
//
// The environment variables S3_ACCESS_KEY and S3_SECRET_KEY are used as the AWS
// credentials. To use different credentials, modify the returned Cache object
// or construct a Cache object manually.
func New(bucketURL string) *Cache {
	return &Cache{
		Config: s3util.Config{
			Keys: &s3.Keys{
				AccessKey: os.Getenv("S3_ACCESS_KEY"),
				SecretKey: os.Getenv("S3_SECRET_KEY"),
			},
			Service: s3.DefaultService,
		},
		BucketURL: bucketURL,
	}
}
