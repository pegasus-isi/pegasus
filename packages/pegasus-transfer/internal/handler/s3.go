package handler

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	ini "github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/creds"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/s3uri"
)

// S3Handler implements S3 and S3-compatible transfers natively via
// aws-sdk-go-v2, replacing transfer.py's S3Handler callouts to the (now
// removed) pegasus-s3 tool. The s3cfg/credentials.conf config format,
// s3://user@site/bucket/key URL scheme, and PEGASUS_CREDENTIALS[_site]
// resolution are all unchanged (frozen input contract).
//
// Checksum validation defaults to "when required" (not "when supported") so
// aws-sdk-go-v2's newer flexible-checksum behavior doesn't break
// S3-compatible endpoints (MinIO, Ceph) that don't implement it.
type S3Handler struct {
	Base
	Hooks

	mu      sync.Mutex
	clients map[string]*s3.Client // keyed by "site|ident"
}

func NewS3Handler(hooks Hooks) *S3Handler {
	return &S3Handler{
		Base: Base{
			HandlerName: "S3Handler",
			ProtocolMap: []string{
				"file->s3", "file->s3s", "s3->file", "s3s->file",
				"s3->s3", "s3->s3s", "s3s->s3", "s3s->s3s",
			},
			MkdirCleanupProtocols: []string{"s3", "s3s"},
		},
		Hooks:   hooks,
		clients: map[string]*s3.Client{},
	}
}

// configForSite mirrors S3Handler._s3_cred_env followed by s3.py's
// get_config(): resolve PEGASUS_CREDENTIALS[_site] (auto-fixing weak
// permissions), then load that exact file as the S3 config.
func configForSite(siteLabel string) (*ini.INI, error) {
	path, err := ini.S3CredEnvPath(siteLabel)
	if err != nil {
		return nil, err
	}
	cfg, _, err := ini.LoadS3Config(path)
	return cfg, err
}

func (h *S3Handler) client(cfg *ini.INI, uri s3uri.URI) (*s3.Client, error) {
	if !cfg.HasSection(uri.Site) {
		return nil, fmt.Errorf("config file has no section for site '%s'", uri.Site)
	}
	if !cfg.HasSection(uri.Ident) {
		return nil, fmt.Errorf("config file has no section for identity '%s'", uri.Ident)
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	key := uri.Site + "|" + uri.Ident
	if c, ok := h.clients[key]; ok {
		return c, nil
	}

	endpoint, _ := cfg.Get(uri.Site, "endpoint")
	region := cfg.GetDefault(uri.Site, "region", "us-east-1")
	accessKey, _ := cfg.Get(uri.Ident, "access_key")
	secretKey, _ := cfg.Get(uri.Ident, "secret_key")

	pathStyle := !strings.Contains(endpoint, "amazonaws.com")
	if style, ok := cfg.Get(uri.Site, "addressing_style"); ok {
		pathStyle = style == "path"
	}

	awsCfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		// Conservative checksum handling so S3-compatible endpoints that
		// don't implement the newer flexible-checksum feature keep working.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}

	c := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = pathStyle
	})
	h.clients[key] = c
	return c, nil
}

func (h *S3Handler) DoMkdirs(ctx context.Context, mkdirs []*model.Mkdir) Result {
	var res Result
	for _, m := range mkdirs {
		if err := h.mkdirOne(ctx, m); err != nil {
			h.logger().Error("s3 mkdir failed", "url", m.URL(), "error", err)
			res.Failed = append(res.Failed, m)
			continue
		}
		res.Succeeded = append(res.Succeeded, m)
	}
	return res
}

func (h *S3Handler) mkdirOne(ctx context.Context, m *model.Mkdir) error {
	uri, err := s3uri.Parse(m.URL())
	if err != nil {
		return err
	}
	cfg, err := configForSite(m.SiteLabel())
	if err != nil {
		return err
	}
	c, err := h.client(cfg, uri)
	if err != nil {
		return err
	}
	return createBucketIfMissing(ctx, c, uri.Bucket, cfg.GetDefault(uri.Site, "region", ""))
}

func createBucketIfMissing(ctx context.Context, c *s3.Client, bucket, region string) error {
	_, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return nil // already exists
	}
	input := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if region != "" && region != "us-east-1" {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		}
	}
	_, err = c.CreateBucket(ctx, input)
	return err
}

func (h *S3Handler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	var res Result
	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()

		if err := h.transferOne(ctx, t); err != nil {
			h.logger().Error("s3 transfer failed", "src", t.SrcURL(), "dst", t.DstURL(), "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}

		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

func isS3(proto string) bool { return proto == "s3" || proto == "s3s" }

func (h *S3Handler) transferOne(ctx context.Context, t *model.Transfer) error {
	switch {
	case isS3(t.SrcProto()) && isS3(t.DstProto()):
		return h.copyS3ToS3(ctx, t)
	case t.DstProto() == "file":
		return h.get(ctx, t)
	default:
		if t.SrcProto() == "file" && !VerifyLocalFile(t.SrcPath()) {
			return fmt.Errorf("source file does not exist or is not readable: %s", t.SrcPath())
		}
		return h.put(ctx, t)
	}
}

func (h *S3Handler) copyS3ToS3(ctx context.Context, t *model.Transfer) error {
	srcURI, err := s3uri.Parse(t.SrcURL())
	if err != nil {
		return err
	}
	dstURI, err := s3uri.Parse(t.DstURL())
	if err != nil {
		return err
	}
	srcCfg, err := configForSite(t.SrcSiteLabel())
	if err != nil {
		return err
	}
	srcClient, err := h.client(srcCfg, srcURI)
	if err != nil {
		return err
	}

	if srcURI.Ident == dstURI.Ident && srcURI.Site == dstURI.Site {
		// Same account/endpoint: a server-side CopyObject.
		_, err := srcClient.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(dstURI.Bucket),
			Key:        aws.String(dstURI.Key),
			CopySource: aws.String(srcURI.Bucket + "/" + srcURI.Key),
		})
		return err
	}

	// Cross-endpoint copy: stream get -> put through a pipe, no local
	// staging file needed.
	dstCfg, err := configForSite(t.DstSiteLabel())
	if err != nil {
		return err
	}
	dstClient, err := h.client(dstCfg, dstURI)
	if err != nil {
		return err
	}

	pr, pw := io.Pipe()
	uploader := manager.NewUploader(dstClient)
	downloadErrCh := make(chan error, 1)
	go func() {
		obj, err := srcClient.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(srcURI.Bucket), Key: aws.String(srcURI.Key)})
		if err != nil {
			downloadErrCh <- err
			pw.CloseWithError(err)
			return
		}
		defer obj.Body.Close()
		_, copyErr := io.Copy(pw, obj.Body)
		downloadErrCh <- copyErr
		pw.CloseWithError(copyErr)
	}()
	_, uploadErr := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(dstURI.Bucket),
		Key:    aws.String(dstURI.Key),
		Body:   pr,
	})
	if downloadErr := <-downloadErrCh; downloadErr != nil {
		return downloadErr
	}
	return uploadErr
}

func (h *S3Handler) get(ctx context.Context, t *model.Transfer) error {
	uri, err := s3uri.Parse(t.SrcURL())
	if err != nil {
		return err
	}
	cfg, err := configForSite(t.SrcSiteLabel())
	if err != nil {
		return err
	}
	c, err := h.client(cfg, uri)
	if err != nil {
		return err
	}
	if err := PrepareLocalDir(filepath.Dir(t.DstPath())); err != nil {
		return err
	}
	out, err := os.Create(t.DstPath())
	if err != nil {
		return err
	}
	defer out.Close()

	downloader := manager.NewDownloader(c)
	_, err = downloader.Download(ctx, out, &s3.GetObjectInput{
		Bucket: aws.String(uri.Bucket),
		Key:    aws.String(uri.Key),
	})
	return err
}

func (h *S3Handler) put(ctx context.Context, t *model.Transfer) error {
	uri, err := s3uri.Parse(t.DstURL())
	if err != nil {
		return err
	}
	cfg, err := configForSite(t.DstSiteLabel())
	if err != nil {
		return err
	}
	c, err := h.client(cfg, uri)
	if err != nil {
		return err
	}
	// mirrors `pegasus-s3 put -b`: create the bucket if it doesn't exist.
	if err := createBucketIfMissing(ctx, c, uri.Bucket, cfg.GetDefault(uri.Site, "region", "")); err != nil {
		h.logger().Warn("s3 put: create-bucket-if-missing failed (continuing)", "bucket", uri.Bucket, "error", err)
	}

	in, err := os.Open(t.SrcPath())
	if err != nil {
		return err
	}
	defer in.Close()

	uploader := manager.NewUploader(c)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(uri.Bucket),
		Key:    aws.String(uri.Key),
		Body:   in,
	})
	return err
}

func (h *S3Handler) DoRemoves(ctx context.Context, removes []*model.Remove) Result {
	var res Result
	for _, r := range removes {
		if err := h.removeOne(ctx, r); err != nil {
			h.logger().Error("s3 remove failed", "url", r.URL(), "error", err)
			res.Failed = append(res.Failed, r)
			continue
		}
		res.Succeeded = append(res.Succeeded, r)
	}
	return res
}

func (h *S3Handler) removeOne(ctx context.Context, r *model.Remove) error {
	uri, err := s3uri.Parse(r.URL())
	if err != nil {
		return err
	}
	cfg, err := configForSite(r.SiteLabel())
	if err != nil {
		return err
	}
	c, err := h.client(cfg, uri)
	if err != nil {
		return err
	}

	if !r.Recursive {
		_, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(uri.Bucket), Key: aws.String(uri.Key)})
		return err
	}

	// PM-790: a recursive remove is a prefix delete (foo/bar -> foo/bar/*),
	// batched per s3cfg's batch_delete_size (default 1000).
	prefix := strings.TrimRight(uri.Key, "/")
	batchSize, _ := strconv.Atoi(cfg.GetDefault(uri.Site, "batch_delete_size", "1000"))
	if batchSize <= 0 {
		batchSize = 1000
	}

	var pageErr error
	paginator := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: aws.String(uri.Bucket), Prefix: aws.String(prefix)})
	var batch []types.ObjectIdentifier
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		_, err := c.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(uri.Bucket),
			Delete: &types.Delete{Objects: batch},
		})
		batch = batch[:0]
		return err
	}
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			pageErr = err
			break
		}
		for _, obj := range page.Contents {
			batch = append(batch, types.ObjectIdentifier{Key: obj.Key})
			if len(batch) >= batchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	if pageErr != nil {
		return pageErr
	}
	return flush()
}
