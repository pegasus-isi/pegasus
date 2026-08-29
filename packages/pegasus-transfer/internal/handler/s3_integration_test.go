package handler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/creds"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// liveS3Ident is the identity section (user@site) TestS3Handler_LiveAmazonBucket
// looks for in the operator's real ~/.pegasus/credentials.conf before it will
// run -- an opt-in check against a real AWS bucket, not the fake/MinIO
// endpoints the rest of this package's tests would use.
const liveS3Ident = "test@amazon"

// liveS3Bucket is the bucket exercised under the identity above.
const liveS3Bucket = "bamboo-030-pegasuslite-s3"

// realS3CredentialsPath returns ~/.pegasus/credentials.conf's path if it
// exists and has a [test@amazon] section, or "" if the live S3 test should
// skip. Independent of PEGASUS_CREDENTIALS/creds.LoadCredentials, which
// govern the transfer-time credential contract, not test setup.
func realS3CredentialsPath(t *testing.T) string {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	path := filepath.Join(home, ".pegasus", "credentials.conf")
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	ini, err := creds.ParseINI(data)
	if err != nil {
		// Deliberately not logging err: ParseINI's error text quotes the
		// raw offending line, which for this file could be a credentials
		// line -- keep it out of test output (-v, CI logs) entirely.
		t.Log("could not parse ~/.pegasus/credentials.conf")
		return ""
	}
	if !ini.HasSection(liveS3Ident) {
		return ""
	}
	return path
}

// TestS3Handler_LiveAmazonBucket exercises S3Handler end-to-end against a
// real AWS bucket: confirm/create the bucket, write an object, read it back,
// then delete it. It only runs when ~/.pegasus/credentials.conf has a
// [test@amazon] section; otherwise it's skipped, so it never runs in CI.
func TestS3Handler_LiveAmazonBucket(t *testing.T) {
	path := realS3CredentialsPath(t)
	if path == "" {
		t.Skipf("skipping: no [%s] section in ~/.pegasus/credentials.conf", liveS3Ident)
	}
	// S3Handler resolves credentials via PEGASUS_CREDENTIALS[_site], not a
	// pre-parsed *creds.INI like WebdavHandler -- point it at the real file
	// for the duration of this test.
	t.Setenv("PEGASUS_CREDENTIALS", path)

	key := fmt.Sprintf("pegasus-transfer-test-%d.txt", time.Now().UnixNano())
	url := fmt.Sprintf("s3://%s/%s/%s", liveS3Ident, liveS3Bucket, key)

	h := NewS3Handler(Hooks{})
	ctx := context.Background()

	t.Cleanup(func() {
		h.DoRemoves(ctx, []*model.Remove{{Target: mustPegasusURL(t, url)}})
	})

	// creating (bucket already exists; this exercises HeadBucket's
	// already-exists path rather than a fresh CreateBucket)
	res := h.DoMkdirs(ctx, []*model.Mkdir{{Target: mustPegasusURL(t, url)}})
	if len(res.Failed) != 0 {
		t.Fatalf("mkdir %s failed: %+v", url, res.Failed)
	}

	// writing
	dir := t.TempDir()
	src := filepath.Join(dir, "hello.txt")
	want := []byte("hello from pegasus-transfer live s3 test")
	if err := os.WriteFile(src, want, 0o644); err != nil {
		t.Fatal(err)
	}
	put := model.NewTransfer()
	if err := put.AddSrc("local", "file://"+src, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := put.AddDst("remote", url, "", nil); err != nil {
		t.Fatal(err)
	}
	res = h.DoTransfers(ctx, []*model.Transfer{put})
	if len(res.Failed) != 0 {
		t.Fatalf("put %s failed: %+v", url, res.Failed)
	}

	// reading
	dst := filepath.Join(dir, "downloaded.txt")
	get := model.NewTransfer()
	if err := get.AddSrc("remote", url, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := get.AddDst("local", "file://"+dst, "", nil); err != nil {
		t.Fatal(err)
	}
	res = h.DoTransfers(ctx, []*model.Transfer{get})
	if len(res.Failed) != 0 {
		t.Fatalf("get %s failed: %+v", url, res.Failed)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("content mismatch: got %q want %q", got, want)
	}

	// deleting
	res = h.DoRemoves(ctx, []*model.Remove{{Target: mustPegasusURL(t, url)}})
	if len(res.Failed) != 0 {
		t.Fatalf("delete %s failed: %+v", url, res.Failed)
	}
}
