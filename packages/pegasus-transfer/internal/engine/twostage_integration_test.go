package engine

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/creds"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/handler"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// liveS3Ident/liveS3Bucket mirror the constants in
// internal/handler/s3_integration_test.go: the identity section (user@site)
// this test looks for in the operator's real ~/.pegasus/credentials.conf
// before it will run, and the bucket exercised under it. Duplicated here
// (rather than exported from internal/handler) to keep each package's
// integration tests self-contained, matching this repo's existing pattern.
const liveS3Ident = "test@amazon"
const liveS3Bucket = "bamboo-030-pegasuslite-s3"

// realS3CredentialsPath mirrors internal/handler/s3_integration_test.go's
// helper of the same purpose: ~/.pegasus/credentials.conf's path if it
// exists and has a [test@amazon] section, or "" if the live test should
// skip.
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
		// Deliberately not logging err: ParseINI's error text quotes the raw
		// offending line, which for this file could be a credentials line --
		// keep it out of test output (-v, CI logs) entirely.
		t.Log("could not parse ~/.pegasus/credentials.conf")
		return ""
	}
	if !ini.HasSection(liveS3Ident) {
		return ""
	}
	return path
}

// TestRun_TwoStageSplit_HTTPToLiveS3 exercises the two-stage split end to end
// against a real dispatch pipeline: an httptest server plays the "src" side
// of a protocol pair with no direct handler to S3 (HTTPHandler only supports
// http(s)->file, never http(s)->s3), so this must go through
// dispatchTwoStage's src->file, file->dst bridge, landing the file in a real
// AWS bucket. It only runs when ~/.pegasus/credentials.conf has a
// [test@amazon] section; otherwise it's skipped, so it never runs in CI.
func TestRun_TwoStageSplit_HTTPToLiveS3(t *testing.T) {
	path := realS3CredentialsPath(t)
	if path == "" {
		t.Skipf("skipping: no [%s] section in ~/.pegasus/credentials.conf", liveS3Ident)
	}
	// S3Handler resolves credentials via PEGASUS_CREDENTIALS[_site].
	t.Setenv("PEGASUS_CREDENTIALS", path)

	want := []byte("hello from pegasus-transfer two-stage split test")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(want)
	}))
	defer srv.Close()

	key := fmt.Sprintf("pegasus-transfer-twostage-test-%d.txt", time.Now().UnixNano())
	dstURL := fmt.Sprintf("s3://%s/%s/%s", liveS3Ident, liveS3Bucket, key)

	s3h := handler.NewS3Handler(handler.Hooks{})
	ctx := context.Background()
	t.Cleanup(func() {
		s3h.DoRemoves(ctx, []*model.Remove{{Target: mustPegasusURLForTest(t, dstURL)}})
	})

	reg := handler.NewRegistry(handler.NewHTTPHandler(handler.Hooks{}, nil), s3h)

	tr := model.NewTransfer()
	if err := tr.AddSrc("remote", srv.URL+"/file", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := tr.AddDst("remote", dstURL, "", nil); err != nil {
		t.Fatal(err)
	}

	ok := Run(ctx, []model.Entry{tr}, Config{MaxAttempts: 1, NumThreads: 1, Registry: reg, Log: silentLogger()})
	if !ok {
		t.Fatal("expected the http->s3 two-stage split to succeed")
	}

	// reading the object back to confirm the upload leg actually landed the
	// right bytes, not just that both legs reported success.
	dir := t.TempDir()
	dst := filepath.Join(dir, "downloaded.txt")
	get := model.NewTransfer()
	if err := get.AddSrc("remote", dstURL, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := get.AddDst("local", "file://"+dst, "", nil); err != nil {
		t.Fatal(err)
	}
	res := s3h.DoTransfers(ctx, []*model.Transfer{get})
	if len(res.Failed) != 0 {
		t.Fatalf("readback of %s failed: %+v", dstURL, res.Failed)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("content mismatch: got %q want %q", got, want)
	}
}

func mustPegasusURLForTest(t *testing.T, rawURL string) *model.PegasusURL {
	t.Helper()
	u, err := model.NewPegasusURL(rawURL, "", "remote", 0)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
