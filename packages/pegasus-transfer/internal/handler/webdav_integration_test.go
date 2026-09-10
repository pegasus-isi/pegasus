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

// liveWebdavHost is the real WebDAV endpoint TestWebdavHandler_LiveISIEndpoint
// exercises. Unlike the rest of this file's tests, this is not run against a
// fake httptest server -- it's an opt-in check against real ISI
// infrastructure, gated on the operator's own credentials (see
// loadRealCredentials).
const liveWebdavHost = "workflow.isi.edu"

// loadRealCredentials reads the operator's actual ~/.pegasus/credentials.conf
// -- independent of PEGASUS_CREDENTIALS/creds.LoadCredentials, which govern
// the transfer-time credential contract, not test setup -- and returns nil
// if the file is absent or has no [workflow.isi.edu] section, in which case
// the caller should skip.
func loadRealCredentials(t *testing.T) *creds.INI {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		return nil
	}
	path := filepath.Join(home, ".pegasus", "credentials.conf")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	ini, err := creds.ParseINI(data)
	if err != nil {
		// Deliberately not logging err: ParseINI's error text quotes the
		// raw offending line, which for this file could be a credentials
		// line -- keep it out of test output (-v, CI logs) entirely.
		t.Log("could not parse ~/.pegasus/credentials.conf")
		return nil
	}
	if !ini.HasSection(liveWebdavHost) {
		return nil
	}
	return ini
}

// TestWebdavHandler_LiveISIEndpoint exercises WebdavHandler end-to-end
// against workflow.isi.edu's WebDAV scratch space: create a scratch
// subdirectory, write a file into it, read it back, then delete both file
// and directory. It only runs when ~/.pegasus/credentials.conf has a
// [workflow.isi.edu] section; otherwise it's skipped, so it never runs in CI.
func TestWebdavHandler_LiveISIEndpoint(t *testing.T) {
	credsIni := loadRealCredentials(t)
	if credsIni == nil {
		t.Skipf("skipping: no [%s] section in ~/.pegasus/credentials.conf", liveWebdavHost)
	}

	base := fmt.Sprintf("webdavs://%s/webdav/scratch-90-days/pegasus-transfer-test-%d",
		liveWebdavHost, time.Now().UnixNano())
	dirURL := base + "/"
	fileURL := base + "/hello.txt"

	h := NewWebdavHandler(Hooks{}, credsIni)
	ctx := context.Background()

	t.Cleanup(func() {
		h.DoRemoves(ctx, []*model.Remove{{Target: mustPegasusURL(t, fileURL)}})
		h.DoRemoves(ctx, []*model.Remove{{Target: mustPegasusURL(t, dirURL)}})
	})

	// creating
	res := h.DoMkdirs(ctx, []*model.Mkdir{{Target: mustPegasusURL(t, dirURL)}})
	if len(res.Failed) != 0 {
		t.Fatalf("mkdir %s failed: %+v", dirURL, res.Failed)
	}

	// writing
	dir := t.TempDir()
	src := filepath.Join(dir, "hello.txt")
	want := []byte("hello from pegasus-transfer live webdav test")
	if err := os.WriteFile(src, want, 0o644); err != nil {
		t.Fatal(err)
	}
	put := model.NewTransfer()
	if err := put.AddSrc("local", "file://"+src, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := put.AddDst("remote", fileURL, "", nil); err != nil {
		t.Fatal(err)
	}
	res = h.DoTransfers(ctx, []*model.Transfer{put})
	if len(res.Failed) != 0 {
		t.Fatalf("put %s failed: %+v", fileURL, res.Failed)
	}

	// reading
	dst := filepath.Join(dir, "downloaded.txt")
	get := model.NewTransfer()
	if err := get.AddSrc("remote", fileURL, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := get.AddDst("local", "file://"+dst, "", nil); err != nil {
		t.Fatal(err)
	}
	res = h.DoTransfers(ctx, []*model.Transfer{get})
	if len(res.Failed) != 0 {
		t.Fatalf("get %s failed: %+v", fileURL, res.Failed)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("content mismatch: got %q want %q", got, want)
	}

	// deleting
	res = h.DoRemoves(ctx, []*model.Remove{{Target: mustPegasusURL(t, fileURL)}})
	if len(res.Failed) != 0 {
		t.Fatalf("delete %s failed: %+v", fileURL, res.Failed)
	}
}

func mustPegasusURL(t *testing.T, rawURL string) *model.PegasusURL {
	t.Helper()
	u, err := model.NewPegasusURL(rawURL, "", "remote", 0)
	if err != nil {
		t.Fatal(err)
	}
	return u
}
