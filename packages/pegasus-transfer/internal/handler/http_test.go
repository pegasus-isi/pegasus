package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/creds"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

func httpTransfer(t *testing.T, srcURL, dst string) *model.Transfer {
	t.Helper()
	tr := model.NewTransfer()
	if err := tr.AddSrc("remote", srcURL, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := tr.AddDst("local", "file://"+dst, "", nil); err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestHTTPHandler_DownloadsFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("payload"))
	}))
	defer srv.Close()

	dir := t.TempDir()
	dst := filepath.Join(dir, "out.txt")

	h := NewHTTPHandler(Hooks{}, nil)
	res := h.DoTransfers(context.Background(), []*model.Transfer{httpTransfer(t, srv.URL+"/file", dst)})
	if len(res.Failed) != 0 {
		t.Fatalf("unexpected failures: %v", res.Failed)
	}
	data, err := os.ReadFile(dst)
	if err != nil || string(data) != "payload" {
		t.Errorf("got %q err=%v", data, err)
	}
}

func TestHTTPHandler_404Fails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	dir := t.TempDir()
	h := NewHTTPHandler(Hooks{}, nil)
	res := h.DoTransfers(context.Background(), []*model.Transfer{httpTransfer(t, srv.URL+"/missing", filepath.Join(dir, "out.txt"))})
	if len(res.Failed) != 1 {
		t.Fatalf("expected 404 to fail the transfer, got %+v", res)
	}
	if _, err := os.Stat(filepath.Join(dir, "out.txt")); !os.IsNotExist(err) {
		t.Error("expected failed download to not leave a file behind")
	}
}

func TestHTTPHandler_CredentialHeaders(t *testing.T) {
	var gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("Authorization")
		w.Write([]byte("ok"))
	}))
	defer srv.Close()

	host := srv.Listener.Addr().String()
	dir := t.TempDir()
	credData := []byte("[http://" + host + "]\nheader.Authorization = Bearer xyz\n")
	credsIni, err := creds.ParseINI(credData)
	if err != nil {
		t.Fatal(err)
	}

	h := NewHTTPHandler(Hooks{}, credsIni)
	res := h.DoTransfers(context.Background(), []*model.Transfer{httpTransfer(t, srv.URL+"/x", filepath.Join(dir, "out.txt"))})
	if len(res.Failed) != 0 {
		t.Fatalf("unexpected failures: %v", res.Failed)
	}
	if gotHeader != "Bearer xyz" {
		t.Errorf("got Authorization header %q", gotHeader)
	}
}
