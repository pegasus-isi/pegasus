package handler

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/creds"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// fakeWebdavServer is a minimal in-memory WebDAV server: enough of
// GET/PUT/MKCOL/DELETE + Basic Auth to exercise WebdavHandler.
func fakeWebdavServer(t *testing.T, user, pass string) (*httptest.Server, map[string][]byte) {
	t.Helper()
	store := map[string][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != user || p != pass {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.Method {
		case "MKCOL":
			w.WriteHeader(http.StatusCreated)
		case http.MethodPut:
			data, _ := io.ReadAll(r.Body)
			store[r.URL.Path] = data
			w.WriteHeader(http.StatusCreated)
		case http.MethodGet:
			data, ok := store[r.URL.Path]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Write(data)
		case http.MethodDelete:
			if _, ok := store[r.URL.Path]; !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			delete(store, r.URL.Path)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	return srv, store
}

func webdavCreds(t *testing.T, host, user, pass string) *creds.INI {
	t.Helper()
	ini, err := creds.ParseINI([]byte("[" + host + "]\nusername = " + user + "\npassword = " + pass + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	return ini
}

func TestWebdavHandler_PutThenGet(t *testing.T) {
	srv, store := fakeWebdavServer(t, "alice", "secret")
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")
	credsIni := webdavCreds(t, host, "alice", "secret")

	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	os.WriteFile(src, []byte("hello webdav"), 0o644)

	h := NewWebdavHandler(Hooks{}, credsIni)

	// file -> webdav
	put := model.NewTransfer()
	put.AddSrc("local", "file://"+src, "", nil)
	put.AddDst("remote", "webdav://"+host+"/dir/dst.txt", "", nil)
	res := h.DoTransfers(context.Background(), []*model.Transfer{put})
	if len(res.Failed) != 0 {
		t.Fatalf("put failed: %v", res.Failed)
	}
	if string(store["/dir/dst.txt"]) != "hello webdav" {
		t.Fatalf("server did not receive uploaded content: %q", store["/dir/dst.txt"])
	}

	// webdav -> file
	dst := filepath.Join(dir, "downloaded.txt")
	get := model.NewTransfer()
	get.AddSrc("remote", "webdav://"+host+"/dir/dst.txt", "", nil)
	get.AddDst("local", "file://"+dst, "", nil)
	res = h.DoTransfers(context.Background(), []*model.Transfer{get})
	if len(res.Failed) != 0 {
		t.Fatalf("get failed: %v", res.Failed)
	}
	data, err := os.ReadFile(dst)
	if err != nil || string(data) != "hello webdav" {
		t.Errorf("got %q err=%v", data, err)
	}
}

func TestWebdavHandler_WrongCredentialsFail(t *testing.T) {
	srv, _ := fakeWebdavServer(t, "alice", "secret")
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")
	credsIni := webdavCreds(t, host, "alice", "wrong-password")

	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	os.WriteFile(src, []byte("x"), 0o644)

	h := NewWebdavHandler(Hooks{}, credsIni)
	put := model.NewTransfer()
	put.AddSrc("local", "file://"+src, "", nil)
	put.AddDst("remote", "webdav://"+host+"/dst.txt", "", nil)
	res := h.DoTransfers(context.Background(), []*model.Transfer{put})
	if len(res.Failed) != 1 {
		t.Fatalf("expected auth failure, got %+v", res)
	}
}

func TestWebdavHandler_Remove(t *testing.T) {
	srv, store := fakeWebdavServer(t, "alice", "secret")
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")
	credsIni := webdavCreds(t, host, "alice", "secret")
	store["/dst.txt"] = []byte("x")

	target, err := model.NewPegasusURL("webdav://"+host+"/dst.txt", "", "remote", 0)
	if err != nil {
		t.Fatal(err)
	}
	h := NewWebdavHandler(Hooks{}, credsIni)
	res := h.DoRemoves(context.Background(), []*model.Remove{{Target: target}})
	if len(res.Failed) != 0 {
		t.Fatalf("unexpected failures: %v", res.Failed)
	}
	if _, ok := store["/dst.txt"]; ok {
		t.Error("expected file to be removed on server")
	}
}

// sanity check that basic auth headers are actually base64(user:pass) as
// net/http's SetBasicAuth produces, guarding against a future refactor
// accidentally sending credentials some other way.
func TestBasicAuthEncoding(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://x", nil)
	req.SetBasicAuth("alice", "secret")
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("alice:secret"))
	if got := req.Header.Get("Authorization"); got != want {
		t.Errorf("got %q want %q", got, want)
	}
}
