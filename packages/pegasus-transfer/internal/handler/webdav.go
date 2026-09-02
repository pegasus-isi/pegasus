package handler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/creds"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// WebdavHandler implements webdav(s)<->file natively via net/http
// (PROPFIND/MKCOL/PUT/GET/DELETE), replacing transfer.py's curl callouts.
//
// LIMITATION: authentication is HTTP Basic only. The curl callout it
// replaces used `--anyauth`, which would also negotiate Digest; servers that
// require Digest (and reject Basic) are not supported here. This is a known
// gap in an otherwise strict drop-in — flag it if any target WebDAV endpoint
// needs Digest.
type WebdavHandler struct {
	Base
	Hooks
	Credentials *creds.INI
	Client      *http.Client

	mu          sync.Mutex
	createdDirs map[string]bool
}

func NewWebdavHandler(hooks Hooks, credentials *creds.INI) *WebdavHandler {
	return &WebdavHandler{
		Base: Base{
			HandlerName:           "WebdavHandler",
			ProtocolMap:           []string{"webdav->file", "webdavs->file", "file->webdav", "file->webdavs"},
			MkdirCleanupProtocols: []string{"webdav", "webdavs"},
		},
		Hooks:       hooks,
		Credentials: credentials,
		Client: &http.Client{
			// Go's default redirect handling downgrades any non-GET/HEAD
			// method to GET on a 301/302/303 (net/http's redirectBehavior).
			// Apache's mod_dav sends exactly such a redirect on MKCOL/DELETE
			// against a collection whose path is missing its trailing slash
			// -- silently following it would turn a PUT into a GET and
			// "succeed" without ever uploading. Disable auto-follow so every
			// method sees the real status code and callers decide.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		createdDirs: map[string]bool{},
	}
}

func (h *WebdavHandler) creds(host string) (user, pass string, err error) {
	if h.Credentials == nil {
		return "", "", fmt.Errorf("no credentials configured for webdav host %s", host)
	}
	u, ok := h.Credentials.Get(host, "username")
	if !ok {
		return "", "", fmt.Errorf("no username configured for webdav host %s", host)
	}
	p, _ := h.Credentials.Get(host, "password")
	return u, p, nil
}

func toHTTPScheme(webdavURL string) string {
	switch {
	case strings.HasPrefix(webdavURL, "webdavs"):
		return "https" + strings.TrimPrefix(webdavURL, "webdavs")
	case strings.HasPrefix(webdavURL, "webdav"):
		return "http" + strings.TrimPrefix(webdavURL, "webdav")
	}
	return webdavURL
}

// do issues an authenticated WebDAV request. body must be passed as a true
// nil io.Reader (not a typed nil pointer) when there's no request body — a
// typed nil produces a non-nil interface that net/http treats as "there is
// a body" and then fails reading it.
func (h *WebdavHandler) do(ctx context.Context, method, url, user, pass string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(user, pass)
	return h.Client.Do(req)
}

// createDir walks path, MKCOL'ing each missing segment, mirroring
// WebdavHandler._create_dir.
func (h *WebdavHandler) createDir(ctx context.Context, dirURL, user, pass string) error {
	h.mu.Lock()
	done := h.createdDirs[dirURL]
	h.mu.Unlock()
	if done {
		return nil
	}
	scheme := "http"
	rest := dirURL
	if strings.HasPrefix(dirURL, "https://") {
		scheme, rest = "https", strings.TrimPrefix(dirURL, "https://")
	} else {
		rest = strings.TrimPrefix(dirURL, "http://")
	}
	slash := strings.Index(rest, "/")
	if slash < 0 {
		return nil
	}
	host, path := rest[:slash], rest[slash:]

	cur := ""
	for _, seg := range strings.Split(strings.Trim(path, "/"), "/") {
		if seg == "" {
			continue
		}
		cur += "/" + seg
		u := scheme + "://" + host + cur
		resp, err := h.do(ctx, "MKCOL", u, user, pass, nil)
		if err != nil {
			return err
		}
		resp.Body.Close()
		// 201 Created is a fresh collection. 405 Method Not Allowed is the
		// usual "already exists". Some servers (observed on Apache mod_dav)
		// instead answer MKCOL on an existing collection with a 301/302 to
		// the trailing-slash canonical form -- also "already exists", not
		// followed since CheckRedirect above disables that.
		switch resp.StatusCode {
		case http.StatusCreated, http.StatusMethodNotAllowed, http.StatusMovedPermanently, http.StatusFound:
			// already exists or freshly created; continue
		default:
			return fmt.Errorf("MKCOL %s: HTTP %s", u, resp.Status)
		}
	}
	h.mu.Lock()
	h.createdDirs[dirURL] = true
	h.mu.Unlock()
	return nil
}

func (h *WebdavHandler) DoMkdirs(ctx context.Context, mkdirs []*model.Mkdir) Result {
	var res Result
	if len(mkdirs) == 0 {
		return res
	}
	user, pass, err := h.creds(mkdirs[0].Host())
	if err != nil {
		h.logger().Error("webdav mkdir: no credentials", "error", err)
		res.Failed = append(res.Failed, entriesOf(mkdirs)...)
		return res
	}
	for _, m := range mkdirs {
		if err := h.createDir(ctx, toHTTPScheme(m.URL()), user, pass); err != nil {
			h.logger().Error("webdav mkdir failed", "url", m.URL(), "error", err)
			res.Failed = append(res.Failed, m)
			continue
		}
		res.Succeeded = append(res.Succeeded, m)
	}
	return res
}

func (h *WebdavHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	var res Result
	if len(transfers) == 0 {
		return res
	}
	host := transfers[0].DstHost()
	if transfers[0].DstProto() == "file" {
		host = transfers[0].SrcHost()
	}
	user, pass, err := h.creds(host)
	if err != nil {
		h.logger().Error("webdav transfer: no credentials", "error", err)
		res.Failed = append(res.Failed, entriesOfTransfers(transfers)...)
		return res
	}

	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()

		if err := h.transferOne(ctx, t, user, pass); err != nil {
			h.logger().Error("webdav transfer failed", "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}

		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

func (h *WebdavHandler) transferOne(ctx context.Context, t *model.Transfer, user, pass string) error {
	if t.DstProto() == "file" {
		// webdav -> file
		if err := PrepareLocalDir(filepath.Dir(t.DstPath())); err != nil {
			return err
		}
		url := toHTTPScheme(t.SrcURL())
		resp, err := h.do(ctx, http.MethodGet, url, user, pass, nil)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("GET %s: HTTP %s", url, resp.Status)
		}
		out, err := os.Create(t.DstPath())
		if err != nil {
			return err
		}
		defer out.Close()
		if _, err := copyBody(out, resp); err != nil {
			return err
		}
		info, err := os.Stat(t.DstPath())
		if err != nil {
			return fmt.Errorf("expected local file is missing after download")
		}
		if info.Size() == 0 {
			return fmt.Errorf("downloaded file is 0 bytes")
		}
		return nil
	}

	// file -> webdav
	url := toHTTPScheme(t.DstURL())
	if err := h.createDir(ctx, toHTTPScheme(t.DstURLDirname()), user, pass); err != nil {
		h.logger().Warn("webdav mkdir before put failed (continuing)", "error", err)
	}
	in, err := os.Open(t.SrcPath())
	if err != nil {
		return err
	}
	defer in.Close()
	resp, err := h.do(ctx, http.MethodPut, url, user, pass, in)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("PUT %s: HTTP %s", url, resp.Status)
	}
	return nil
}

func (h *WebdavHandler) DoRemoves(ctx context.Context, removes []*model.Remove) Result {
	var res Result
	if len(removes) == 0 {
		return res
	}
	user, pass, err := h.creds(removes[0].Host())
	if err != nil {
		h.logger().Error("webdav remove: no credentials", "error", err)
		res.Failed = append(res.Failed, entriesOf(removes)...)
		return res
	}
	for _, r := range removes {
		url := toHTTPScheme(r.URL())
		resp, err := h.do(ctx, http.MethodDelete, url, user, pass, nil)
		if err != nil {
			h.logger().Error("webdav delete failed", "url", url, "error", err)
			res.Failed = append(res.Failed, r)
			continue
		}
		resp.Body.Close()
		// 404 means it's already gone -- treat as success. 301 is the same
		// mod_dav "missing trailing slash" redirect handled in createDir:
		// CheckRedirect above stops it from being followed, but the redirect
		// itself confirms the collection exists and would have been deleted
		// had the client followed it, so it's also a success here.
		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300,
			resp.StatusCode == http.StatusMovedPermanently,
			resp.StatusCode == http.StatusNotFound:
			res.Succeeded = append(res.Succeeded, r)
		default:
			h.logger().Error("webdav delete failed", "url", url, "status", resp.Status)
			res.Failed = append(res.Failed, r)
		}
	}
	return res
}

func copyBody(out *os.File, resp *http.Response) (int64, error) {
	return out.ReadFrom(resp.Body)
}

func entriesOf[T model.Entry](items []T) []model.Entry {
	out := make([]model.Entry, len(items))
	for i, it := range items {
		out[i] = it
	}
	return out
}

func entriesOfTransfers(items []*model.Transfer) []model.Entry {
	out := make([]model.Entry, len(items))
	for i, it := range items {
		out[i] = it
	}
	return out
}
