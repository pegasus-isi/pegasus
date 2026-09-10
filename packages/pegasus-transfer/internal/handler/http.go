package handler

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/creds"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// HTTPHandler pulls http/https URLs natively via net/http, replacing
// transfer.py's HttpHandler callouts to wget/curl for these two protocols.
// Plain ftp->file (which HttpHandler used to also cover) is out of the
// native-protocol scope and is handled separately by FTPHandler.
type HTTPHandler struct {
	Base
	Hooks
	Credentials *creds.INI
	Client      *http.Client
}

func NewHTTPHandler(hooks Hooks, credentials *creds.INI) *HTTPHandler {
	return &HTTPHandler{
		Base: Base{
			HandlerName: "HTTPHandler",
			ProtocolMap: []string{"http->file", "https->file"},
		},
		Hooks:       hooks,
		Credentials: credentials,
		Client: &http.Client{
			Timeout: 5 * time.Minute, // matches wget's --timeout=300
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				// The existing HttpHandler callout passes curl --insecure /
				// wget --no-check-certificate unconditionally, so strict
				// drop-in compatibility means matching that here rather
				// than the tighter default this project discussed and
				// assumed during design (see decision record — flagged as
				// a correction, not a silent behavior change).
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // matches existing callout behavior
			},
		},
	}
}

func (h *HTTPHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	var res Result

	// Squid proxy support for OSG sites: only for the first attempt, like
	// HttpHandler (subsequent attempts bypass the cache after a failure).
	proxyEnv := ""
	if v, ok := os.LookupEnv("OSG_SQUID_LOCATION"); ok {
		if _, has := os.LookupEnv("http_proxy"); !has {
			proxyEnv = v
		}
	}

	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()

		if err := PrepareLocalDir(filepath.Dir(t.DstPath())); err != nil {
			h.logger().Error("prepare local dir failed", "error", err)
		}

		useProxy := proxyEnv
		if t.Attempts >= 1 {
			useProxy = "" // disable squid caching after a previous failure
		}

		if err := h.fetch(ctx, t, useProxy); err != nil {
			h.logger().Error("http transfer failed", "url", t.SrcURL(), "error", err)
			os.Remove(t.DstPath()) // wget/curl can leave 0-byte files behind
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}

		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

func (h *HTTPHandler) fetch(ctx context.Context, t *model.Transfer, proxyOverride string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.SrcURL(), nil)
	if err != nil {
		return err
	}
	h.addCredHeaders(req, t.SrcProto(), t.SrcHost())

	client := h.Client
	if proxyOverride != "" {
		proxyURL, err := url.Parse(proxyOverride)
		if err == nil {
			transport := h.Client.Transport.(*http.Transport).Clone()
			transport.Proxy = http.ProxyURL(proxyURL)
			client = &http.Client{Timeout: h.Client.Timeout, Transport: transport}
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %s fetching %s", resp.Status, t.SrcURL())
	}

	out, err := os.Create(t.DstPath())
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err := io.Copy(out, resp.Body); err != nil {
		return err
	}
	return nil
}

// addCredHeaders mirrors HttpHandler._cred_options: a [proto://host] or
// [host] section in the credentials file whose "header.X" keys become
// request headers.
func (h *HTTPHandler) addCredHeaders(req *http.Request, proto, host string) {
	if h.Credentials == nil {
		return
	}
	section := proto + "://" + host
	if !h.Credentials.HasSection(section) {
		if !h.Credentials.HasSection(host) {
			return
		}
		section = host
	}
	for key, value := range h.Credentials.Items(section) {
		if strings.HasPrefix(key, "header.") {
			req.Header.Set(strings.TrimPrefix(key, "header."), value)
		}
	}
}
