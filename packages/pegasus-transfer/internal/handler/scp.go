package handler

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/creds"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

var reHostPort = regexp.MustCompile(`:([0-9]+)`)

func extractHostname(host string) string {
	if i := strings.Index(host, ":"); i >= 0 {
		return host[:i]
	}
	return host
}

func extractPort(host string) string {
	if m := reHostPort.FindStringSubmatch(host); m != nil {
		return m[1]
	}
	return "22"
}

var scpBaseArgs = []string{"-o", "UserKnownHostsFile=/dev/null", "-o", "StrictHostKeyChecking=no"}

func sshKeyFor(siteLabel string) []string {
	if v, ok := creds.SiteEnv("SSH_PRIVATE_KEY", siteLabel); ok {
		return []string{"-i", v}
	}
	return nil
}

// ScpHandler copies to/from remote hosts via scp/ssh, mirroring transfer.py's
// ScpHandler.
//
// SIMPLIFICATION: the original groups multiple similar transfers into one
// scp invocation to amortize SSH connection setup; each transfer here runs
// its own scp/ssh call instead. Functionally equivalent, slower on a batch
// of many small files to the same host — revisit if that matters in
// practice.
type ScpHandler struct {
	Base
	Hooks

	mu                sync.Mutex
	remoteDirsCreated map[string]bool
}

func NewScpHandler(hooks Hooks) *ScpHandler {
	return &ScpHandler{
		Base: Base{
			HandlerName:           "ScpHandler",
			ProtocolMap:           []string{"scp->file", "file->scp"},
			MkdirCleanupProtocols: []string{"scp"},
		},
		Hooks:             hooks,
		remoteDirsCreated: map[string]bool{},
	}
}

func (h *ScpHandler) DoMkdirs(ctx context.Context, mkdirs []*model.Mkdir) Result {
	var res Result
	for _, m := range mkdirs {
		argv := append([]string{"/usr/bin/ssh"}, scpBaseArgs...)
		argv = append(argv, sshKeyFor(m.SiteLabel())...)
		argv = append(argv, "-p", extractPort(m.Host()), extractHostname(m.Host()),
			fmt.Sprintf("/bin/mkdir -p %s", m.Path()))
		if _, err := h.runCallout(ctx, argv, nil); err != nil {
			h.logger().Error("scp mkdir failed", "path", m.Path(), "error", err)
			res.Failed = append(res.Failed, m)
			continue
		}
		res.Succeeded = append(res.Succeeded, m)
	}
	return res
}

func (h *ScpHandler) prepareRemoteDir(ctx context.Context, siteLabel, host, dir string) {
	key := "scp://" + extractHostname(host) + ":" + dir
	h.mu.Lock()
	done := h.remoteDirsCreated[key]
	h.mu.Unlock()
	if done {
		return
	}
	argv := append([]string{"/usr/bin/ssh"}, scpBaseArgs...)
	argv = append(argv, sshKeyFor(siteLabel)...)
	argv = append(argv, "-p", extractPort(host), extractHostname(host), fmt.Sprintf("/bin/mkdir -p %s", dir))
	_, _ = h.runCallout(ctx, argv, nil) // best-effort, matching _prepare_scp_dir swallowing errors
	h.mu.Lock()
	h.remoteDirsCreated[key] = true
	h.mu.Unlock()
}

func (h *ScpHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	var res Result
	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()

		argv := []string{"/usr/bin/scp", "-r", "-B"}
		argv = append(argv, scpBaseArgs...)

		var err error
		if t.DstProto() == "file" {
			argv = append(argv, sshKeyFor(t.SrcSiteLabel())...)
			argv = append(argv, "-P", extractPort(t.SrcHost()))
			if perr := PrepareLocalDir(filepath.Dir(t.DstPath())); perr != nil {
				h.logger().Error("prepare local dir failed", "error", perr)
			}
			argv = append(argv, extractHostname(t.SrcHost())+":"+t.SrcPath(), t.DstPath())
		} else {
			if !VerifyLocalFile(t.SrcPath()) {
				h.logger().Error("source file does not exist or is not readable", "src", t.SrcPath())
				h.PostTransferAttempt(t, false, start)
				res.Failed = append(res.Failed, t)
				continue
			}
			argv = append(argv, sshKeyFor(t.DstSiteLabel())...)
			h.prepareRemoteDir(ctx, t.DstSiteLabel(), t.DstHost(), filepath.Dir(t.DstPath()))
			argv = append(argv, "-P", extractPort(t.DstHost()))
			argv = append(argv, t.SrcPath(), extractHostname(t.DstHost())+":"+t.DstPath())
		}

		if _, err = h.runCallout(ctx, argv, nil); err != nil {
			h.logger().Error("scp transfer failed", "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}
		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

func (h *ScpHandler) DoRemoves(ctx context.Context, removes []*model.Remove) Result {
	var res Result
	for _, r := range removes {
		argv := append([]string{"/usr/bin/ssh"}, scpBaseArgs...)
		argv = append(argv, sshKeyFor(r.SiteLabel())...)
		rmCmd := "/bin/rm -f"
		if r.Recursive {
			rmCmd += " -r"
		}
		rmCmd += fmt.Sprintf(" %q", r.Path())
		argv = append(argv, "-p", extractPort(r.Host()), extractHostname(r.Host()), rmCmd)
		if _, err := h.runCallout(ctx, argv, nil); err != nil {
			h.logger().Error("scp remove failed", "path", r.Path(), "error", err)
			res.Failed = append(res.Failed, r)
			continue
		}
		res.Succeeded = append(res.Succeeded, r)
	}
	return res
}
