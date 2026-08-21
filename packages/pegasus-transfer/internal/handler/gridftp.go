package handler

import (
	"context"
	"os"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/creds"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// GridFtpHandler transfers to/from/between GridFTP (and sshftp) servers via
// gfal-copy (preferred) or globus-url-copy, mirroring transfer.py's
// GridFtpHandler.
//
// SIMPLIFICATION: the original ran multiple guc "tuning" attempts on the
// first transfer of a group and adaptively tracked gsiftp_failures across
// the run to change guc options. That was a throughput optimization on top
// of guc, not a correctness requirement — the engine's own retry loop
// already retries failures — so it is not reimplemented here. Transfers are
// also not grouped into a single multi-file guc invocation; each runs its
// own command. Revisit if e2e throughput on real gsiftp endpoints regresses.
type GridFtpHandler struct {
	Base
	Hooks
}

func NewGridFtpHandler(hooks Hooks) *GridFtpHandler {
	return &GridFtpHandler{
		Base: Base{
			HandlerName: "GridFtpHandler",
			ProtocolMap: []string{
				"file->gsiftp", "gsiftp->file", "gsiftp->gsiftp",
				"ftp->ftp", "ftp->gsiftp", "gsiftp->ftp",
				"file->sshftp", "sshftp->file", "sshftp->sshftp",
			},
			MkdirCleanupProtocols: []string{"gsiftp", "sshftp"},
		},
		Hooks: hooks,
	}
}

func gfalAvailable(proto1, proto2 string) (string, bool) {
	if _, forced := os.LookupEnv("PEGASUS_FORCE_GUC"); forced {
		return "", false
	}
	if proto1 == "sshftp" || proto2 == "sshftp" {
		return "", false
	}
	return findTool("gfal-copy")
}

func siteCredEnv(siteLabel string) map[string]string {
	env := map[string]string{}
	if v, ok := creds.SiteEnv("X509_USER_PROXY", siteLabel); ok {
		env["X509_USER_PROXY"] = v
	}
	if v, ok := creds.SiteEnv("SSH_PRIVATE_KEY", siteLabel); ok {
		env["SSH_PRIVATE_KEY"] = v
	}
	return env
}

func (h *GridFtpHandler) DoMkdirs(ctx context.Context, mkdirs []*model.Mkdir) Result {
	var res Result
	for _, m := range mkdirs {
		env := siteCredEnv(m.SiteLabel())
		var argv []string
		if path, ok := findTool("gfal-mkdir"); ok && m.Proto() != "sshftp" {
			argv = []string{path, "-p", m.URL()}
		} else if path, ok := findTool("globus-url-copy"); ok {
			argv = []string{path, "-create-dest", "file:///dev/null", m.URL() + "/.create-dir"}
		} else {
			h.logger().Error("unable to mkdir: gfal-mkdir/globus-url-copy not found")
			res.Failed = append(res.Failed, m)
			continue
		}
		if _, err := h.runCallout(ctx, argv, env); err != nil {
			h.logger().Error("gridftp mkdir failed", "url", m.URL(), "error", err)
			res.Failed = append(res.Failed, m)
			continue
		}
		res.Succeeded = append(res.Succeeded, m)
	}
	return res
}

func (h *GridFtpHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	var res Result
	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()
		env := siteCredEnv(t.SrcSiteLabel())
		for k, v := range siteCredEnv(t.DstSiteLabel()) {
			env[k] = v
		}

		var argv []string
		if path, ok := gfalAvailable(t.SrcProto(), t.DstProto()); ok {
			argv = []string{path, "-f", t.SrcURL(), t.DstURL()}
		} else if path, ok := findTool("globus-url-copy"); ok {
			argv = []string{path, "-create-dest", t.SrcURL(), t.DstURL()}
		} else {
			h.logger().Error("unable to do gsiftp transfers: gfal-copy/globus-url-copy not found")
			return Result{Failed: entriesOfTransfers(transfers)}
		}

		if _, err := h.runCallout(ctx, argv, env); err != nil {
			h.logger().Error("gridftp transfer failed", "src", t.SrcURL(), "dst", t.DstURL(), "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}
		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

func (h *GridFtpHandler) DoRemoves(ctx context.Context, removes []*model.Remove) Result {
	var res Result
	for _, r := range removes {
		if r.Recursive {
			// matches transfer.py: recursive gsiftp/sshftp deletes are
			// silently treated as already-successful no-ops.
			res.Succeeded = append(res.Succeeded, r)
			continue
		}
		env := siteCredEnv(r.SiteLabel())
		var argv []string
		if path, ok := findTool("gfal-rm"); ok && r.Proto() != "sshftp" {
			argv = []string{path, r.URL()}
		} else if path, ok := findTool("globus-url-copy"); ok {
			// matches the null-copy delete trick used when gfal is absent.
			argv = []string{path, "file:///dev/null", r.URL()}
		} else {
			h.logger().Error("unable to remove: gfal-rm/globus-url-copy not found")
			res.Failed = append(res.Failed, r)
			continue
		}
		if _, err := h.runCallout(ctx, argv, env); err != nil {
			h.logger().Error("gridftp remove failed", "url", r.URL(), "error", err)
			res.Failed = append(res.Failed, r)
			continue
		}
		res.Succeeded = append(res.Succeeded, r)
	}
	return res
}
