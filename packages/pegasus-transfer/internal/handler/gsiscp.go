package handler

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// GSIScpHandler is GSI-authenticated scp/ssh (gsiscp/gsissh), mirroring
// transfer.py's GSIScpHandler with the same per-transfer simplification
// noted on ScpHandler (no batching of similar transfers into one command).
type GSIScpHandler struct {
	Base
	Hooks
}

func NewGSIScpHandler(hooks Hooks) *GSIScpHandler {
	return &GSIScpHandler{
		Base: Base{
			HandlerName:           "GSIScpHandler",
			ProtocolMap:           []string{"gsiscp->file", "file->gsiscp"},
			MkdirCleanupProtocols: []string{"gsiscp"},
		},
		Hooks: hooks,
	}
}

func (h *GSIScpHandler) DoMkdirs(ctx context.Context, mkdirs []*model.Mkdir) Result {
	var res Result
	for _, m := range mkdirs {
		env := siteCredEnv(m.SiteLabel())
		argv := []string{"gsissh", "-p", extractPort(m.Host()), extractHostname(m.Host()), fmt.Sprintf("/bin/mkdir -p %s", m.Path())}
		if _, err := h.runCallout(ctx, argv, env); err != nil {
			h.logger().Error("gsiscp mkdir failed", "path", m.Path(), "error", err)
			res.Failed = append(res.Failed, m)
			continue
		}
		res.Succeeded = append(res.Succeeded, m)
	}
	return res
}

func (h *GSIScpHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	var res Result
	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()

		env := siteCredEnv(t.SrcSiteLabel())
		for k, v := range siteCredEnv(t.DstSiteLabel()) {
			env[k] = v
		}

		var argv []string
		if t.DstProto() == "file" {
			if err := PrepareLocalDir(filepath.Dir(t.DstPath())); err != nil {
				h.logger().Error("prepare local dir failed", "error", err)
			}
			argv = []string{"gsiscp", "-P", extractPort(t.SrcHost()), extractHostname(t.SrcHost()) + ":" + t.SrcPath(), t.DstPath()}
		} else {
			if !VerifyLocalFile(t.SrcPath()) {
				h.logger().Error("source file does not exist or is not readable", "src", t.SrcPath())
				h.PostTransferAttempt(t, false, start)
				res.Failed = append(res.Failed, t)
				continue
			}
			argv = []string{"gsiscp", "-P", extractPort(t.DstHost()), t.SrcPath(), extractHostname(t.DstHost()) + ":" + t.DstPath()}
		}

		if _, err := h.runCallout(ctx, argv, env); err != nil {
			h.logger().Error("gsiscp transfer failed", "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}
		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

func (h *GSIScpHandler) DoRemoves(ctx context.Context, removes []*model.Remove) Result {
	var res Result
	for _, r := range removes {
		env := siteCredEnv(r.SiteLabel())
		rmCmd := "/bin/rm -f"
		if r.Recursive {
			rmCmd += " -r"
		}
		rmCmd += fmt.Sprintf(" %q", r.Path())
		argv := []string{"gsissh", "-p", extractPort(r.Host()), extractHostname(r.Host()), rmCmd}
		if _, err := h.runCallout(ctx, argv, env); err != nil {
			h.logger().Error("gsiscp remove failed", "path", r.Path(), "error", err)
			res.Failed = append(res.Failed, r)
			continue
		}
		res.Succeeded = append(res.Succeeded, r)
	}
	return res
}
