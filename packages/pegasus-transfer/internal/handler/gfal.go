package handler

import (
	"context"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// GFALHandler transfers root/srm/gsidavs URLs via gfal-copy/gfal-mkdir/gfal-rm,
// mirroring transfer.py's GFALHandler.
type GFALHandler struct {
	Base
	Hooks
}

func NewGFALHandler(hooks Hooks) *GFALHandler {
	return &GFALHandler{
		Base: Base{
			HandlerName:           "GFALHandler",
			ProtocolMap:           []string{"root->file", "file->root", "srm->file", "file->srm", "gsidavs->file", "file->gsidavs"},
			MkdirCleanupProtocols: []string{"root", "srm", "gsidavs"},
		},
		Hooks: hooks,
	}
}

func (h *GFALHandler) DoMkdirs(ctx context.Context, mkdirs []*model.Mkdir) Result {
	path, ok := findTool("gfal-mkdir")
	if !ok {
		h.logger().Error("unable to mkdir: gfal-mkdir not found")
		return Result{Failed: entriesOf(mkdirs)}
	}
	var res Result
	for _, m := range mkdirs {
		env := siteCredEnv(m.SiteLabel())
		if _, err := h.runCallout(ctx, []string{path, "-p", m.URL()}, env); err != nil {
			h.logger().Error("gfal mkdir failed", "url", m.URL(), "error", err)
			res.Failed = append(res.Failed, m)
			continue
		}
		res.Succeeded = append(res.Succeeded, m)
	}
	return res
}

func (h *GFALHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	path, ok := findTool("gfal-copy")
	if !ok {
		h.logger().Error("unable to do transfers: gfal-copy not found")
		return Result{Failed: entriesOfTransfers(transfers)}
	}
	var res Result
	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()
		env := siteCredEnv(t.SrcSiteLabel())
		for k, v := range siteCredEnv(t.DstSiteLabel()) {
			env[k] = v
		}
		if _, err := h.runCallout(ctx, []string{path, "-f", t.SrcURL(), t.DstURL()}, env); err != nil {
			h.logger().Error("gfal transfer failed", "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}
		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

func (h *GFALHandler) DoRemoves(ctx context.Context, removes []*model.Remove) Result {
	path, ok := findTool("gfal-rm")
	if !ok {
		h.logger().Error("unable to remove: gfal-rm not found")
		return Result{Failed: entriesOf(removes)}
	}
	var res Result
	for _, r := range removes {
		env := siteCredEnv(r.SiteLabel())
		argv := []string{path, r.URL()}
		if r.Recursive {
			argv = []string{path, "-r", r.URL()}
		}
		if _, err := h.runCallout(ctx, argv, env); err != nil {
			h.logger().Error("gfal remove failed", "url", r.URL(), "error", err)
			res.Failed = append(res.Failed, r)
			continue
		}
		res.Succeeded = append(res.Succeeded, r)
	}
	return res
}
