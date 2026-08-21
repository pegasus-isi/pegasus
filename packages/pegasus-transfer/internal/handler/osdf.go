package handler

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// OSDFHandler transfers osdf/pelican/stash<->file via `pelican object copy`
// (preferred) or `stashcp`, mirroring transfer.py's OSDFHandler. Mkdir is a
// no-op there too — transfer.py's do_mkdirs has an unconditional early
// `return` before any real logic, so this preserves that (surprising but
// deliberate) drop-in behavior rather than "fixing" it.
type OSDFHandler struct {
	Base
	Hooks
}

func NewOSDFHandler(hooks Hooks) *OSDFHandler {
	return &OSDFHandler{
		Base: Base{
			HandlerName: "OSDFHandler",
			ProtocolMap: []string{
				"osdf->file", "pelican->file", "stash->file",
				"file->osdf", "file->pelican", "file->stash",
			},
			MkdirCleanupProtocols: []string{"osdf", "pelican", "stash"},
		},
		Hooks: hooks,
	}
}

func (h *OSDFHandler) DoMkdirs(ctx context.Context, mkdirs []*model.Mkdir) Result {
	return Result{Succeeded: entriesOf(mkdirs)}
}

func osdfBaseArgv() ([]string, bool) {
	if path, ok := findTool("pelican"); ok {
		return []string{path, "object", "copy"}, true
	}
	if path, ok := findTool("stashcp"); ok {
		return []string{path}, true
	}
	return nil, false
}

func (h *OSDFHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	base, ok := osdfBaseArgv()
	if !ok {
		h.logger().Error("unable to do OSDF transfers: pelican/stashcp not found")
		return Result{Failed: entriesOfTransfers(transfers)}
	}
	os.Setenv("http_proxy", "") // matches OSDFHandler disabling the proxy for curl

	var res Result
	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()

		var src, dst string
		switch {
		case t.DstProto() == "osdf" || t.DstProto() == "pelican" || t.DstProto() == "stash":
			if !VerifyLocalFile(t.SrcPath()) {
				h.PostTransferAttempt(t, false, start)
				res.Failed = append(res.Failed, t)
				continue
			}
			src, dst = t.SrcPath(), t.DstURL()
		default:
			if err := PrepareLocalDir(filepath.Dir(t.DstPath())); err != nil {
				h.logger().Error("prepare local dir failed", "error", err)
			}
			src, dst = t.SrcURL(), t.DstPath()
		}

		argv := append(append([]string{}, base...), src, dst)
		if _, err := h.runCallout(ctx, argv, nil); err != nil {
			h.logger().Error("osdf transfer failed", "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}
		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

func (h *OSDFHandler) DoRemoves(ctx context.Context, removes []*model.Remove) Result {
	base, ok := osdfBaseArgv()
	if !ok {
		h.logger().Error("unable to do OSDF removes: pelican/stashcp not found")
		return Result{Failed: entriesOf(removes)}
	}
	os.Setenv("http_proxy", "")

	var res Result
	for _, r := range removes {
		if r.Recursive {
			continue // unsupported, matches transfer.py silently skipping it
		}
		argv := append(append([]string{}, base...), "/dev/null", r.URL())
		if _, err := h.runCallout(ctx, argv, nil); err != nil {
			h.logger().Error("osdf remove failed", "url", r.URL(), "error", err)
			res.Failed = append(res.Failed, r)
			continue
		}
		res.Succeeded = append(res.Succeeded, r)
	}
	return res
}
