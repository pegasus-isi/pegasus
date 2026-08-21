package handler

import (
	"context"
	"path/filepath"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// FTPHandler pulls plain ftp:// URLs via wget/curl. Plain FTP was never in
// the native-protocol scope of the Go rewrite (only file/symlink/moveto and
// http(s)/webdav were internalized), so — unlike HTTPHandler — this keeps
// the callout, mirroring the wget-then-curl fallback the combined
// transfer.py HttpHandler used to apply to http/https/ftp alike.
type FTPHandler struct {
	Base
	Hooks
}

func NewFTPHandler(hooks Hooks) *FTPHandler {
	return &FTPHandler{
		Base:  Base{HandlerName: "FTPHandler", ProtocolMap: []string{"ftp->file"}},
		Hooks: hooks,
	}
}

func (h *FTPHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	var res Result
	wget, hasWget := findTool("wget")
	curl, hasCurl := findTool("curl")
	if !hasWget && !hasCurl {
		h.logger().Error("unable to do ftp transfers because neither curl nor wget could be found")
		return Result{Failed: entriesOfTransfers(transfers)}
	}

	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()

		if err := PrepareLocalDir(filepath.Dir(t.DstPath())); err != nil {
			h.logger().Error("prepare local dir failed", "error", err)
		}

		var argv []string
		if hasWget {
			argv = []string{wget, "-nv", "--no-cookies", "--no-check-certificate", "--timeout=300", "--tries=1", "-O", t.DstPath(), t.SrcURL()}
		} else {
			argv = []string{curl, "-s", "-S", "--fail", "--insecure", "--location", "-o", t.DstPath(), t.SrcURL()}
		}

		if _, err := h.runCallout(ctx, argv, nil); err != nil {
			h.logger().Error("ftp transfer failed", "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}

		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}
