package handler

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// SymlinkHandler creates symlinks for file->symlink and symlink->symlink
// transfers, mirroring transfer.py's SymlinkHandler.
type SymlinkHandler struct {
	Base
	Hooks
}

func NewSymlinkHandler(hooks Hooks) *SymlinkHandler {
	return &SymlinkHandler{
		Base: Base{
			HandlerName:           "SymlinkHandler",
			ProtocolMap:           []string{"file->symlink", "symlink->symlink"},
			MkdirCleanupProtocols: []string{"symlink"},
		},
		Hooks: hooks,
	}
}

func (h *SymlinkHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	var res Result
	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()

		if err := PrepareLocalDir(filepath.Dir(t.DstPath())); err != nil {
			h.logger().Error("prepare local dir failed", "error", err)
		}

		// no dangling symlinks, unless explicitly allowed
		if t.VerifySymlinkSource {
			if _, err := os.Stat(t.SrcPath()); err != nil {
				h.logger().Warn("symlink source does not exist", "src", t.SrcPath())
				h.PostTransferAttempt(t, false, start)
				res.Failed = append(res.Failed, t)
				continue
			}
		}

		if _, err := os.Stat(t.SrcPath()); err == nil {
			if _, err := os.Stat(t.DstPath()); err == nil && SameFile(t.SrcPath(), t.DstPath()) {
				h.logger().Warn("symlink: src and dst already exist and are the same file",
					"src", t.SrcPath(), "dst", t.DstPath())
				h.PostTransferAttempt(t, true, start)
				res.Succeeded = append(res.Succeeded, t)
				continue
			}
		}

		if err := symlinkForce(t.SrcPath(), t.DstPath()); err != nil {
			h.logger().Error("symlink failed", "src", t.SrcPath(), "dst", t.DstPath(), "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}

		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

func (h *SymlinkHandler) DoRemoves(ctx context.Context, removes []*model.Remove) Result {
	var res Result
	for _, r := range removes {
		err := os.Remove(r.Path())
		if os.IsNotExist(err) {
			err = nil
		}
		if err != nil {
			h.logger().Error("remove failed", "path", r.Path(), "error", err)
			res.Failed = append(res.Failed, r)
			continue
		}
		res.Succeeded = append(res.Succeeded, r)
	}
	return res
}
