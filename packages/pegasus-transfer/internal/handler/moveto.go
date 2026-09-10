package handler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// MovetoHandler renames files in place for file->moveto transfers — used
// only internally by the planner, mirroring transfer.py's MovetoHandler.
type MovetoHandler struct {
	Base
	Hooks
}

func NewMovetoHandler(hooks Hooks) *MovetoHandler {
	return &MovetoHandler{
		Base: Base{
			HandlerName: "MovetoHandler",
			ProtocolMap: []string{"file->moveto"},
		},
		Hooks: hooks,
	}
}

func (h *MovetoHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	var res Result
	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()

		if err := PrepareLocalDir(filepath.Dir(t.DstPath())); err != nil {
			h.logger().Error("prepare local dir failed", "error", err)
		}

		if _, err := os.Stat(t.SrcPath()); err != nil {
			h.logger().Warn("moveto source does not exist", "src", t.SrcPath())
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}

		if err := renameOrCopy(t.SrcPath(), t.DstPath()); err != nil {
			h.logger().Error("moveto failed", "src", t.SrcPath(), "dst", t.DstPath(), "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}

		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

// renameOrCopy mirrors `mv`'s behavior: try an atomic rename first, and fall
// back to copy+remove when src/dst are on different filesystems (os.Rename
// returns EXDEV, unlike the mv(1) it replaces).
func renameOrCopy(src, dst string) error {
	err := os.Rename(src, dst)
	if err == nil {
		return nil
	}
	if !errors.Is(err, syscall.EXDEV) {
		return err
	}
	info, statErr := os.Stat(src)
	if statErr != nil {
		return statErr
	}
	if copyErr := copyFileFollowingLinks(src, dst); copyErr != nil {
		return copyErr
	}
	if info.IsDir() {
		return os.RemoveAll(src)
	}
	return os.Remove(src)
}
