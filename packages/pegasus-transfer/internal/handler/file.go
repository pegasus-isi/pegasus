package handler

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// FileHandler implements file->file local copies, mirroring transfer.py's
// FileHandler — but doing the copy/mkdir/remove with the os package
// directly instead of shelling out to /bin/cp, /bin/mkdir, /bin/rm, ln.
type FileHandler struct {
	Base
	Hooks
	// SymlinkShortcut mirrors the global symlink_file_transfer flag (the
	// CLI's -s/--symlink option): when set, file->file "copies" become
	// symlinks instead, matching FileHandler.do_transfers.
	SymlinkShortcut bool
}

func NewFileHandler(hooks Hooks, symlinkShortcut bool) *FileHandler {
	return &FileHandler{
		Base: Base{
			HandlerName:           "FileHandler",
			ProtocolMap:           []string{"file->file"},
			MkdirCleanupProtocols: []string{"file"},
		},
		Hooks:           hooks,
		SymlinkShortcut: symlinkShortcut,
	}
}

func (h *FileHandler) DoMkdirs(ctx context.Context, mkdirs []*model.Mkdir) Result {
	var res Result
	for _, m := range mkdirs {
		if err := os.MkdirAll(m.Path(), 0o755); err != nil {
			h.logger().Error("mkdir failed", "path", m.Path(), "error", err)
			res.Failed = append(res.Failed, m)
			continue
		}
		res.Succeeded = append(res.Succeeded, m)
	}
	return res
}

func (h *FileHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	var res Result
	for _, t := range transfers {
		start := time.Now()

		if err := PrepareLocalDir(filepath.Dir(t.DstPath())); err != nil {
			h.logger().Error("prepare local dir failed", "error", err)
		}

		if !VerifyLocalFile(t.SrcPath()) {
			h.logger().Error("source file does not exist or is not readable", "src", t.SrcPath())
			res.Failed = append(res.Failed, t)
			h.PostTransferAttempt(t, false, start)
			continue
		}

		h.PreTransferAttempt(t)

		if _, err := os.Stat(t.SrcPath()); err == nil {
			if _, err := os.Stat(t.DstPath()); err == nil && SameFile(t.SrcPath(), t.DstPath()) {
				h.logger().Warn("cp: src and dst already exist and are the same file",
					"src", t.SrcPath(), "dst", t.DstPath())
				res.Succeeded = append(res.Succeeded, t)
				h.PostTransferAttempt(t, true, start)
				continue
			}
		}

		if t.VerifySymlinkSource && !VerifyReadAccess(t.SrcPath()) {
			h.logger().Error("ln: src does not exist, or is not readable", "src", t.SrcPath())
			res.Failed = append(res.Failed, t)
			h.PostTransferAttempt(t, false, start)
			continue
		}

		var err error
		if h.SymlinkShortcut {
			err = symlinkForce(t.SrcPath(), t.DstPath())
		} else {
			err = copyFileFollowingLinks(t.SrcPath(), t.DstPath())
		}
		if err != nil {
			h.logger().Error("copy failed", "src", t.SrcPath(), "dst", t.DstPath(), "error", err)
			res.Failed = append(res.Failed, t)
			h.PostTransferAttempt(t, false, start)
			continue
		}

		res.Succeeded = append(res.Succeeded, t)
		h.PostTransferAttempt(t, true, start)
	}
	return res
}

func (h *FileHandler) DoRemoves(ctx context.Context, removes []*model.Remove) Result {
	var res Result
	for _, r := range removes {
		var err error
		if r.Recursive {
			err = os.RemoveAll(r.Path())
		} else {
			err = os.Remove(r.Path())
			if os.IsNotExist(err) {
				err = nil // /bin/rm -f semantics: missing file is not an error
			}
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

func symlinkForce(src, dst string) error {
	_ = os.Remove(dst)
	return os.Symlink(src, dst)
}

// copyFileFollowingLinks mirrors `/bin/cp -f -R -L`: follow symlinks in the
// source, overwrite the destination, and recurse if the source is a
// directory.
func copyFileFollowingLinks(src, dst string) error {
	info, err := os.Stat(src) // Stat (not Lstat) follows symlinks, matching -L
	if err != nil {
		return err
	}
	if info.IsDir() {
		return copyDir(src, dst, info.Mode())
	}
	return copyRegularFile(src, dst, info.Mode())
}

func copyDir(src, dst string, mode os.FileMode) error {
	if err := os.MkdirAll(dst, mode.Perm()); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := copyFileFollowingLinks(filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func copyRegularFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	_ = os.Remove(dst) // -f: clobber an existing destination
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode.Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
