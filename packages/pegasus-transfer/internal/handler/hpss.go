package handler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/executil"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// HPSSHandler pulls files out of HPSS tar archives via htar, mirroring
// transfer.py's HPSSHandler.
//
// REDUCED FIDELITY: the original batches multiple transfers that share a
// destination directory into one htar extraction (building a member list
// file, then flattening any tar-internal subdirectory structure onto the
// destination with post-extraction moves). HPSS is niche, not exercised by
// the e2e corpus, and that batching is a throughput optimization on top of
// a correctness-equivalent one-file-at-a-time extraction — so this port
// does a straightforward per-transfer `htar -xf <archive> <member>` into a
// scratch dir followed by a move to the destination. Validate against a
// real HPSS endpoint before relying on this in production; the tar-file/
// member-path derivation in particular (_get_tar_file/_get_file_in_tar
// in transfer.py) encodes site-specific archive-naming conventions that
// aren't fully captured here.
type HPSSHandler struct {
	Base
	Hooks
}

func NewHPSSHandler(hooks Hooks) *HPSSHandler {
	return &HPSSHandler{
		Base:  Base{HandlerName: "HPSSHandler", ProtocolMap: []string{"hpss->file", "hpps->file"}},
		Hooks: hooks,
	}
}

// setupCreds mirrors HPSSHandler._setup_creds: HPSS_CREDENTIAL, if set, gets
// copied to ~/.netrc; otherwise ~/.netrc must already exist with 0600 perms.
func setupHPSSCreds() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	defaultCred := filepath.Join(home, ".netrc")

	userCred, ok := os.LookupEnv("HPSS_CREDENTIAL")
	if !ok {
		_, err := ensureExistsAnd0600(defaultCred)
		return err
	}
	if _, err := ensureExistsAnd0600(userCred); err != nil {
		return err
	}
	if SameFile(userCred, defaultCred) {
		return nil
	}
	return copyRegularFileMode(userCred, defaultCred)
}

func ensureExistsAnd0600(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	if info.Mode().Perm() != 0o600 {
		if err := os.Chmod(path, 0o600); err != nil {
			return false, err
		}
	}
	return true, nil
}

func copyRegularFileMode(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	return copyRegularFile(src, dst, info.Mode())
}

func (h *HPSSHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	htar, ok := findTool("htar")
	if !ok {
		h.logger().Error("unable to do hpss transfers: htar not found")
		return Result{Failed: entriesOfTransfers(transfers)}
	}
	if err := setupHPSSCreds(); err != nil {
		h.logger().Error("hpss credential setup failed", "error", err)
		return Result{Failed: entriesOfTransfers(transfers)}
	}

	var res Result
	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()

		if err := h.extractOne(ctx, htar, t); err != nil {
			h.logger().Error("hpss extraction failed", "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}
		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

// extractOne treats the src path as "<archive>.tar/<member>" — the archive
// is the ".tar"-suffixed prefix of the path, everything after it is the
// in-archive member name.
func (h *HPSSHandler) extractOne(ctx context.Context, htar string, t *model.Transfer) error {
	archive, member, ok := splitTarPath(t.SrcPath())
	if !ok {
		return fmt.Errorf("unable to derive an htar archive/member from path: %s", t.SrcPath())
	}
	if err := PrepareLocalDir(filepath.Dir(t.DstPath())); err != nil {
		return err
	}

	scratch, err := os.MkdirTemp("", "pegasus-transfer-hpss-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(scratch)

	if _, err := executil.Run(ctx, []string{htar, "-xf", archive, member}, executil.Options{Dir: scratch}); err != nil {
		return err
	}
	return renameOrCopy(filepath.Join(scratch, member), t.DstPath())
}

func splitTarPath(path string) (archive, member string, ok bool) {
	idx := strings.Index(path, ".tar/")
	if idx < 0 {
		return "", "", false
	}
	return path[:idx+4], path[idx+5:], true
}
