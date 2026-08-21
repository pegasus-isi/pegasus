package handler

import (
	"log/slog"
	"os"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/stats"
)

// Integrity is the subset of integrity.Generator every handler needs;
// defined here (rather than importing the integrity package directly) so
// handler doesn't depend on integrity's OS-specific stat internals.
type Integrity interface {
	Generate(lfn, path string) error
}

// Hooks bundles the per-transfer-attempt side effects shared by every
// handler, matching TransferHandlerBase._pre_transfer_attempt /
// _post_transfer_attempt: integrity checksumming and stats accounting.
type Hooks struct {
	Integrity Integrity
	Stats     *stats.Collector
	Log       *slog.Logger
}

// PreTransferAttempt checksums the source file before a transfer, if
// requested and the source is local — matching
// TransferHandlerBase._pre_transfer_attempt.
func (h Hooks) PreTransferAttempt(t *model.Transfer) {
	if t.GenerateChecksum && t.SrcProto() == "file" && h.Integrity != nil {
		if err := h.Integrity.Generate(t.LFN, t.SrcPath()); err != nil {
			h.logger().Error("integrity checksum generation failed", "lfn", t.LFN, "error", err)
		}
	}
}

// PostTransferAttempt checksums the destination file on success (if
// requested and local) and records stats, matching
// TransferHandlerBase._post_transfer_attempt. The PEGASUS_TRANSFER_ERROR_RATE
// fault injector that used to live here was dropped in the Go rewrite.
func (h Hooks) PostTransferAttempt(t *model.Transfer, success bool, start time.Time) {
	end := time.Now()
	if success && t.GenerateChecksum && t.DstProto() == "file" && h.Integrity != nil {
		if err := h.Integrity.Generate(t.LFN, t.DstPath()); err != nil {
			h.logger().Error("integrity checksum generation failed", "lfn", t.LFN, "error", err)
		}
	}
	t.Attempts++
	if h.Stats != nil {
		h.Stats.Add(t, success, start, end)
	}
}

func (h Hooks) logger() *slog.Logger {
	if h.Log != nil {
		return h.Log
	}
	return slog.Default()
}

// VerifyLocalFile mirrors transfer.py's verify_local_file: the path must
// exist and be openable for read.
func VerifyLocalFile(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	f.Close()
	return true
}

// VerifyReadAccess mirrors transfer.py's _verify_read_access: exists, and a
// small read at the front succeeds (catches permission-mapping filesystems
// like CVMFS where POSIX bits alone can lie).
func VerifyReadAccess(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	if info.Size() > 0 {
		buf := make([]byte, 1024)
		if _, err := f.Read(buf); err != nil {
			return false
		}
	}
	return true
}

// PrepareLocalDir mirrors transfer.py's prepare_local_dir: mkdir -p,
// tolerating a pre-existing directory.
func PrepareLocalDir(path string) error {
	if path == "" {
		return nil
	}
	if info, err := os.Stat(path); err == nil {
		if info.IsDir() {
			return nil
		}
	}
	return os.MkdirAll(path, 0o755)
}

// SameFile mirrors the src/dst inode comparison used by FileHandler and
// SymlinkHandler to detect a same-file no-op copy.
func SameFile(a, b string) bool {
	ai, err := os.Stat(a)
	if err != nil {
		return false
	}
	bi, err := os.Stat(b)
	if err != nil {
		return false
	}
	return os.SameFile(ai, bi)
}
