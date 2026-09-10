package handler

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

func fileTransfer(t *testing.T, src, dst string) *model.Transfer {
	t.Helper()
	tr := model.NewTransfer()
	if err := tr.AddSrc("local", "file://"+src, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := tr.AddDst("local", "file://"+dst, "", nil); err != nil {
		t.Fatal(err)
	}
	return tr
}

func TestFileHandler_CopiesFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "sub", "dst.txt")
	if err := os.WriteFile(src, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	h := NewFileHandler(Hooks{}, false)
	res := h.DoTransfers(context.Background(), []*model.Transfer{fileTransfer(t, src, dst)})
	if len(res.Failed) != 0 {
		t.Fatalf("unexpected failures: %v", res.Failed)
	}
	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Errorf("got %q", data)
	}
}

func TestFileHandler_MissingSourceFails(t *testing.T) {
	dir := t.TempDir()
	h := NewFileHandler(Hooks{}, false)
	res := h.DoTransfers(context.Background(), []*model.Transfer{
		fileTransfer(t, filepath.Join(dir, "nope.txt"), filepath.Join(dir, "dst.txt")),
	})
	if len(res.Succeeded) != 0 || len(res.Failed) != 1 {
		t.Fatalf("expected 1 failure, got succeeded=%d failed=%d", len(res.Succeeded), len(res.Failed))
	}
}

func TestFileHandler_SymlinkShortcut(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	if err := os.WriteFile(src, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	h := NewFileHandler(Hooks{}, true) // -s / --symlink
	res := h.DoTransfers(context.Background(), []*model.Transfer{fileTransfer(t, src, dst)})
	if len(res.Failed) != 0 {
		t.Fatalf("unexpected failures: %v", res.Failed)
	}
	info, err := os.Lstat(dst)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode()&os.ModeSymlink == 0 {
		t.Error("expected dst to be a symlink")
	}
}

func TestFileHandler_SameFileIsNoopSuccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.txt")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	h := NewFileHandler(Hooks{}, false)
	res := h.DoTransfers(context.Background(), []*model.Transfer{fileTransfer(t, path, path)})
	if len(res.Succeeded) != 1 {
		t.Fatalf("expected same-file transfer to succeed as a no-op, got %+v", res)
	}
}

func TestFileHandler_Mkdir(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "a", "b", "c")
	m, err := model.NewPegasusURL("file://"+target, "", "local", 0)
	if err != nil {
		t.Fatal(err)
	}
	h := NewFileHandler(Hooks{}, false)
	res := h.DoMkdirs(context.Background(), []*model.Mkdir{{Target: m}})
	if len(res.Failed) != 0 {
		t.Fatalf("unexpected failures: %v", res.Failed)
	}
	if info, err := os.Stat(target); err != nil || !info.IsDir() {
		t.Errorf("expected directory to be created: %v", err)
	}
}

func TestFileHandler_RemoveNonRecursive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.txt")
	os.WriteFile(path, []byte("x"), 0o644)
	target, err := model.NewPegasusURL("file://"+path, "", "local", 0)
	if err != nil {
		t.Fatal(err)
	}
	h := NewFileHandler(Hooks{}, false)
	res := h.DoRemoves(context.Background(), []*model.Remove{{Target: target}})
	if len(res.Failed) != 0 {
		t.Fatalf("unexpected failures: %v", res.Failed)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("expected file to be removed")
	}
}

func TestFileHandler_RemoveMissingFileIsSuccess(t *testing.T) {
	dir := t.TempDir()
	target, err := model.NewPegasusURL("file://"+filepath.Join(dir, "nope.txt"), "", "local", 0)
	if err != nil {
		t.Fatal(err)
	}
	h := NewFileHandler(Hooks{}, false)
	res := h.DoRemoves(context.Background(), []*model.Remove{{Target: target}})
	if len(res.Succeeded) != 1 {
		t.Fatalf("expected /bin/rm -f semantics (missing file = success), got %+v", res)
	}
}

func TestSymlinkHandler_CreatesSymlink(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	os.WriteFile(src, []byte("x"), 0o644)

	h := NewSymlinkHandler(Hooks{})
	res := h.DoTransfers(context.Background(), []*model.Transfer{fileTransfer(t, src, dst)})
	if len(res.Failed) != 0 {
		t.Fatalf("unexpected failures: %v", res.Failed)
	}
	if info, err := os.Lstat(dst); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Error("expected symlink")
	}
}

func TestSymlinkHandler_DanglingSymlinkRejectedByDefault(t *testing.T) {
	dir := t.TempDir()
	h := NewSymlinkHandler(Hooks{})
	res := h.DoTransfers(context.Background(), []*model.Transfer{
		fileTransfer(t, filepath.Join(dir, "nope.txt"), filepath.Join(dir, "dst.txt")),
	})
	if len(res.Failed) != 1 {
		t.Fatalf("expected dangling symlink to fail by default, got %+v", res)
	}
}

func TestMovetoHandler_RenamesFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "moved", "dst.txt")
	os.WriteFile(src, []byte("hello"), 0o644)

	h := NewMovetoHandler(Hooks{})
	res := h.DoTransfers(context.Background(), []*model.Transfer{fileTransfer(t, src, dst)})
	if len(res.Failed) != 0 {
		t.Fatalf("unexpected failures: %v", res.Failed)
	}
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Error("expected source to be gone after move")
	}
	data, err := os.ReadFile(dst)
	if err != nil || string(data) != "hello" {
		t.Errorf("expected moved content, got %q err=%v", data, err)
	}
}
