package checkpoint

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestMatchedFilenames(t *testing.T) {
	dir := t.TempDir()
	for _, f := range []string{"output.txt", "output.dat", "checkpoint.bin", "notes.md"} {
		if err := os.WriteFile(filepath.Join(dir, f), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}

	got, err := MatchedFilenames(dir, []string{`output\..*`, "subdir"})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	want := []string{"output.dat", "output.txt", "subdir"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestMatchedFilenamesFullmatchOnly(t *testing.T) {
	// re.fullmatch semantics: a pattern that only matches a prefix must not
	// match, unlike re.search.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "output.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := MatchedFilenames(dir, []string{"output"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no fullmatch, got %v", got)
	}
}

func TestArchiveAndCompressRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(cwd)

	if err := os.WriteFile("a.txt", []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir("sub", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join("sub", "b.txt"), []byte("world"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("a.txt", "link.txt"); err != nil {
		t.Fatal(err)
	}

	if err := ArchiveAndCompress([]string{"a.txt", "sub", "link.txt"}); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(CheckpointFilename)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(gz)

	found := map[string]*tar.Header{}
	contents := map[string]string{}
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		found[hdr.Name] = hdr
		if hdr.Typeflag == tar.TypeReg {
			b, _ := io.ReadAll(tr)
			contents[hdr.Name] = string(b)
		}
	}

	if contents["a.txt"] != "hello" {
		t.Errorf("a.txt contents = %q", contents["a.txt"])
	}
	if contents["sub/b.txt"] != "world" {
		t.Errorf("sub/b.txt contents = %q", contents["sub/b.txt"])
	}
	if _, ok := found["sub/"]; !ok {
		t.Errorf("expected directory entry sub/, got %v", found)
	}
	link, ok := found["link.txt"]
	if !ok || link.Typeflag != tar.TypeSymlink || link.Linkname != "a.txt" {
		t.Errorf("expected symlink link.txt -> a.txt, got %+v", link)
	}
}
