package globusconf

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSaveThenLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "globus.conf")

	want := Config{
		ClientID:      "client-abc",
		TransferAT:    "access-token",
		TransferRT:    "refresh-token",
		TransferATExp: 1234567890,
	}
	if err := Save(path, want); err != nil {
		t.Fatal(err)
	}

	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestSaveCreatesParentDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", ".pegasus", "globus.conf")
	if err := Save(path, Config{ClientID: "c"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nope.conf"))
	if err == nil {
		t.Fatal("expected an error for a missing config file")
	}
}

func TestLoadMissingClientID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "globus.conf")
	os.WriteFile(path, []byte("[oauth]\ntransfer_at = x\n"), 0o600)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected an error when client_id is missing")
	}
}

func TestLoadIgnoresCommentsAndOtherSections(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "globus.conf")
	content := "# a comment\n[other]\nclient_id = wrong\n\n[oauth]\n; another comment\nclient_id = right\ntransfer_at = at\ntransfer_rt = rt\ntransfer_at_exp = 42\n"
	os.WriteFile(path, []byte(content), 0o600)

	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.ClientID != "right" {
		t.Fatalf("got client_id %q, want to only read the [oauth] section", got.ClientID)
	}
	if got.TransferATExp != 42 {
		t.Fatalf("got exp %d", got.TransferATExp)
	}
}

func TestLoadNoRefreshTokenIsFine(t *testing.T) {
	// Matches --permanent not having been used: transfer_rt is absent, and
	// that alone isn't an error at load time (acquire_clients decides what
	// to do with it, not the config loader).
	dir := t.TempDir()
	path := filepath.Join(dir, "globus.conf")
	os.WriteFile(path, []byte("[oauth]\nclient_id = c\ntransfer_at = at\ntransfer_at_exp = 1\n"), 0o600)

	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.TransferRT != "" {
		t.Fatalf("got %q", got.TransferRT)
	}
}
