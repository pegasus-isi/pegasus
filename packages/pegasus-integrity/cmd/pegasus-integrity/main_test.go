package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseArgsExactlyOneRequired(t *testing.T) {
	a, err := parseArgs([]string{"--verify", "data.1"})
	if err != nil {
		t.Fatal(err)
	}
	if !a.verifySet || a.generateSet || a.generateYAMLSet || a.generateFullstatYAMLSet {
		t.Fatalf("got %+v", a)
	}
}

func TestParseArgsDistinguishesUnsetFromEmpty(t *testing.T) {
	// Matches optparse: an explicitly-passed empty string still counts as
	// "the flag was specified" (None vs "" are different in Python).
	a, err := parseArgs([]string{"--generate="})
	if err != nil {
		t.Fatal(err)
	}
	if !a.generateSet {
		t.Fatal("expected --generate= to count as specified even though empty")
	}
}

func TestSplitLFNPFN(t *testing.T) {
	cases := []struct{ in, lfn, pfn string }{
		{"data.1", "", "data.1"},
		{"foo=bar", "foo", "bar"},
		{"foo=bar=baz", "foo", "bar=baz"}, // only splits on the FIRST "="
	}
	for _, c := range cases {
		lfn, pfn := splitLFNPFN(c.in)
		if lfn != c.lfn || pfn != c.pfn {
			t.Errorf("splitLFNPFN(%q) = (%q, %q), want (%q, %q)", c.in, lfn, pfn, c.lfn, c.pfn)
		}
	}
}

func TestRunVerifyAgainstFixtures(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	fixtureDir, err := filepath.Abs(filepath.Join("..", "..", "testdata"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(fixtureDir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(cwd)

	log := logger{}
	// Matches the ported Python test: verify data.1 (by its own name) and
	// data.2 under the alias "foo.2", against testdata/data.meta.
	if rc := runVerify("data.1;;;foo.2=data.2", true, log); rc != 0 {
		t.Fatalf("expected success, got exit code %d", rc)
	}
}

func TestRunVerifyMismatchReturnsNonzero(t *testing.T) {
	dir := t.TempDir()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(cwd)

	if err := os.WriteFile("data.1", []byte("wrong content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("data.meta", []byte(`[{"_id":"data.1","_attributes":{"checksum.value":"deadbeef"}}]`), 0o644); err != nil {
		t.Fatal(err)
	}

	if rc := runVerify("data.1", false, logger{}); rc == 0 {
		t.Fatal("expected nonzero exit code for a checksum mismatch")
	}
}
