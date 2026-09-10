package model

import (
	"os"
	"testing"
)

func TestPegasusURL_File(t *testing.T) {
	cases := []struct {
		url  string
		path string
	}{
		{"file:///tmp/a.txt", "/tmp/a.txt"},
		{"file://localhost/tmp/a.txt", "/tmp/a.txt"},
		{"file:/tmp/a.txt", "/tmp/a.txt"},
		{"file:relative/a.txt", "relative/a.txt"},
		{"/tmp/no-scheme.txt", "/tmp/no-scheme.txt"}, // no ':' -> defaults to file://
	}
	for _, c := range cases {
		u, err := NewPegasusURL(c.url, "", "local", 0)
		if err != nil {
			t.Fatalf("%s: %v", c.url, err)
		}
		if u.Proto != "file" {
			t.Errorf("%s: proto = %q, want file", c.url, u.Proto)
		}
		if u.Path != c.path {
			t.Errorf("%s: path = %q, want %q", c.url, u.Path, c.path)
		}
	}
}

func TestPegasusURL_SymlinkAndMoveto(t *testing.T) {
	u, err := NewPegasusURL("symlink:///tmp/a.txt", "", "local", 0)
	if err != nil {
		t.Fatal(err)
	}
	if u.Proto != "symlink" || u.Path != "/tmp/a.txt" {
		t.Errorf("got proto=%q path=%q", u.Proto, u.Path)
	}

	u2, err := NewPegasusURL("moveto:/tmp/a.txt", "", "local", 0)
	if err != nil {
		t.Fatal(err)
	}
	if u2.Proto != "moveto" || u2.Path != "/tmp/a.txt" {
		t.Errorf("got proto=%q path=%q", u2.Proto, u2.Path)
	}
}

func TestPegasusURL_EnvVarExpansion(t *testing.T) {
	os.Setenv("PT_TEST_DIR", "/scratch/x")
	defer os.Unsetenv("PT_TEST_DIR")

	u, err := NewPegasusURL("file://$PT_TEST_DIR/a.txt", "", "local", 0)
	if err != nil {
		t.Fatal(err)
	}
	if u.Path != "/scratch/x/a.txt" {
		t.Errorf("got path %q", u.Path)
	}

	u2, err := NewPegasusURL("file://${PT_TEST_DIR}/b.txt", "", "local", 0)
	if err != nil {
		t.Fatal(err)
	}
	if u2.Path != "/scratch/x/b.txt" {
		t.Errorf("got path %q", u2.Path)
	}
}

func TestPegasusURL_UnknownEnvVarExpandsEmpty(t *testing.T) {
	os.Unsetenv("PT_TEST_DOES_NOT_EXIST")
	u, err := NewPegasusURL("file://$PT_TEST_DOES_NOT_EXIST/a.txt", "", "local", 0)
	if err != nil {
		t.Fatal(err)
	}
	if u.Path != "/a.txt" {
		t.Errorf("got path %q", u.Path)
	}
}

func TestPegasusURL_RemoteProtocols(t *testing.T) {
	cases := []struct {
		url, proto, host, path string
	}{
		{"http://example.com/a/b.txt", "http", "example.com", "/a/b.txt"},
		{"gsiftp://host.example.com:2811/a//b.txt", "gsiftp", "host.example.com:2811", "/a/b.txt"}, // "//"+ collapse
		{"s3://user@site/bucket/key", "s3", "user@site", "/bucket/key"},
	}
	for _, c := range cases {
		u, err := NewPegasusURL(c.url, "", "local", 0)
		if err != nil {
			t.Fatalf("%s: %v", c.url, err)
		}
		if u.Proto != c.proto || u.Host != c.host || u.Path != c.path {
			t.Errorf("%s: got proto=%q host=%q path=%q, want proto=%q host=%q path=%q",
				c.url, u.Proto, u.Host, u.Path, c.proto, c.host, c.path)
		}
	}
}

func TestPegasusURL_SiteLabelNormalization(t *testing.T) {
	u, err := NewPegasusURL("file:///tmp/a.txt", "", "my-site-1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if u.SiteLabel != "my_site_1" {
		t.Errorf("got site label %q, want my_site_1", u.SiteLabel)
	}
}

func TestPegasusURL_SrmRootExtraSlash(t *testing.T) {
	u, err := NewPegasusURL("srm://host.example.com/path/to/file", "", "local", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := u.GetURL(); got != "srm://host.example.com//path/to/file" {
		t.Errorf("got %q", got)
	}
}
