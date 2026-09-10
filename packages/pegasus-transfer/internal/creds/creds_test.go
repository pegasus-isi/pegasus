package creds

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseINI_SectionsAndDefault(t *testing.T) {
	data := []byte(`
[DEFAULT]
batch_delete = True

[amazon]
endpoint = https://s3.amazonaws.com
region = us-east-1

[test@amazon]
access_key = AKIA...
secret_key = secret
`)
	ini, err := ParseINI(data)
	if err != nil {
		t.Fatal(err)
	}
	if !ini.HasSection("amazon") {
		t.Fatal("expected section 'amazon'")
	}
	if v, ok := ini.Get("amazon", "endpoint"); !ok || v != "https://s3.amazonaws.com" {
		t.Errorf("got %q ok=%v", v, ok)
	}
	if v, ok := ini.Get("amazon", "batch_delete"); !ok || v != "True" {
		t.Errorf("expected inherited DEFAULT key, got %q ok=%v", v, ok)
	}
	if v, ok := ini.Get("test@amazon", "access_key"); !ok || v != "AKIA..." {
		t.Errorf("got %q ok=%v", v, ok)
	}
}

func TestLoadCredentials_MissingEnvReturnsNil(t *testing.T) {
	os.Unsetenv("PEGASUS_CREDENTIALS")
	ini, err := LoadCredentials()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ini != nil {
		t.Fatalf("expected nil credentials when env unset")
	}
}

func TestLoadCredentials_RejectsLiberalPermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "creds.conf")
	if err := os.WriteFile(path, []byte("[foo]\nbar=baz\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PEGASUS_CREDENTIALS", path)
	if _, err := LoadCredentials(); err == nil {
		t.Fatal("expected error for world-readable credentials file")
	}
}

func TestLoadCredentials_AcceptsStrictPermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "creds.conf")
	if err := os.WriteFile(path, []byte("[http://example.com]\nheader.Authorization=Bearer x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PEGASUS_CREDENTIALS", path)
	ini, err := LoadCredentials()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if v, ok := ini.Get("http://example.com", "header.authorization"); !ok || v != "Bearer x" {
		t.Errorf("got %q ok=%v", v, ok)
	}
}

func TestSiteEnv_PerSitePrecedence(t *testing.T) {
	t.Setenv("X509_USER_PROXY", "/generic/proxy")
	t.Setenv("X509_USER_PROXY_mysite", "/site/proxy")
	v, ok := SiteEnv("X509_USER_PROXY", "mysite")
	if !ok || v != "/site/proxy" {
		t.Errorf("got %q ok=%v, want per-site override", v, ok)
	}
	v2, ok := SiteEnv("X509_USER_PROXY", "othersite")
	if !ok || v2 != "/generic/proxy" {
		t.Errorf("got %q ok=%v, want generic fallback", v2, ok)
	}
}

func TestEnsureFSPermissions_FixesLiberalMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	fixed, err := EnsureFSPermissions(path)
	if err != nil {
		t.Fatal(err)
	}
	if !fixed {
		t.Error("expected fixed=true")
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0o600 {
		t.Errorf("got mode %v, want 0600", info.Mode().Perm())
	}
}
