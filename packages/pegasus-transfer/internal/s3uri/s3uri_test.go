package s3uri

import "testing"

func TestParse_Basic(t *testing.T) {
	u, err := Parse("s3://test@amazon/pegasus-isi-transfer-tests/medium-files/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if u.User != "test" || u.Site != "amazon" || u.Ident != "test@amazon" {
		t.Errorf("got %+v", u)
	}
	if u.Bucket != "pegasus-isi-transfer-tests" || u.Key != "medium-files/a.txt" {
		t.Errorf("got bucket=%q key=%q", u.Bucket, u.Key)
	}
	if u.Secure {
		t.Errorf("expected insecure for s3://")
	}
}

func TestParse_Secure(t *testing.T) {
	u, err := Parse("s3s://test@amazon/bucket/key")
	if err != nil {
		t.Fatal(err)
	}
	if !u.Secure {
		t.Errorf("expected secure for s3s://")
	}
}

func TestParse_NoBucket(t *testing.T) {
	u, err := Parse("s3://test@amazon")
	if err != nil {
		t.Fatal(err)
	}
	if u.Bucket != "" || u.Key != "" {
		t.Errorf("expected empty bucket/key, got bucket=%q key=%q", u.Bucket, u.Key)
	}
}

func TestParse_BucketOnly(t *testing.T) {
	u, err := Parse("s3://test@amazon/bucket")
	if err != nil {
		t.Fatal(err)
	}
	if u.Bucket != "bucket" || u.Key != "" {
		t.Errorf("got bucket=%q key=%q", u.Bucket, u.Key)
	}
}

func TestParse_MissingUser(t *testing.T) {
	if _, err := Parse("s3://amazon/bucket/key"); err == nil {
		t.Fatal("expected error for missing user")
	}
}

func TestParse_InvalidScheme(t *testing.T) {
	if _, err := Parse("http://foo"); err == nil {
		t.Fatal("expected error for invalid scheme")
	}
}

func TestParse_PortInSite(t *testing.T) {
	u, err := Parse("s3://user@minio.local:9000/bucket/key")
	if err != nil {
		t.Fatal(err)
	}
	if u.Site != "minio.local:9000" {
		t.Errorf("got site %q", u.Site)
	}
}

func TestString_RoundTrip(t *testing.T) {
	u, err := Parse("s3s://user@site/bucket/key/sub")
	if err != nil {
		t.Fatal(err)
	}
	if got := u.String(); got != "s3s://user@site/bucket/key/sub" {
		t.Errorf("got %q", got)
	}
}

func TestHasWildcards(t *testing.T) {
	if !HasWildcards("foo/*.txt") {
		t.Error("expected wildcard detection")
	}
	if HasWildcards("foo/bar.txt") {
		t.Error("expected no wildcard")
	}
}
