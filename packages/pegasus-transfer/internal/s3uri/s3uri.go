// Package s3uri parses pegasus-transfer's S3 URI scheme
// (s3[s]://user@site[/bucket[/key]]), matching s3.py's S3URI/parse_uri
// exactly so the merged-in S3 support keeps the frozen s3cfg/URL contract.
package s3uri

import (
	"fmt"
	"net/url"
	"strings"
)

// URI is a parsed S3 URI.
type URI struct {
	User   string
	Site   string // host[:port]
	Ident  string // "user@site" — the identity section name in the config file
	Bucket string // "" if unset
	Key    string // "" if unset
	Secure bool   // true for s3s://
}

// String reconstructs the URI, matching S3URI.__repr__.
func (u URI) String() string {
	scheme := "s3"
	if u.Secure {
		scheme = "s3s"
	}
	s := fmt.Sprintf("%s://%s", scheme, u.Ident)
	if u.Bucket != "" {
		s += "/" + u.Bucket
	}
	if u.Key != "" {
		s += "/" + u.Key
	}
	return s
}

// Parse parses an s3:// or s3s:// URI, matching s3.py's parse_uri().
func Parse(raw string) (URI, error) {
	var secure bool
	var rest string
	switch {
	case strings.HasPrefix(raw, "s3s://"):
		secure = true
		rest = "http://" + strings.TrimPrefix(raw, "s3s://")
	case strings.HasPrefix(raw, "s3://"):
		secure = false
		rest = "http://" + strings.TrimPrefix(raw, "s3://")
	default:
		return URI{}, fmt.Errorf("invalid URL scheme: %s", raw)
	}

	// Preserve a literal '?' wildcard as part of the path rather than
	// letting it be parsed as a query separator, matching parse_uri's
	// special-casing of "?" (used for single-character glob matches).
	hasQuestion := strings.Contains(raw, "?")

	parsed, err := url.Parse(rest)
	if err != nil {
		return URI{}, fmt.Errorf("unable to parse URL: %s", raw)
	}

	path := strings.TrimSpace(parsed.Path)
	if hasQuestion {
		path = strings.TrimSpace(parsed.Path + "?" + parsed.RawQuery)
	}
	path = strings.TrimPrefix(path, "/")

	var bucket, key string
	if path != "" {
		parts := strings.SplitN(path, "/", 2)
		bucket = parts[0]
		if len(parts) == 2 && parts[1] != "" {
			key = parts[1]
		}
	}

	if parsed.User == nil || parsed.User.Username() == "" {
		return URI{}, fmt.Errorf("user missing from URL: %s", raw)
	}
	user := parsed.User.Username()

	site := parsed.Hostname()
	if port := parsed.Port(); port != "" {
		site = site + ":" + port
	}

	return URI{
		User:   user,
		Site:   site,
		Ident:  user + "@" + site,
		Bucket: bucket,
		Key:    key,
		Secure: secure,
	}, nil
}

// HasWildcards reports whether s contains any of the glob metacharacters
// pegasus-s3 recognizes, matching s3.py's has_wildcards().
func HasWildcards(s string) bool {
	return strings.ContainsAny(s, "*?[]")
}
