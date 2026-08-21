// Package model holds the transfer request types read from pegasus-transfer's
// JSON input format, and the PegasusURL representation shared by all of them.
package model

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
)

// reParseURL mirrors transfer.py's re_parse_url: proto://host/path. Left
// unanchored to match Python's re.search semantics (leftmost match, need not
// consume the whole string).
var reParseURL = regexp.MustCompile(`([\w]+)://([\w.\-:@#]*)(/[\S ]*)?`)

// reEnvVar mirrors transfer.py's expand_env_vars regex: ${VAR} or $VAR.
var reEnvVar = regexp.MustCompile(`\$\{?([a-zA-Z][a-zA-Z0-9_]+)\}?`)

// ExpandEnvVars replaces $VAR / ${VAR} references with the current process
// environment, exactly like transfer.py's expand_env_vars(). Unknown
// variables expand to the empty string (not left as-is), and Go's raw byte
// slice regex substitution matches Python's re.sub character semantics.
func ExpandEnvVars(s string) string {
	return reEnvVar.ReplaceAllStringFunc(s, func(m string) string {
		sub := reEnvVar.FindStringSubmatch(m)
		return os.Getenv(sub[1])
	})
}

// re for collapsing repeated slashes in a parsed URL path, matching
// transfer.py's `re.sub("//+", "/", self.path)`.
var reMultiSlash = regexp.MustCompile(`//+`)

// PegasusURL is a parsed transfer endpoint: a URL, its site label, and an
// optional priority used to order multiple sources/destinations. It mirrors
// transfer.py's PegasusURL class field-for-field so the frozen JSON input
// contract and grouping/dispatch logic behave identically.
type PegasusURL struct {
	URL       string
	FileType  string // "" if unspecified
	SiteLabel string
	Priority  int

	Proto string
	Host  string
	Path  string
}

// NewPegasusURL parses url/fileType/siteLabel into a PegasusURL, matching
// transfer.py's PegasusURL.__init__ / _parse_url.
func NewPegasusURL(rawURL, fileType, siteLabel string, priority int) (*PegasusURL, error) {
	p := &PegasusURL{
		URL:       rawURL,
		FileType:  fileType,
		SiteLabel: strings.ReplaceAll(siteLabel, "-", "_"),
		Priority:  priority,
	}
	if err := p.parse(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *PegasusURL) parse() error {
	// default protocol is file://
	if !strings.Contains(p.URL, ":") {
		p.URL = "file://" + p.URL
	}

	switch {
	case strings.HasPrefix(p.URL, "file:"):
		p.Proto = "file"
		p.Path = ExpandEnvVars(stripFileLikePrefix(p.URL, "file"))
		return nil
	case strings.HasPrefix(p.URL, "symlink:"):
		p.Proto = "symlink"
		p.Path = ExpandEnvVars(stripFileLikePrefix(p.URL, "symlink"))
		return nil
	case strings.HasPrefix(p.URL, "moveto:"):
		p.Proto = "moveto"
		p.Path = ExpandEnvVars(stripFileLikePrefix(p.URL, "moveto"))
		return nil
	}

	m := reParseURL.FindStringSubmatch(p.URL)
	if m == nil {
		return fmt.Errorf("unable to parse URL: %s", p.URL)
	}
	p.Proto = m[1]
	p.Host = m[2]
	p.Path = reMultiSlash.ReplaceAllString(m[3], "/")
	return nil
}

// stripFileLikePrefix strips a "<scheme>:", "<scheme>://" or
// "<scheme>://localhost" prefix (file: only) from a URL, mirroring the
// per-scheme regexes in transfer.py's PegasusURL._parse_url.
func stripFileLikePrefix(rawURL, scheme string) string {
	rest := strings.TrimPrefix(rawURL, scheme+":")
	if scheme == "file" {
		rest = strings.TrimPrefix(rest, "//localhost")
		rest = strings.TrimPrefix(rest, "//")
		// re-apply in case only "//" (no localhost) matched above already
		return rest
	}
	rest = strings.TrimPrefix(rest, "//")
	return rest
}

// GetURL reconstructs the URL string, matching PegasusURL.get_url(). srm and
// root protocols get an extra leading slash for historical compatibility
// with broken srm-copy URLs.
func (p *PegasusURL) GetURL() string {
	if p.Proto == "srm" || p.Proto == "root" {
		return fmt.Sprintf("%s://%s/%s", p.Proto, p.Host, p.Path)
	}
	return fmt.Sprintf("%s://%s%s", p.Proto, p.Host, p.Path)
}

// GetURLEncoded matches PegasusURL.get_url_encoded(): the path component is
// percent-encoded (space etc.), proto/host are not.
func (p *PegasusURL) GetURLEncoded() string {
	return fmt.Sprintf("%s://%s%s", p.Proto, p.Host, encodePath(p.Path))
}

// encodePath percent-encodes a URL path the way Python's urllib.parse.quote
// does with default safe="/": every path segment is escaped individually so
// existing "/" separators survive.
func encodePath(path string) string {
	segments := strings.Split(path, "/")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	return strings.Join(segments, "/")
}

// GetURLDirname matches PegasusURL.get_url_dirname(): proto://host + dirname(path).
func (p *PegasusURL) GetURLDirname() string {
	return fmt.Sprintf("%s://%s%s", p.Proto, p.Host, dirname(p.Path))
}

func dirname(path string) string {
	i := strings.LastIndex(path, "/")
	if i < 0 {
		return ""
	}
	if i == 0 {
		return "/"
	}
	return path[:i]
}
