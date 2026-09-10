package integrity

import (
	"fmt"
	"os"
	"time"
)

// MultipartWriter mirrors multipart_out()/check_info_yaml()/
// dump_summary_yaml(): when $PEGASUS_MULTIPART_DIR is set, per-file and
// summary integrity-check stats are appended (as YAML) to
// $PEGASUS_MULTIPART_DIR/<unix-time-of-first-write>-integrity, for
// pegasus-monitord to pick up. A no-op when the env var is unset. Once
// opening the file fails, further writes are silently dropped for the rest
// of the run, matching the Python original's tri-state (unset / failed /
// open) behavior.
type MultipartWriter struct {
	dir    string
	f      *os.File
	failed bool
}

// NewMultipartWriter reads $PEGASUS_MULTIPART_DIR once; the file itself is
// opened lazily on first Write, matching multipart_out()'s "open only if
// something is actually written" behavior.
func NewMultipartWriter() *MultipartWriter {
	dir, _ := os.LookupEnv("PEGASUS_MULTIPART_DIR")
	return &MultipartWriter{dir: dir}
}

func (m *MultipartWriter) Write(s string) {
	if m.dir == "" || m.failed {
		return
	}
	if m.f == nil {
		path := fmt.Sprintf("%s/%d-integrity", m.dir, time.Now().Unix())
		f, err := os.Create(path)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Integrity check: Unable to write stats to $PEGASUS_MULTIPART_DIR:", err)
			m.failed = true
			return
		}
		m.f = f
	}
	fmt.Fprint(m.f, s)
}

func (m *MultipartWriter) Close() {
	if m.f != nil {
		m.f.Close()
	}
}

// WriteAttemptsHeader emits the "- integrity_verification_attempts:" list
// header, matching main()'s `multipart_out("- integrity_verification_attempts:\n")`.
func (m *MultipartWriter) WriteAttemptsHeader() {
	m.Write("- integrity_verification_attempts:\n")
}

// WriteCheckInfo emits one --verify result, matching check_info_yaml().
func (m *MultipartWriter) WriteCheckInfo(r CheckResult) {
	m.Write(fmt.Sprintf(
		"  - lfn: %q\n    pfn: %q\n    sha256: %s\n    success: %s\n",
		r.LFN, r.PFN, r.SHA256, pyBool(r.Success),
	))
	if !r.Success {
		m.Write(fmt.Sprintf("    sha256_expected: %s\n", r.ExpectedSHA256))
	}
}

// WriteSummary emits the closing integrity_summary block, matching
// dump_summary_yaml().
func (m *MultipartWriter) WriteSummary(s Stats) {
	m.Write(fmt.Sprintf(
		"- integrity_summary:\n    succeeded: %d\n    failed: %d\n    duration: %.3f\n",
		s.Succeeded, s.Failed, s.Duration.Seconds(),
	))
}

// pyBool renders a bool the way Python's str(bool) does ("True"/"False"),
// matching the exact YAML text the original tool emits.
func pyBool(b bool) string {
	if b {
		return "True"
	}
	return "False"
}
