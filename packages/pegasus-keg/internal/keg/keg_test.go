package keg

import (
	"bytes"
	"errors"
	"math"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

const fakeID = "IP addr and hostname: 192.0.2.1 (test)\n"

// fakeClock records the virtual time keg spins and sleeps for.
type fakeClock struct {
	slept, spun []time.Duration
}

// setup runs the test in a fresh temp dir with a virtual clock and a fixed
// identity line. ioCost is added to the clock by the first call to now()
// after Run starts, simulating time spent in the I/O phase.
func setup(t *testing.T, ioCost time.Duration) *fakeClock {
	t.Helper()
	t.Chdir(t.TempDir())
	c := &fakeClock{}
	cur, calls := time.Unix(1000, 0), 0
	oldNow, oldSleep, oldSpin, oldID := now, sleepFor, spinFor, identity
	now = func() time.Time {
		if calls++; calls == 2 {
			cur = cur.Add(ioCost)
		}
		return cur
	}
	sleepFor = func(d time.Duration) { c.slept = append(c.slept, d); cur = cur.Add(d) }
	spinFor = func(d time.Duration) { c.spun = append(c.spun, d); cur = cur.Add(d) }
	identity = func() string { return fakeID }
	t.Cleanup(func() { now, sleepFor, spinFor, identity = oldNow, oldSleep, oldSpin, oldID })
	return c
}

// run executes keg with stdin and returns exit code, stdout, stderr.
func run(stdin string, args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	rc := Run(append([]string{"/usr/bin/pegasus-keg"}, args...), strings.NewReader(stdin), &out, &errOut)
	return rc, out.String(), errOut.String()
}

func readFile(t *testing.T, fn string) string {
	t.Helper()
	b, err := os.ReadFile(fn)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestParseArgs(t *testing.T) {
	opts, help := parseArgs("keg", strings.Fields(
		"x -i a b -o c d=1K -G 5 -u K -a app -t10 -T 3 -s 7 -m 2 -l log -P >> -e HOME -C -r -p q -t -i e"))
	if help {
		t.Fatal("unexpected help")
	}
	want := options{
		app: "app", inputs: []string{"a", "b", "e"}, outputs: []string{"c", "d=1K"},
		sizes: []string{"5"}, unit: 'K', wall: 10, spin: 3, sleep: 7, memMB: 2,
		logfile: "log", prefix: ">>",
	}
	if !reflect.DeepEqual(opts, want) {
		t.Errorf("got  %+v\nwant %+v", opts, want)
	}
}

func TestParseArgsDashIsValue(t *testing.T) {
	opts, _ := parseArgs("keg", []string{"-i", "-", "-x", "-o", "-"})
	if !reflect.DeepEqual(opts.inputs, []string{"-", "-x"}) || !reflect.DeepEqual(opts.outputs, []string{"-"}) {
		t.Errorf("inputs=%q outputs=%q", opts.inputs, opts.outputs)
	}
}

func TestStrtoul(t *testing.T) {
	for s, want := range map[string]uint64{
		"12": 12, "+7": 7, " \n\t9x": 9, "abc": 0, "-3": 0, "": 0,
		"99999999999999999999999": math.MaxUint64,
	} {
		if got := strtoul(s); got != want {
			t.Errorf("strtoul(%q)=%d want %d", s, got, want)
		}
	}
	if got := seconds("99999999999999999999"); got != maxSeconds {
		t.Errorf("seconds not clamped: %d", got)
	}
}

func TestHelpMatchesCxx(t *testing.T) {
	want, err := os.ReadFile("testdata/help.golden") // captured from the C++ keg
	if err != nil {
		t.Fatal(err)
	}
	setup(t, 0)
	rc, out, _ := run("", "-t", "3", "-h")
	if rc != exitOK || out != string(want) {
		t.Errorf("rc=%d help differs:\n%s", rc, out)
	}
	for _, args := range [][]string{nil, {"-i", "x", "-h"}} {
		if rc, out, _ := run("", args...); rc != exitOK || !strings.HasPrefix(out, "Usage:\tpegasus-keg ") {
			t.Errorf("args %q: rc=%d out=%q", args, rc, out)
		}
	}
}

func TestInputsCopiedToOutput(t *testing.T) {
	setup(t, 0)
	os.WriteFile("f1", []byte("one\ntwo\n"), 0o644)
	if rc, _, errOut := run("stdin\n", "-P", ">>", "-i", "f1", "-", "-o", "out"); rc != exitOK {
		t.Fatalf("rc=%d stderr=%q", rc, errOut)
	}
	want := ">>--- start f1 ----\none\ntwo\n--- final f1 ----\n" +
		"--- start - ----\nstdin\n--- final - ----\n" + fakeID
	if got := readFile(t, "out"); got != want {
		t.Errorf("got %q\nwant %q", got, want)
	}
}

func TestInputsReadIntoMemory(t *testing.T) {
	setup(t, 0)
	os.WriteFile("small", []byte("x\n"), 0o644)
	os.WriteFile("big", bytes.Repeat([]byte("y"), 3<<20), 0o644)
	// Inputs fitting in -m, larger than -m, and -m without inputs: the -m
	// memory must never leak into outputs.
	for _, tc := range []struct{ in, want string }{
		{"small", "  --- start small ----\nx\n--- final small ----\n" + fakeID},
		{"big", "  --- start big ----\n" + strings.Repeat("y", 3<<20) + "--- final big ----\n" + fakeID},
		{"", "  " + fakeID},
	} {
		args := []string{"-m", "1", "-o", "out"}
		if tc.in != "" {
			args = append(args, "-i", tc.in)
		}
		if rc, _, _ := run("", args...); rc != exitOK {
			t.Fatalf("%s: rc=%d", tc.in, rc)
		}
		if got := readFile(t, "out"); got != tc.want {
			t.Errorf("%s: got %d bytes %.60q", tc.in, len(got), got)
		}
	}
}

func TestGeneratedSizes(t *testing.T) {
	setup(t, 0)
	if rc, _, _ := run("", "-G", "1", "130", "-u", "K", "-o", "a", "b", "c", "x=100", "y=2K", "z=5B", "w=5k"); rc != exitOK {
		t.Fatalf("rc=%d", rc)
	}
	for fn, size := range map[string]int{"a": 1024, "b": 130 << 10, "c": 1024, "x": 100, "y": 2048, "z": 5, "w": 5} {
		want := string(bytes.Repeat(block, size/len(block)+1)[:size]) + "\n" + fakeID
		if got := readFile(t, fn); got != want {
			t.Errorf("%s: len=%d want %d", fn, len(got), len(want))
		}
	}
}

func TestGeneratorWithZeroSize(t *testing.T) {
	setup(t, 0)
	rc, _, _ := run("x\n", "-G", "0", "-i", "-", "-o", "out")
	// -G selects generator mode even for 0 bytes: no prefix, no inputs.
	if got := readFile(t, "out"); rc != exitOK || got != "\n"+fakeID {
		t.Errorf("rc=%d out=%q", rc, got)
	}
}

func TestOutputToStdout(t *testing.T) {
	setup(t, 0)
	// "-=3B" is a file named "-", not stdout; only a bare "-" is stdout.
	if rc, out, _ := run("", "-o", "-=3B"); rc != exitOK || out != "" {
		t.Errorf("rc=%d stdout=%q", rc, out)
	}
	if rc, out, _ := run("", "-G", "3", "-o", "-", "-"); rc != exitOK || out != strings.Repeat("012\n"+fakeID, 2) {
		t.Errorf("rc=%d stdout=%q", rc, out)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestWriteErrorExits2(t *testing.T) {
	setup(t, 0)
	var errOut bytes.Buffer
	rc := Run([]string{"keg", "-G", "1", "-u", "G", "-o", "-"}, nil, failingWriter{}, &errOut)
	if rc != exitOpen || errOut.String() != "write(-): disk full\n" {
		t.Errorf("rc=%d stderr=%q", rc, errOut.String())
	}
}

func TestOverwritesWriteOnlyFile(t *testing.T) {
	setup(t, 0)
	os.WriteFile("out", []byte("old"), 0o200)
	if rc, _, errOut := run("", "-G", "2", "-o", "out"); rc != exitOK {
		t.Fatalf("rc=%d stderr=%q", rc, errOut)
	}
	os.Chmod("out", 0o600)
	if got := readFile(t, "out"); got != "01\n"+fakeID {
		t.Errorf("out=%q", got)
	}
}

func TestOutputCreatesDirectories(t *testing.T) {
	setup(t, 0)
	rc, out, _ := run("", "-G", "1", "-o", "d1/d2/f")
	if rc != exitOK {
		t.Fatalf("rc=%d", rc)
	}
	if _, err := os.Stat(filepath.Join("d1", "d2", "f")); err != nil {
		t.Error(err)
	}
	for _, line := range []string{"need to create directory is d1/d2/", "trying to create dir d1\n", "trying to create dir d1/d2\n"} {
		if !strings.Contains(out, line) {
			t.Errorf("stdout missing %q: %q", line, out)
		}
	}
}

func TestMkdirFailure(t *testing.T) {
	setup(t, 0)
	os.WriteFile("file", nil, 0o644)
	rc, _, errOut := run("", "-G", "1", "-o", "file/sub/f")
	if rc != exitMkdir || !strings.HasPrefix(errOut, "[error] Unable to mkdir file/sub/: ") {
		t.Errorf("rc=%d stderr=%q", rc, errOut)
	}
}

func TestMissingInput(t *testing.T) {
	setup(t, 0)
	rc, out, _ := run("", "-i", "nope", "-o", "out")
	if rc != exitOpen || out != "[error] open \"nope\": 2: No such file or directory\n" {
		t.Errorf("rc=%d stdout=%q", rc, out)
	}
	if _, err := os.Stat("out"); !errors.Is(err, os.ErrNotExist) {
		t.Error("output should not have been created")
	}
}

func TestDirectoryInputIsEmpty(t *testing.T) {
	setup(t, 0)
	os.Mkdir("adir", 0o755)
	rc, _, _ := run("", "-i", "adir", "-o", "out")
	want := "  --- start adir ----\n--- final adir ----\n" + fakeID
	if got := readFile(t, "out"); rc != exitOK || got != want {
		t.Errorf("rc=%d out=%q", rc, got)
	}
}

func TestUnopenableOutput(t *testing.T) {
	setup(t, 0)
	rc, _, errOut := run("", "-o", "nodir/x=5B")
	if rc != exitOpen || errOut != "open(nodir/x=5B): No such file or directory\n" {
		t.Errorf("rc=%d stderr=%q", rc, errOut)
	}
}

func TestImpossibleMemoryIsReported(t *testing.T) {
	setup(t, 0)
	rc, out, _ := run("", "-m", "1073741824") // 1 PiB
	if rc != exitOK || !strings.HasPrefix(out, "Memory allocation failure:  ") {
		t.Errorf("rc=%d stdout=%q", rc, out)
	}
}

func TestSpinAndSleep(t *testing.T) {
	c := setup(t, 2*time.Second)
	if rc, _, _ := run("", "-T", "5", "-t", "8", "-s", "30"); rc != exitOK {
		t.Fatalf("rc=%d", rc)
	}
	// I/O took 2s: spin the remaining 3s, sleep to t=8 (3s), then 30-8-5=17s.
	if !reflect.DeepEqual(c.spun, []time.Duration{3 * time.Second}) ||
		!reflect.DeepEqual(c.slept, []time.Duration{3 * time.Second, 17 * time.Second}) {
		t.Errorf("spun=%v slept=%v", c.spun, c.slept)
	}
}

func TestSleepAlone(t *testing.T) {
	c := setup(t, 2*time.Second)
	if rc, _, _ := run("", "-s", "4"); rc != exitOK || !reflect.DeepEqual(c.slept, []time.Duration{4 * time.Second}) {
		t.Errorf("rc=%d slept=%v", rc, c.slept)
	}
}

func TestOverTime(t *testing.T) {
	setup(t, 5*time.Second)
	rc, out, _ := run("", "-T", "2")
	if rc != exitOverTime || !strings.Contains(out, "you specified 2 [s] to spin but you've already exceeded this value by 3 [s]") {
		t.Errorf("rc=%d stdout=%q", rc, out)
	}
	setup(t, 5*time.Second)
	rc, out, _ = run("", "-t", "4")
	if rc != exitOverTime || !strings.Contains(out, "you specified 4 [s] to sleep but you've already exceeded this value by 1 [s]") {
		t.Errorf("rc=%d stdout=%q", rc, out)
	}
}

func TestEffectiveSleep(t *testing.T) {
	for _, tc := range []struct{ s, t, T, want int64 }{
		{3, 0, 0, 3},
		{5, 10, 0, 0}, // wrapped to ~2^64 in the C++ keg
		{10, 4, 0, 6},
		{10, 0, 4, 6},
		{30, 8, 5, 17},
		{12, 8, 5, 0},
		{0, 5, 5, 0},
	} {
		if got := effectiveSleep(tc.s, tc.t, tc.T); got != tc.want {
			t.Errorf("effectiveSleep(%d,%d,%d)=%d want %d", tc.s, tc.t, tc.T, got, tc.want)
		}
	}
}

func TestLogfile(t *testing.T) {
	setup(t, 0)
	os.WriteFile("log", []byte("old\n"), 0o644)
	if rc, _, _ := run("", "-l", "log"); rc != exitOK {
		t.Fatalf("rc=%d", rc)
	}
	if got := readFile(t, "log"); got != "old\n"+fakeID+"\n" {
		t.Errorf("log=%q", got)
	}
	// An unopenable log only warns.
	rc, _, errOut := run("", "-l", "nodir/log")
	if rc != exitOK || errOut != "WARNING: open(nodir/log): No such file or directory\n" {
		t.Errorf("rc=%d stderr=%q", rc, errOut)
	}
}

func TestSpinRunsAtLeastOnce(t *testing.T) {
	if n := spin(0); n < 1 {
		t.Errorf("spin(0)=%d", n)
	}
}

func TestFractal(t *testing.T) {
	if n := fractal(0, 0, 0, 0, 100); n != 100 {
		t.Errorf("origin should never escape, got %d", n)
	}
	if n := fractal(3, 0, 0, 0, 100); n != 0 {
		t.Errorf("|z|>2 escapes immediately, got %d", n)
	}
}

func TestDescribeHost(t *testing.T) {
	oldAddr, oldHost := lookupAddr, lookupHost
	t.Cleanup(func() { lookupAddr, lookupHost = oldAddr, oldHost })
	fail := func(string) ([]string, error) { return nil, errors.New("no") }
	for _, tc := range []struct {
		ip        string
		addr, fwd func(string) ([]string, error)
		want      string
	}{
		{"10.1.2.3", fail, fail, "10.1.2.3 (VPN)"},
		{"172.20.0.1", fail, fail, "172.20.0.1 (VPN)"},
		{"192.168.1.1", fail, fail, "192.168.1.1 (VPN)"},
		{"128.9.0.1", func(string) ([]string, error) { return []string{"a.isi.edu."}, nil }, fail, "128.9.0.1 (a.isi.edu)"},
		{"128.9.0.1", fail, fail, "128.9.0.1"},
		{"0.0.0.0", fail, func(string) ([]string, error) { return []string{"::1", "128.9.0.2"}, nil }, "128.9.0.2 (myhost)"},
		{"0.0.0.0", fail, fail, "0.0.0.0"},
	} {
		lookupAddr, lookupHost = tc.addr, tc.fwd
		if got := describeHost(net.ParseIP(tc.ip), "myhost"); got != tc.want {
			t.Errorf("%s: got %q want %q", tc.ip, got, tc.want)
		}
	}
}

func TestPrimaryIPv4(t *testing.T) {
	ip := primaryIPv4()
	if ip.To4() == nil || ip.IsLoopback() {
		t.Errorf("primaryIPv4()=%v, want a non-loopback IPv4 address or 0.0.0.0", ip)
	}
}
