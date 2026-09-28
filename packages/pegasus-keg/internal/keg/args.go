package keg

import (
	"fmt"
	"io"
	"math"
	"time"
)

// options holds the parsed command line.
type options struct {
	app     string   // -a
	inputs  []string // -i
	outputs []string // -o
	sizes   []string // -G
	unit    byte     // -u
	wall    int64    // -t, seconds
	spin    int64    // -T, seconds
	sleep   int64    // -s, seconds
	memMB   uint64   // -m
	logfile string   // -l
	prefix  string   // -P
}

// parseArgs parses args (without argv[0]) the way the C++ keg did: a flag
// switches the parser state and may carry an attached value (-t10). Values
// following a multi-value flag (-i, -o, -G, -e, -p) are collected until the
// next flag; a single-value flag consumes one value. "-" and unknown "-x"
// are plain values. -e, -p and -C are accepted but ignored, as are bare
// words before any flag. It returns help=true when -h is seen, with opts
// holding whatever was parsed up to that point.
func parseArgs(app string, args []string) (o options, help bool) {
	o = options{app: app, unit: 'B', prefix: "  "}
	var ignored []string
	multi := map[byte]*[]string{
		'i': &o.inputs, 'o': &o.outputs, 'G': &o.sizes, 'e': &ignored, 'p': &ignored,
	}
	single := map[byte]func(string){
		'a': func(s string) { o.app = s },
		'l': func(s string) { o.logfile = s },
		'P': func(s string) { o.prefix = s },
		'u': func(s string) { o.unit = s[0] },
		'm': func(s string) { o.memMB = strtoul(s) },
		't': func(s string) { o.wall = seconds(s) },
		'T': func(s string) { o.spin = seconds(s) },
		's': func(s string) { o.sleep = seconds(s) },
	}

	list, set := &ignored, (func(string))(nil)
	for _, s := range args {
		if len(s) > 1 && s[0] == '-' {
			c := s[1]
			switch {
			case c == 'h':
				return o, true
			case c == 'C':
				continue
			case c == 'r': // MPI-only (root-only -m); ignored
				s = s[2:]
			case multi[c] != nil:
				list, set, s = multi[c], nil, s[2:]
			case single[c] != nil:
				set, s = single[c], s[2:]
			}
		}
		if s == "" {
			continue
		}
		if set != nil {
			set(s)
			list, set = &ignored, nil
		} else {
			*list = append(*list, s)
		}
	}
	return o, false
}

// strtoul mimics C's strtoul(s, 0, 10) for keg's purposes: it skips leading
// whitespace and an optional '+', parses the leading decimal digits, and
// saturates on overflow. Anything else (including a '-' sign) yields 0.
func strtoul(s string) uint64 {
	i := 0
	for i < len(s) && (s[i] == ' ' || (s[i] >= '\t' && s[i] <= '\r')) {
		i++
	}
	if i < len(s) && s[i] == '+' {
		i++
	}
	var n uint64
	for ; i < len(s) && s[i] >= '0' && s[i] <= '9'; i++ {
		d := uint64(s[i] - '0')
		if n > (math.MaxUint64-d)/10 {
			return math.MaxUint64
		}
		n = n*10 + d
	}
	return n
}

// maxSeconds keeps second counts convertible to a time.Duration.
const maxSeconds = int64(math.MaxInt64 / time.Second)

// seconds parses a -t/-T/-s value, clamped to maxSeconds.
func seconds(s string) int64 {
	return int64(min(strtoul(s), uint64(maxSeconds)))
}

// unitMultiplier returns the number of bytes in the data unit B, K, M or G,
// and whether c is one of those units.
func unitMultiplier(c byte) (uint64, bool) {
	switch c {
	case 'B':
		return 1, true
	case 'K':
		return 1 << 10, true
	case 'M':
		return 1 << 20, true
	case 'G':
		return 1 << 30, true
	}
	return 1, false
}

// effectiveSleep reconciles -s with -t/-T the way the C++ keg intended:
// -s is reduced by -t and -T, and is dropped when either exceeds it. The C++
// code did this on unsigned values, so e.g. -s 5 -t 10 wrapped around and
// slept for ~2^64 seconds.
func effectiveSleep(sleep, wall, spin int64) int64 {
	if sleep > 0 && wall > 0 {
		sleep = abs(sleep - wall)
	}
	if sleep > 0 && spin > 0 {
		sleep = abs(sleep - spin)
	}
	if wall > sleep || spin > sleep {
		sleep = 0
	}
	return sleep
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func printHelp(w io.Writer, o options) {
	fmt.Fprintf(w, "Usage:\t%s [-a appname] [(-s|-t|-T) thinktime] [-l fn] [-o fn [..]]\n"+
		"\t[-i fn [..] | -G size] [-e env [..]] [-p p [..]] [-P ps] [-h]\n", o.app)
	fmt.Fprintf(w, " -a app\tset name of application to something else, default %s\n", o.app)
	fmt.Fprintf(w, " -m me\tallocate 'me' MB of memory\n")
	fmt.Fprintf(w, " -t to\tsleep for 'to' seconds during execution, default %d\n", o.wall)
	fmt.Fprintf(w, " -s to\tsleep for 'to' seconds after the I/O phase, default %d\n", o.sleep)
	fmt.Fprintf(w, " -T to\tspin for 'to' seconds during execution, default %d\n", o.spin)
	fmt.Fprintf(w, " -l fn\tappend own information atomically to a logfile\n")
	fmt.Fprintf(w, " -o ..\tenumerate space-separated list output files to create\n"+
		"        Accept also '<filename>=<filesize><data_unit>' form, where <data_unit>\n"+
		"        is a character supported by '-u' switch.\n"+
		"        If you don't specify input files you have to specify output file size.\n")
	fmt.Fprintf(w, " -i ..\tenumerate space-separated list input to read and copy\n")
	fmt.Fprintf(w, " -G ..\tenumerate space-separated list of output file sizes\n")
	fmt.Fprintf(w, " -u un\tdata unit for output files generator - accepted values includes [ B K M G ], default is B\n")
	fmt.Fprintf(w, " -p ..\tenumerate space-separated parameters to mention\n")
	fmt.Fprintf(w, " -e ..\tenumerate space-separated environment values to print\n")
	fmt.Fprintf(w, " -C\tprint all environment variables starting with _CONDOR\n")
	fmt.Fprintf(w, " -P ps\tprefix input file lines with 'ps', default \"%s\"\n", o.prefix)
	fmt.Fprintf(w, " -h\tshows this help message\n")
}
