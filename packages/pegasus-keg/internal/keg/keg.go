// Package keg implements pegasus-keg, the Kanonical Executable for Grids.
//
// keg runs in phases: read all input files into memory, write each output
// file (either the input content or generated data of a given size, followed
// by an identification line), spin the CPU until -T seconds have passed,
// sleep until -t seconds have passed, sleep a further -s seconds, and finally
// append the identification line to the -l log file.
package keg

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Exit codes.
const (
	exitOK       = 0
	exitMkdir    = 1 // could not create an output file's parent directory
	exitOpen     = 2 // could not open (or write) an input or output file
	exitOverTime = 3 // I/O took longer than the -T/-t budget
)

// Hooks replaced by tests.
var (
	now      = time.Now
	sleepFor = time.Sleep
	spinFor  = func(d time.Duration) { spin(d) }
	identity = identify
)

// block is the repeating 64-byte pattern written to generated output files.
var block = []byte(strings.Repeat("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz\r\n", 1024))

// Run executes keg with the given argv and returns the process exit code.
func Run(argv []string, stdin io.Reader, stdout, stderr io.Writer) int {
	start := now()

	opts, help := parseArgs(filepath.Base(argv[0]), argv[1:])
	if help || len(argv) == 1 {
		printHelp(stdout, opts)
		return exitOK
	}

	// Phase 1: allocate the -m memory and read the inputs, into that memory
	// when they fit (so peak usage is max(-m, inputs), as in the C++ keg).
	ballast := allocate(opts.memMB, stdout)
	defer func() {
		if ballast != nil {
			syscall.Munmap(ballast)
		}
	}()
	buf := ballast[:0]
	if n := inputSize(opts.inputs); uint64(n) > uint64(len(ballast)) {
		if ballast != nil {
			syscall.Munmap(ballast)
			ballast = nil
		}
		buf = make([]byte, 0, n)
	}
	content, err := readInputs(buf, opts.inputs, stdin)
	if err != nil {
		var pe *fs.PathError
		errors.As(err, &pe)
		fmt.Fprintf(stdout, "[error] open \"%s\": %d: %s\n", pe.Path, errno(err), strerror(err))
		return exitOpen
	}

	// Phase 2: write outputs.
	var id string
	if len(opts.outputs) > 0 || opts.logfile != "" {
		id = identity()
	}
	for i, spec := range opts.outputs {
		fn, size, plain := parseOutputSpec(spec)
		generated := size > 0 || len(opts.sizes) > 0 // -G forces generator mode, even for 0 bytes
		if size == 0 && len(opts.sizes) > 0 {
			mult, _ := unitMultiplier(opts.unit)
			size = strtoul(opts.sizes[i%len(opts.sizes)]) * mult
		}
		if plain && spec != "-" {
			if rc := makeParents(fn, stdout, stderr); rc != exitOK {
				return rc
			}
		}
		rc := output(spec, fn, stdout, stderr, func(w io.Writer) error {
			return writeOutput(w, opts.prefix, content, generated, size, id)
		})
		if rc != exitOK {
			return rc
		}
	}

	// Phase 3: spin until -T seconds after start.
	// Phase 4: sleep until -t seconds after start, then a further -s seconds.
	if !budget(stdout, "spin", opts.spin, start, spinFor) ||
		!budget(stdout, "sleep", opts.wall, start, sleepFor) {
		return exitOverTime
	}
	if s := effectiveSleep(opts.sleep, opts.wall, opts.spin); s > 0 {
		sleepFor(time.Duration(s) * time.Second)
	}

	if opts.logfile != "" {
		appendLog(opts.logfile, id, stderr)
	}
	return exitOK
}

// allocate maps and touches memMB MiB of memory, so it is actually resident.
// Anonymous mmap rather than make() lets an impossible size fail with ENOMEM
// (reported, then carry on, like the C++ keg's malloc) instead of the Go
// runtime crashing or paging forever.
func allocate(memMB uint64, stdout io.Writer) []byte {
	if memMB == 0 {
		return nil
	}
	mem, err := syscall.Mmap(-1, 0, int(memMB<<20),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		fmt.Fprintf(stdout, "Memory allocation failure:  %s\n", strerror(err))
		return nil
	}
	for i := 0; i < len(mem); i += os.Getpagesize() {
		mem[i] = 'Z'
	}
	return mem
}

// budget runs wait for whatever is left of total seconds since start. It
// reports and returns false if that time has already passed.
func budget(stdout io.Writer, what string, total int64, start time.Time, wait func(time.Duration)) bool {
	if total <= 0 {
		return true
	}
	left := total - int64(now().Sub(start).Seconds())
	if left < 0 {
		fmt.Fprintf(stdout, "[error] you specified %d [s] to %s but you've already exceeded this value by %d [s]\n", total, what, -left)
		return false
	}
	wait(time.Duration(left) * time.Second)
	return true
}

// inputSize estimates the bytes readInputs will produce; stdin counts as 0.
func inputSize(inputs []string) int {
	n := 0
	for _, fn := range inputs {
		n += 2 * len(marker("start", fn))
		if st, err := os.Stat(fn); fn != "-" && err == nil && st.Mode().IsRegular() {
			n += int(st.Size())
		}
	}
	return n
}

func marker(what, fn string) string { return "--- " + what + " " + fn + " ----\n" }

// readInputs appends all input files ("-" is stdin), each wrapped in
// start/final marker lines, to buf.
func readInputs(buf []byte, inputs []string, stdin io.Reader) ([]byte, error) {
	var err error
	for _, fn := range inputs {
		buf = append(buf, marker("start", fn)...)
		if buf, err = readInput(buf, fn, stdin); err != nil {
			return nil, err
		}
		buf = append(buf, marker("final", fn)...)
	}
	return buf, nil
}

// readInput appends one input file to buf. A directory reads as empty, as
// it did in the C++ keg.
func readInput(buf []byte, fn string, stdin io.Reader) ([]byte, error) {
	r := stdin
	if fn != "-" {
		f, err := os.Open(fn)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	}
	buf, err := readAll(buf, r)
	if err != nil && !errors.Is(err, syscall.EISDIR) {
		return nil, &fs.PathError{Op: "read", Path: fn, Err: err}
	}
	return buf, nil
}

// readAll appends r's content to buf, like io.ReadAll but without the copy.
func readAll(buf []byte, r io.Reader) ([]byte, error) {
	for {
		if len(buf) == cap(buf) {
			buf = append(buf, 0)[:len(buf)]
		}
		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err == io.EOF {
			return buf, nil
		}
		if err != nil {
			return buf, err
		}
	}
}

// parseOutputSpec splits an -o value "fn" or "fn=<n>[BKMG]" into the file
// name and size; plain reports the "fn" form.
func parseOutputSpec(spec string) (fn string, size uint64, plain bool) {
	eq := strings.LastIndexByte(spec, '=')
	if eq < 0 {
		return spec, 0, true
	}
	num := spec[eq+1:]
	mult := uint64(1)
	if n := len(num); n > 0 {
		if m, ok := unitMultiplier(num[n-1]); ok {
			mult, num = m, num[:n-1]
		}
	}
	return spec[:eq], strtoul(num) * mult, false
}

// output opens the output file fn ("-" is stdout), runs write on it and
// closes it, reporting failures the way the C++ keg did.
func output(spec, fn string, stdout, stderr io.Writer, write func(io.Writer) error) int {
	if spec == "-" {
		if err := write(stdout); err != nil {
			fmt.Fprintf(stderr, "write(%s): %s\n", spec, strerror(err))
			return exitOpen
		}
		return exitOK
	}
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o666)
	if err != nil {
		fmt.Fprintf(stderr, "open(%s): %s\n", spec, strerror(err))
		return exitOpen
	}
	err = write(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		fmt.Fprintf(stderr, "write(%s): %s\n", spec, strerror(err))
		return exitOpen
	}
	return exitOK
}

// writeOutput writes either size bytes of generated data plus a newline, or
// the prefix and input content, followed by the identification line.
func writeOutput(w io.Writer, prefix string, content []byte, generated bool, size uint64, id string) error {
	bw := bufio.NewWriter(w)
	if generated {
		for size > 0 {
			n := min(size, uint64(len(block)))
			if _, err := bw.Write(block[:n]); err != nil {
				return err
			}
			size -= n
		}
		bw.WriteByte('\n')
	} else {
		bw.WriteString(prefix)
		bw.Write(content)
	}
	bw.WriteString(id)
	return bw.Flush()
}

// makeParents creates the parent directories of fn, printing the same
// progress chatter as the C++ keg.
func makeParents(fn string, stdout, stderr io.Writer) int {
	fmt.Fprintf(stdout, "output file to be generated is %s\n", fn)
	slash := strings.LastIndexByte(fn, '/')
	if slash < 0 {
		return exitOK
	}
	dir := fn[:slash+1]
	fmt.Fprintf(stdout, "need to create directory is %s\n", dir)
	for i := 1; i < len(dir); i++ {
		if dir[i] != '/' {
			continue
		}
		fmt.Fprintf(stdout, "trying to create dir %s\n", dir[:i])
		if err := os.Mkdir(dir[:i], 0o777); err != nil && !errors.Is(err, fs.ErrExist) {
			fmt.Fprintf(stderr, "[error] Unable to mkdir %s: %d: %s\n", dir, errno(err), strerror(err))
			return exitMkdir
		}
	}
	return exitOK
}

// appendLog appends line plus a blank line to logfile in a single write, so
// concurrent keg jobs sharing a log don't interleave.
func appendLog(logfile, line string, stderr io.Writer) {
	f, err := os.OpenFile(logfile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o666)
	if err != nil {
		fmt.Fprintf(stderr, "WARNING: open(%s): %s\n", logfile, strerror(err))
		return
	}
	defer f.Close()
	f.WriteString(line + "\n")
}

// errno returns err's errno, or 0.
func errno(err error) int {
	var no syscall.Errno
	if errors.As(err, &no) {
		return int(no)
	}
	return 0
}

// strerror returns err's message capitalized like C's strerror
// ("No such file or directory").
func strerror(err error) string {
	var no syscall.Errno
	if !errors.As(err, &no) {
		return err.Error()
	}
	msg := no.Error()
	return strings.ToUpper(msg[:1]) + msg[1:]
}
