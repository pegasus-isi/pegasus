// Command pegasus-integrity either generates a file checksum (usually
// called from pegasus-kickstart) or verifies a checksum for a file using
// metadata in the current working directory (usually from PegasusLite). Go
// port of pegasus-integrity.py.
//
// The --generate-xmls and --generate-fullstat-xmls flags from the Python
// original are intentionally NOT ported: they were already dead code there
// (accepted by the option parser and counted toward the "exactly one flag"
// validation, but never handled by any branch of main() — passing either
// flag silently did nothing and exited 0). Nothing in kickstart, PegasusLite,
// or the planner ever passed them.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/pegasus-isi/pegasus/packages/pegasus-integrity/internal/integrity"
)

// logger writes "Integrity check: <message>" lines to stdout — matching the
// Python original's `sys.stderr = sys.stdout` plus its log Formatter, so
// that a caller capturing this tool's stdout sees both log output and any
// plain print() output interleaved on a single stream, with nothing lost to
// a separate stderr.
type logger struct{ debug bool }

func (l logger) Debugf(format string, a ...any) {
	if l.debug {
		fmt.Printf("Integrity check: "+format+"\n", a...)
	}
}
func (l logger) Errorf(format string, a ...any) {
	fmt.Printf("Integrity check: "+format+"\n", a...)
}

type args struct {
	generate             string
	generateYAML         string
	generateFullstatYAML string
	verify               string
	printTimings         bool
	debug                bool

	generateSet, generateYAMLSet, generateFullstatYAMLSet, verifySet bool
}

func parseArgs(argv []string) (args, error) {
	fs := flag.NewFlagSet("pegasus-integrity", flag.ContinueOnError)
	var a args
	fs.StringVar(&a.generate, "generate", "", "Generate a SHA256 hash for a set of files")
	fs.StringVar(&a.generateYAML, "generate-yaml", "", "Generate hashes for the given file, output to kickstart yaml")
	fs.StringVar(&a.generateFullstatYAML, "generate-fullstat-yaml", "", "Generate hashes for the given file, output to kickstart yaml, with file stat records")
	fs.StringVar(&a.verify, "verify", "", "Verify the hash for the given file")
	fs.BoolVar(&a.printTimings, "print-timings", false, "Display timing data after verifying files")
	fs.BoolVar(&a.debug, "debug", false, "Enables debugging output")

	if err := fs.Parse(argv); err != nil {
		return args{}, err
	}
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "generate":
			a.generateSet = true
		case "generate-yaml":
			a.generateYAMLSet = true
		case "generate-fullstat-yaml":
			a.generateFullstatYAMLSet = true
		case "verify":
			a.verifySet = true
		}
	})
	return a, nil
}

func main() {
	a, err := parseArgs(os.Args[1:])
	if err != nil {
		os.Exit(2)
	}
	log := logger{debug: a.debug}

	set := 0
	for _, ok := range []bool{a.generateSet, a.generateYAMLSet, a.generateFullstatYAMLSet, a.verifySet} {
		if ok {
			set++
		}
	}
	if set != 1 {
		log.Errorf("One, and only one, of --generate-* and --verify needs to be specified")
		fmt.Fprintln(os.Stderr, "usage: pegasus-integrity [options]")
		os.Exit(1)
	}

	switch {
	case a.generateSet:
		runGenerate(a.generate)
	case a.generateYAMLSet:
		runGenerateYAML(a.generateYAML)
	case a.generateFullstatYAMLSet:
		runGenerateFullstatYAML(a.generateFullstatYAML)
	case a.verifySet:
		os.Exit(runVerify(a.verify, a.printTimings, log))
	}
	os.Exit(0)
}

// splitLFNPFN splits an "lfn=pfn" or bare "pfn" entry on the first "=",
// matching `if "=" in f: lfn, pfn = str.split(f, "=", 1)`.
func splitLFNPFN(entry string) (lfn, pfn string) {
	if i := strings.Index(entry, "="); i >= 0 {
		return entry[:i], entry[i+1:]
	}
	return "", entry
}

func runGenerate(spec string) {
	for _, f := range strings.Split(spec, ";;;") {
		sum, err := integrity.GenerateSHA256(f)
		if err != nil {
			fmt.Printf("Integrity check: %s\n", err)
			os.Exit(1)
		}
		fmt.Println(sum + "  " + f)
	}
}

func runGenerateYAML(spec string) {
	for _, f := range strings.Split(spec, ";;;") {
		_, pfn := splitLFNPFN(f)
		result, err := integrity.GenerateYAML(pfn)
		if err != nil || result == "" {
			if err != nil {
				fmt.Printf("Integrity check: %s\n", err)
			}
			os.Exit(1)
		}
		// print(results): Python's print() adds its own trailing newline on
		// top of the string's already-trailing "\n", producing a blank line
		// between entries — replicated here for output parity.
		fmt.Println(result)
	}
}

func runGenerateFullstatYAML(spec string) {
	dataFile, hasDataFile := os.LookupEnv("KICKSTART_INTEGRITY_DATA")
	for _, f := range strings.Split(spec, ";;;") {
		lfn, pfn := splitLFNPFN(f)
		if lfn == "" {
			// No "=" present: Python leaves lfn as None, and
			// generate_fullstat_yaml() renders it via plain %s
			// interpolation ('    "%s":\n' % lfn), which stringifies None
			// as the literal text "None" — not an empty/omitted key.
			lfn = "None"
		}
		result, err := integrity.GenerateFullstatYAML(lfn, pfn)
		if err != nil || result == "" {
			if err != nil {
				fmt.Printf("Integrity check: %s\n", err)
			}
			os.Exit(1)
		}
		if hasDataFile {
			appendToFile(dataFile, result)
		} else {
			fmt.Println(result)
		}
	}
}

func appendToFile(path, s string) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Printf("Integrity check: unable to write %s: %s\n", path, err)
		return
	}
	defer f.Close()
	f.WriteString(s)
}

// runVerify implements --verify: unlike the generate-* modes (which exit
// immediately on the first failure), this checks every entry and only
// exits with a nonzero code after the full list has been processed,
// matching main()'s `exit_code = 0; ...; myexit(exit_code)` loop.
func runVerify(spec string, printTimings bool, log logger) int {
	mp := integrity.NewMultipartWriter()
	defer mp.Close()
	if printTimings {
		mp.WriteAttemptsHeader()
	}

	metaData := loadMetaFiles(log)

	var files string
	if spec == "stdin" {
		b, _ := io.ReadAll(os.Stdin)
		files = strings.TrimSpace(string(b))
	} else {
		files = spec
	}

	stats := &integrity.Stats{}
	exitCode := 0
	for _, f := range strings.Split(files, ";;;") {
		lfn, pfn := splitLFNPFN(f)
		result := integrity.CheckIntegrity(pfn, lfn, metaData, stats)

		if result.NoMetaEntry {
			log.Errorf("No checksum in the meta data for %s", result.LFN)
			exitCode = 1
			continue
		}
		if printTimings {
			mp.WriteCheckInfo(result)
		}
		if !result.Success {
			log.Errorf(
				"%s: Expected checksum (%s) does not match the calculated checksum (%s) (timing: %.3f)",
				result.PFN, result.ExpectedSHA256, result.SHA256, result.Elapsed.Seconds(),
			)
			exitCode = 1
		}
	}

	if printTimings {
		mp.WriteSummary(*stats)
	}
	return exitCode
}

// loadMetaFiles reads every *.meta file in the current working directory,
// matching `for meta_file in glob.glob("*.meta")`.
func loadMetaFiles(log logger) []integrity.MetaEntry {
	matches, _ := filepath.Glob("*.meta")
	var all []integrity.MetaEntry
	for _, m := range matches {
		log.Debugf("Loading metadata from %s", m)
		entries, err := integrity.ReadMetaData(m)
		if err != nil {
			log.Errorf("%s", err)
			continue
		}
		all = append(all, entries...)
	}
	return all
}
