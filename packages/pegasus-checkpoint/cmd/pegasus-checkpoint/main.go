// Command pegasus-checkpoint periodically transfers checkpoints back to the
// staging site. It is a Go port of pegasus-checkpoint.py: same CLI flags,
// same trigger semantics (SIGUSR1 or a periodic interval), same archive
// name and pegasus-transfer callout.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-checkpoint/internal/checkpoint"
)

// patternList implements flag.Value to collect repeated -p/--pattern flags,
// matching argparse's action="append".
type patternList []string

func (p *patternList) String() string { return fmt.Sprint([]string(*p)) }
func (p *patternList) Set(v string) error {
	*p = append(*p, v)
	return nil
}

type config struct {
	patterns   []string
	interval   int  // seconds; only meaningful if intervalSet
	intervalOK bool // true if -i/--interval was given AND is > 0 (matches
	// Python's `if args.interval:` treating an explicit 0 as falsy — no
	// periodic notifier is started in that case either)
	debug     bool
	logToFile bool
}

func parseArgs(args []string) (config, error) {
	fs := flag.NewFlagSet("pegasus-checkpoint", flag.ContinueOnError)

	var patterns patternList
	fs.Var(&patterns, "p", "regex pattern to match when searching for files to checkpoint")
	fs.Var(&patterns, "pattern", "regex pattern to match when searching for files to checkpoint")

	var intervalStr string
	fs.StringVar(&intervalStr, "i", "", "interval in seconds at which to send checkpoints back to the staging site")
	fs.StringVar(&intervalStr, "interval", "", "interval in seconds at which to send checkpoints back to the staging site")

	var debug bool
	fs.BoolVar(&debug, "d", false, "enable debug logging")
	fs.BoolVar(&debug, "debug", false, "enable debug logging")

	var logToFile bool
	fs.BoolVar(&logToFile, "l", false, "enable logging to file pegasus-checkpoint.log")
	fs.BoolVar(&logToFile, "log-to-file", false, "enable logging to file pegasus-checkpoint.log")

	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	cfg := config{patterns: patterns, debug: debug, logToFile: logToFile}
	if intervalStr != "" {
		v, err := strconv.Atoi(intervalStr)
		if err != nil || v < 0 {
			return config{}, fmt.Errorf("interval %s must be a nonnegative integer", intervalStr)
		}
		cfg.interval = v
		cfg.intervalOK = v > 0
	}
	return cfg, nil
}

// logger is a tiny stand-in for the Python tool's logging setup: always logs
// to stderr, optionally tees to pegasus-checkpoint.log (plain append, no
// rotation — a deliberate simplification for this short-lived per-job
// process, see the plan of record).
type logger struct {
	debug bool
	mu    sync.Mutex
	out   *log.Logger
}

func newLogger(debug, logToFile bool) (*logger, func(), error) {
	writers := []io.Writer{os.Stderr}
	closeFn := func() {}
	if logToFile {
		f, err := os.OpenFile("pegasus-checkpoint.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, nil, err
		}
		writers = append(writers, f)
		closeFn = func() { f.Close() }
	}
	out := log.New(io.MultiWriter(writers...), "", 0)
	return &logger{debug: debug, out: out}, closeFn, nil
}

func (l *logger) log(level, format string, a ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.out.Printf("%s [%s] %s", time.Now().Format("2006-01-02 15:04:05,000"), level, fmt.Sprintf(format, a...))
}

func (l *logger) Debugf(format string, a ...any) {
	if l.debug {
		l.log("DEBUG", format, a...)
	}
}
func (l *logger) Infof(format string, a ...any)  { l.log("INFO", format, a...) }
func (l *logger) Warnf(format string, a ...any)  { l.log("WARNING", format, a...) }
func (l *logger) Errorf(format string, a ...any) { l.log("ERROR", format, a...) }

func main() {
	cfg, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	log, closeLog, err := newLogger(cfg.debug, cfg.logToFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pegasus-checkpoint:", err)
		os.Exit(1)
	}
	defer closeLog()

	if err := checkpoint.WritePID(); err != nil {
		log.Errorf("unable to write PID file: %v", err)
	} else {
		log.Debugf("started with PID: %d", os.Getpid())
	}

	// notify is a 1-buffered "event" — set() is a non-blocking send (a
	// pending, unconsumed notification stays coalesced into one wakeup,
	// matching threading.Event's set()/clear() semantics), the worker loop
	// below is the sole receiver (its wait()).
	notify := make(chan struct{}, 1)
	signalNotify := func() {
		select {
		case notify <- struct{}{}:
		default:
		}
	}

	if cfg.intervalOK {
		go func() {
			log.Debugf("PeriodicCheckpointNotifier started with interval: %d seconds", cfg.interval)
			for {
				time.Sleep(time.Duration(cfg.interval) * time.Second)
				signalNotify()
				log.Debugf("notify event set")
			}
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGUSR1)
	go func() {
		for range sigCh {
			log.Infof("SIGUSR1 received, setting notification")
			signalNotify()
		}
	}()

	runWorker(notify, cfg.patterns, log)
}

// runWorker mirrors CheckpointWorker.run: wait for a notification, archive
// the matched files, stage the archive out via pegasus-transfer, repeat
// forever.
func runWorker(notify <-chan struct{}, patterns []string, log *logger) {
	for range notify {
		log.Debugf("got notification, starting work")

		matched, err := checkpoint.MatchedFilenames(".", patterns)
		if err != nil {
			log.Errorf("unable to scan for checkpoint files: %v", err)
			continue
		}
		log.Infof("given patterns matched the following filenames: %v", matched)

		start := time.Now()
		if err := checkpoint.ArchiveAndCompress(matched); err != nil {
			log.Errorf("unable to create checkpoint archive: %v", err)
			continue
		}
		info, err := os.Stat(checkpoint.CheckpointFilename)
		if err == nil {
			log.Infof("created %.6f MB checkpoint file: %s", float64(info.Size())/(1<<20), checkpoint.CheckpointFilename)
		}
		log.Infof("archive and gzip took %v", time.Since(start))

		// pegasus-transfer is a compiled Go binary shipped alongside this
		// tool in the worker package's bin/, resolved via PATH exactly like
		// every other worker-side tool callout. As with the Python
		// original, the callout's own errors are logged but not otherwise
		// handled (fire-and-forget) — this preserves that pre-existing
		// character rather than scope-creeping into new failure handling.
		path, lookErr := exec.LookPath("pegasus-transfer")
		if lookErr != nil {
			log.Errorf("pegasus-transfer not found on PATH; unable to stage checkpoint")
		} else {
			cmd := exec.Command(path, "-m", "3", "-n", "8", "-f", checkpoint.TransferURLFile, "-s")
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if runErr := cmd.Run(); runErr != nil {
				log.Warnf("pegasus-transfer exited with error while staging checkpoint: %v", runErr)
			}
		}

		log.Debugf("work done, cleared notification")
	}
}
