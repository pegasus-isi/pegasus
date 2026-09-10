// Command pegasus-transfer is a strict drop-in replacement for the Python
// pegasus-transfer tool: same CLI flags, same JSON input contract (stdin or
// -f file), same exit codes and retry semantics. See the package-level docs
// under internal/ for what changed under the hood (native file/symlink/
// moveto/http(s)/webdav/S3, merged-in S3 support, dropped v1 input format,
// dropped Panorama/error-rate-injection).
package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/creds"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/engine"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/handler"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/integrity"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/stats"
)

func main() {
	os.Exit(run())
}

func run() int {
	var (
		file        = flag.String("f", "", "File containing URL pairs to be transferred. If not given, list is read from stdin.")
		maxAttempts = flag.Int("m", 3, "Number of attempts allowed for each transfer.")
		threads     = flag.Int("n", 0, "Number of threads to process transfers. Default is 8, or $PEGASUS_TRANSFER_THREADS.")
		symlink     = flag.Bool("s", false, "Allow symlinking of file URLs instead of copying.")
		debug       = flag.Bool("d", false, "Enables debugging output.")
	)
	// Long-flag aliases, matching optparse's --file/--max-attempts/etc.
	flag.StringVar(file, "file", "", "")
	flag.IntVar(maxAttempts, "max-attempts", 3, "")
	flag.IntVar(threads, "threads", 0, "")
	flag.BoolVar(symlink, "symlink", false, "")
	flag.BoolVar(debug, "debug", false, "")
	flag.Parse()

	log := setupLogger(*debug)

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT)
	go func() {
		if _, ok := <-sigCh; ok {
			log.Warn("Exiting due to signal")
			cancel()
		}
	}()

	success, err := pegasusTransfer(ctx, log, *maxAttempts, resolveThreads(*threads), *file, *symlink)
	if err != nil {
		log.Error(err.Error())
		return 1
	}
	if !success {
		return 1
	}
	return 0
}

func resolveThreads(cliValue int) int {
	if cliValue != 0 {
		return cliValue
	}
	if v, ok := os.LookupEnv("PEGASUS_TRANSFER_THREADS"); ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 8
}

func setupLogger(debug bool) *slog.Logger {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	return slog.New(newPlainHandler(os.Stderr, level))
}

// pegasusTransfer is the library-style entry point, matching transfer.py's
// pegasus_transfer(): read input, build the handler registry, run the
// engine, flush stats.
func pegasusTransfer(ctx context.Context, log *slog.Logger, maxAttempts, numThreads int, file string, symlink bool) (bool, error) {
	data, err := readInput(file)
	if err != nil {
		return false, err
	}

	entries, err := model.ParseJSON(data)
	if err != nil {
		return false, err
	}

	credentials, err := creds.LoadCredentials()
	if err != nil {
		return false, err
	}

	statsCollector := stats.NewCollector()
	hooks := handler.Hooks{
		Integrity: integrity.NewGenerator(),
		Stats:     statsCollector,
		Log:       log,
	}

	registry := buildRegistry(hooks, credentials, symlink)

	ok := engine.Run(ctx, entries, engine.Config{
		MaxAttempts: maxAttempts,
		NumThreads:  numThreads,
		Registry:    registry,
		Log:         log,
	})

	if err := statsCollector.Flush(); err != nil {
		log.Error("failed to flush stats", "error", err)
	}

	return ok, nil
}

func readInput(file string) ([]byte, error) {
	if file == "" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(file)
}

// buildRegistry wires up every protocol handler in transfer.py's
// registration-order priority (transfer.py:4249-4264: FileHandler first,
// then the remaining callout handlers, SymlinkHandler/MovetoHandler last
// among the file-adjacent ones). First match wins, matching
// TransferHandlerBase.protocol_check dispatch.
func buildRegistry(hooks handler.Hooks, credentials *creds.INI, symlinkShortcut bool) *handler.Registry {
	return handler.NewRegistry(
		handler.NewFileHandler(hooks, symlinkShortcut),
		handler.NewGridFtpHandler(hooks),
		handler.NewHTTPHandler(hooks, credentials),
		handler.NewFTPHandler(hooks),
		handler.NewHPSSHandler(hooks),
		handler.NewIRodsHandler(hooks),
		handler.NewS3Handler(hooks),
		handler.NewGlobusOnlineHandler(hooks),
		handler.NewGSHandler(hooks),
		handler.NewGFALHandler(hooks),
		handler.NewScpHandler(hooks),
		handler.NewGSIScpHandler(hooks),
		handler.NewOSDFHandler(hooks),
		handler.NewSymlinkHandler(hooks),
		handler.NewMovetoHandler(hooks),
		handler.NewDockerHandler(hooks),
		handler.NewSingularityHandler(hooks),
		handler.NewWebdavHandler(hooks, credentials),
	)
}
