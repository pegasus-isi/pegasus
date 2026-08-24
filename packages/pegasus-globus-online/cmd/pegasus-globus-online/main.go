// Command pegasus-globus-online is a tool used by pegasus-transfer to do
// transfers between Globus Online endpoints. Go port of
// pegasus-globus-online.py, rebuilt against the Globus Transfer REST API
// (via internal/globusapi) instead of the globus-sdk Python package —
// see packages/pegasus-globus-online/CLAUDE.md for the decision record.
//
// The JSON request-spec contract (--mkdir/--transfer/--remove, --file) is
// unchanged: it must stay byte-for-byte compatible with what
// packages/pegasus-transfer/internal/handler/globusonline.go writes.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-globus-online/internal/globusapi"
)

// logger matches pegasus-globus-online.py's log Formatter:
// "%(asctime)s %(levelname)7s:  %(message)s".
type logger struct{ debug bool }

func (l logger) log(level, format string, a ...any) {
	fmt.Printf("%s %7s:  %s\n", time.Now().Format("2006-01-02 15:04:05,000"), level, fmt.Sprintf(format, a...))
}
func (l logger) Debugf(format string, a ...any) {
	if l.debug {
		l.log("DEBUG", format, a...)
	}
}
func (l logger) Infof(format string, a ...any)     { l.log("INFO", format, a...) }
func (l logger) Warnf(format string, a ...any)     { l.log("WARNING", format, a...) }
func (l logger) Errorf(format string, a ...any)    { l.log("ERROR", format, a...) }
func (l logger) Criticalf(format string, a ...any) { l.log("CRITICAL", format, a...) }

// rawRequest is the JSON spec file's shape. "files" is left raw because its
// element type differs by mode: a bare path string for mkdir/remove, a
// {"src","dst"} pair for transfer.
type rawRequest struct {
	Endpoint      string          `json:"endpoint"`
	SrcEndpoint   string          `json:"src_endpoint"`
	DstEndpoint   string          `json:"dst_endpoint"`
	ClientID      string          `json:"client_id"`
	TransferAT    string          `json:"transfer_at"`
	TransferRT    string          `json:"transfer_rt"`
	TransferATExp int64           `json:"transfer_at_exp"`
	Recursive     bool            `json:"recursive"`
	Files         json.RawMessage `json:"files"`
}

type transferFileSpec struct {
	Src string `json:"src"`
	Dst string `json:"dst"`
}

// globusClient is the subset of *globusapi.TransferClient the mkdir/
// transfer/remove operations need. Defined as an interface purely for
// testability: it lets tests exercise runMkdir/runTransfer/runRemove and
// findMkdirBase against a fake, in-process implementation instead of an
// HTTP server (the real REST interactions are already covered by
// internal/globusapi's own httptest-based tests).
type globusClient interface {
	OperationLs(ctx context.Context, endpointID, path string, limit int) error
	OperationMkdir(ctx context.Context, endpointID, path string) error
	SubmitTransfer(ctx context.Context, srcEndpoint, dstEndpoint, label string, files []globusapi.TransferFilePair) (string, error)
	SubmitDelete(ctx context.Context, endpoint, label string, recursive bool, files []string) (string, error)
	CancelTask(ctx context.Context, taskID string) error
	WaitForTask(ctx context.Context, taskID string, logf func(format string, args ...any)) error
}

// activeTask tracks the in-flight task (if any) so the SIGINT/SIGTERM
// handler can cancel it, matching the Python original's module-global
// client/task_id used by prog_sigint_handler.
var activeTask struct {
	mu     sync.Mutex
	client globusClient
	taskID string
}

func setActiveTask(c globusClient, taskID string) {
	activeTask.mu.Lock()
	defer activeTask.mu.Unlock()
	activeTask.client, activeTask.taskID = c, taskID
}

func cancelActiveTask() {
	activeTask.mu.Lock()
	c, taskID := activeTask.client, activeTask.taskID
	activeTask.mu.Unlock()
	if c != nil && taskID != "" {
		// Matches cancel_task()'s own swallow-all-errors behavior — this is
		// a best-effort cleanup on the way out (signal handler or an
		// already-failing operation), not a place to report a second error.
		_ = c.CancelTask(context.Background(), taskID)
	}
}

func main() {
	fs := flag.NewFlagSet("pegasus-globus-online", flag.ContinueOnError)
	var mkdirMode, transferMode, removeMode, debug bool
	var file string
	fs.BoolVar(&mkdirMode, "mkdir", false, "Select mkdir mode")
	fs.BoolVar(&transferMode, "transfer", false, "Select transfer mode")
	fs.BoolVar(&removeMode, "remove", false, "Select remove mode")
	fs.StringVar(&file, "file", "", "File containing GO URL pairs to be transferred")
	fs.BoolVar(&debug, "d", false, "Enables debugging output")
	fs.BoolVar(&debug, "debug", false, "Enables debugging output")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}

	log := logger{debug: debug}

	if file == "" {
		log.Criticalf("An input file has to be given with --file")
		os.Exit(1)
	}

	data, err := os.ReadFile(file)
	if err != nil {
		log.Criticalf("%s", err)
		os.Exit(1)
	}
	var raw rawRequest
	if err := json.Unmarshal(data, &raw); err != nil {
		log.Criticalf("%s", err)
		os.Exit(1)
	}

	// Die nicely when asked to (Ctrl+C, system shutdown), matching
	// prog_sigint_handler's registration for both SIGINT and SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Warnf("Exiting due to signal %v", sig)
		cancelActiveTask()
		os.Exit(1)
	}()

	ctx := context.Background()

	switch {
	case mkdirMode:
		runMkdir(ctx, raw, log)
	case transferMode:
		runTransfer(ctx, raw, log)
	case removeMode:
		runRemove(ctx, raw, log)
	default:
		log.Criticalf("Please specify one of: --mkdir, --transfer, --remove")
		os.Exit(1)
	}
}

func newClient(ctx context.Context, raw rawRequest) globusClient {
	var expiresAt time.Time
	if raw.TransferATExp > 0 {
		expiresAt = time.Unix(raw.TransferATExp, 0)
	}
	ts := globusapi.TokenSource(ctx, raw.ClientID, raw.TransferAT, raw.TransferRT, expiresAt)
	return globusapi.NewTransferClient(ctx, ts)
}

// transferLabel matches the `if "PEGASUS_WF_UUID" in os.environ and
// "PEGASUS_DAG_JOB_ID" in os.environ` label convention shared by
// transfer()/remove().
func transferLabel() string {
	wf, wfOK := os.LookupEnv("PEGASUS_WF_UUID")
	job, jobOK := os.LookupEnv("PEGASUS_DAG_JOB_ID")
	if wfOK && jobOK {
		return wf + " - " + job
	}
	return ""
}

// posixSplit matches Python's os.path.split(): splits on the last '/',
// stripping trailing slashes from head unless head is composed entirely of
// slashes (so split("/a/b") -> ("/a","b"), split("/") -> ("/","")).
func posixSplit(p string) (head, tail string) {
	i := strings.LastIndex(p, "/") + 1
	head, tail = p[:i], p[i:]
	if head != "" && strings.Trim(head, "/") != "" {
		head = strings.TrimRight(head, "/")
	}
	return head, tail
}

// findMkdirBase walks f's ancestors looking for the deepest existing
// directory via operation_ls, matching mkdir()'s inline base_path-finding
// loop exactly (including its check-before-continuing-on-ls-failure
// semantics). Returns the existing base path and the child directory
// components (deepest first, then reversed to creation order) that still
// need to be created under it.
func findMkdirBase(ctx context.Context, client globusClient, endpoint, f string, log logger) (basePath string, childDirs []string) {
	basePath = f
	found := false
	for !found && basePath != "/" {
		found = true
		var dirName string
		basePath, dirName = posixSplit(basePath)
		if dirName != "" && dirName != "/" {
			childDirs = append(childDirs, dirName)
		}
		if err := client.OperationLs(ctx, endpoint, basePath, 2); err != nil {
			log.Warnf("Finding existing parent dir for mkdir %s", f)
			log.Warnf("%s", err)
			found = false
		}
	}
	for i, j := 0, len(childDirs)-1; i < j; i, j = i+1, j-1 {
		childDirs[i], childDirs[j] = childDirs[j], childDirs[i]
	}
	return basePath, childDirs
}

// runMkdir matches mkdir(): operation_mkdir doesn't support recursive
// directory creation, so each target path is walked back to its deepest
// existing ancestor and the missing components are created one at a time.
func runMkdir(ctx context.Context, raw rawRequest, log logger) {
	runMkdirWithClient(ctx, newClient(ctx, raw), raw, log)
}

// runMkdirWithClient is runMkdir's testable core, taking an already-built
// globusClient so tests can supply a fake instead of hitting a real (or
// httptest-fake) HTTP endpoint at this layer.
func runMkdirWithClient(ctx context.Context, client globusClient, raw rawRequest, log logger) {
	var files []string
	if err := json.Unmarshal(raw.Files, &files); err != nil {
		log.Criticalf("invalid files list: %s", err)
		os.Exit(1)
	}

	for _, f := range files {
		basePath, childDirs := findMkdirBase(ctx, client, raw.Endpoint, f, log)
		path := basePath
		for _, child := range childDirs {
			if strings.HasSuffix(path, "/") {
				path += child
			} else {
				path = path + "/" + child
			}
			if err := client.OperationMkdir(ctx, raw.Endpoint, path); err != nil {
				var apiErr *globusapi.APIError
				if errors.As(err, &apiErr) && apiErr.Code == "ExternalError.MkdirFailed.Exists" {
					log.Warnf("Directory already exists: %s", path)
					continue
				}
				log.Criticalf("%s", err)
				os.Exit(1)
			}
		}
	}
	log.Infof("Mkdir complete")
}

// runTransfer matches transfer(): submit a transfer task, then block until
// it completes, canceling and exiting nonzero on any error along the way.
func runTransfer(ctx context.Context, raw rawRequest, log logger) {
	runTransferWithClient(ctx, newClient(ctx, raw), raw, log)
}

// runTransferWithClient is runTransfer's testable core; see runMkdirWithClient.
func runTransferWithClient(ctx context.Context, client globusClient, raw rawRequest, log logger) {
	var files []transferFileSpec
	if err := json.Unmarshal(raw.Files, &files); err != nil {
		log.Criticalf("invalid files list: %s", err)
		os.Exit(1)
	}

	pairs := make([]globusapi.TransferFilePair, len(files))
	for i, f := range files {
		pairs[i] = globusapi.TransferFilePair{Src: f.Src, Dst: f.Dst}
	}

	taskID, err := client.SubmitTransfer(ctx, raw.SrcEndpoint, raw.DstEndpoint, transferLabel(), pairs)
	if err != nil {
		log.Errorf("%s", err)
		os.Exit(1)
	}
	setActiveTask(client, taskID)

	if err := client.WaitForTask(ctx, taskID, log.Infof); err != nil {
		log.Errorf("%s", err)
		cancelActiveTask()
		os.Exit(1)
	}
	log.Infof("Transfer complete")
}

// runRemove matches remove(): submit a delete task, then block until it
// completes, canceling and exiting nonzero on any error along the way.
func runRemove(ctx context.Context, raw rawRequest, log logger) {
	runRemoveWithClient(ctx, newClient(ctx, raw), raw, log)
}

// runRemoveWithClient is runRemove's testable core; see runMkdirWithClient.
func runRemoveWithClient(ctx context.Context, client globusClient, raw rawRequest, log logger) {
	var files []string
	if err := json.Unmarshal(raw.Files, &files); err != nil {
		log.Criticalf("invalid files list: %s", err)
		os.Exit(1)
	}

	taskID, err := client.SubmitDelete(ctx, raw.Endpoint, transferLabel(), raw.Recursive, files)
	if err != nil {
		log.Errorf("%s", err)
		os.Exit(1)
	}
	setActiveTask(client, taskID)

	if err := client.WaitForTask(ctx, taskID, log.Infof); err != nil {
		log.Errorf("%s", err)
		cancelActiveTask()
		os.Exit(1)
	}
	log.Infof("Delete complete")
}
