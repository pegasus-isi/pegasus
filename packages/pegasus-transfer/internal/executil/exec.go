// Package executil runs external tools for the callout-based protocols
// (gsiftp, scp, irods, htar, docker, ...) that still shell out rather than
// being implemented natively. Unlike transfer.py's TimedCommand, commands
// run as argv slices via exec.Command, never through a shell — quoting bugs
// in the old `'%s'` string interpolation cannot recur here.
package executil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// DefaultTimeout mirrors TimedCommand's 6 hour default.
const DefaultTimeout = 6 * time.Hour

// Options configures a Run call.
type Options struct {
	// EnvOverrides are merged on top of the current process environment.
	EnvOverrides map[string]string
	// Timeout defaults to DefaultTimeout when zero.
	Timeout time.Duration
	// Dir is the working directory; empty means inherit.
	Dir string
	// DisplayArgv, if set, is used only for the error message (so
	// credentials in the real argv are never echoed back).
	DisplayArgv []string
	// Stdin, if set, is piped to the command (e.g. irods' `cat pw | iinit`).
	Stdin io.Reader
}

// Result carries the outcome of a completed command.
type Result struct {
	Output   string
	ExitCode int
	Duration time.Duration
}

// Run executes argv[0] with argv[1:] as arguments, capturing combined
// stdout+stderr, matching TimedCommand's shape: a timeout kills the whole
// process group and returns an error, as does a non-zero exit code.
func Run(ctx context.Context, argv []string, opts Options) (*Result, error) {
	if len(argv) == 0 {
		return nil, fmt.Errorf("executil.Run: empty argv")
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	display := opts.DisplayArgv
	if display == nil {
		display = argv
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, argv[0], argv[1:]...)
	cmd.Dir = opts.Dir
	cmd.Stdin = opts.Stdin
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	env := os.Environ()
	for k, v := range opts.EnvOverrides {
		env = append(env, k+"="+v)
	}
	cmd.Env = env

	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf

	start := time.Now()
	err := cmd.Run()
	duration := time.Since(start)

	if runCtx.Err() == context.DeadlineExceeded {
		killProcessGroup(cmd)
		return nil, fmt.Errorf("command timed out after %d seconds: %v", int(timeout.Seconds()), display)
	}

	res := &Result{Output: trimOutput(buf.String()), Duration: duration}
	if exitErr, ok := err.(*exec.ExitError); ok {
		res.ExitCode = exitErr.ExitCode()
	} else if err != nil {
		return nil, fmt.Errorf("command failed to start: %v: %w", display, err)
	}

	if res.ExitCode != 0 {
		return res, fmt.Errorf("command exited with non-zero exit code (%d): %v", res.ExitCode, display)
	}
	return res, nil
}

func killProcessGroup(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err == nil {
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
	}
	_ = cmd.Process.Kill()
}

func trimOutput(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r' || s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	for len(s) > 0 && (s[0] == '\n' || s[0] == '\r' || s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	return s
}
