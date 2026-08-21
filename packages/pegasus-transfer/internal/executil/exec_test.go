package executil

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestRun_Success(t *testing.T) {
	res, err := Run(context.Background(), []string{"echo", "hello world"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Output != "hello world" {
		t.Errorf("got %q", res.Output)
	}
}

func TestRun_NonZeroExit(t *testing.T) {
	_, err := Run(context.Background(), []string{"sh", "-c", "exit 3"}, Options{})
	if err == nil || !strings.Contains(err.Error(), "non-zero exit code (3)") {
		t.Fatalf("got err=%v", err)
	}
}

func TestRun_Timeout(t *testing.T) {
	_, err := Run(context.Background(), []string{"sleep", "5"}, Options{Timeout: 50 * time.Millisecond})
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("got err=%v", err)
	}
}

func TestRun_EnvOverride(t *testing.T) {
	res, err := Run(context.Background(), []string{"sh", "-c", "echo $FOO"}, Options{
		EnvOverrides: map[string]string{"FOO": "bar"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Output != "bar" {
		t.Errorf("got %q", res.Output)
	}
}

func TestRun_ArgvNotShellInterpreted(t *testing.T) {
	// A single-quote in a filename must not break out of quoting the way it
	// would with transfer.py's `'%s'` shell string interpolation.
	res, err := Run(context.Background(), []string{"echo", "it's a file"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Output != "it's a file" {
		t.Errorf("got %q", res.Output)
	}
}
