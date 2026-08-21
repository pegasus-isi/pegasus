package handler

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/executil"
)

// findTool searches $PATH (and, for "pegasus-*" tools, $PEGASUS_HOME/bin
// first) for an executable, matching worker_utils.Tools.find's lookup order
// closely enough for callout dispatch — this rewrite doesn't need the
// version-string caching the Python Tools singleton did.
func findTool(name string) (string, bool) {
	var dirs []string
	if strings.HasPrefix(name, "pegasus-") {
		if home, ok := os.LookupEnv("PEGASUS_HOME"); ok {
			dirs = append(dirs, filepath.Join(home, "bin"))
		}
	}
	if path, ok := os.LookupEnv("PATH"); ok {
		dirs = append(dirs, strings.Split(path, ":")...)
	}
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		full := filepath.Join(dir, name)
		if info, err := os.Stat(full); err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
			return full, true
		}
	}
	return "", false
}

// runCallout is a thin wrapper around executil.Run used by every
// callout-based handler, so failures are logged uniformly.
func (h Hooks) runCallout(ctx context.Context, argv []string, env map[string]string) (*executil.Result, error) {
	return executil.Run(ctx, argv, executil.Options{EnvOverrides: env})
}

// runCalloutStdin is runCallout with data piped to the command's stdin
// (needed by irods' `cat pw | iinit` login flow).
func (h Hooks) runCalloutStdin(ctx context.Context, argv []string, env map[string]string, stdin string) (*executil.Result, error) {
	return executil.Run(ctx, argv, executil.Options{EnvOverrides: env, Stdin: strings.NewReader(stdin)})
}
