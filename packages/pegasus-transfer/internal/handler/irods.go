package handler

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/creds"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// IRodsHandler transfers file<->irods URLs via the icommands (iget, iput,
// imkdir, irm), mirroring transfer.py's IRodsHandler including its
// once-per-run iinit login flow.
type IRodsHandler struct {
	Base
	Hooks

	mu sync.Mutex
}

func NewIRodsHandler(hooks Hooks) *IRodsHandler {
	return &IRodsHandler{
		Base: Base{
			HandlerName:           "IRodsHandler",
			ProtocolMap:           []string{"file->irods", "irods->file"},
			MkdirCleanupProtocols: []string{"irods"},
		},
		Hooks: hooks,
	}
}

// irodsLogin mirrors IRodsHandler._irods_login: resolve the environment
// file, read the password/ticket out of it, and run `iinit` once (an
// existing .irodsA auth file means we're already logged in).
func (h *IRodsHandler) irodsLogin(ctx context.Context, siteLabel string) (map[string]string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	env := map[string]string{"IRODS_AUTHENTICATION_FILE": filepath.Join(cwd, ".irodsA")}

	envFile, ok := creds.SiteEnv("IRODS_ENVIRONMENT_FILE", siteLabel)
	if !ok {
		return nil, fmt.Errorf("missing IRODS_ENVIRONMENT_FILE - unable to do irods transfers")
	}
	env["IRODS_ENVIRONMENT_FILE"] = envFile

	if _, err := creds.EnsureFSPermissions(envFile); err != nil {
		return nil, err
	}

	password, ticket, err := readIrodsCreds(envFile)
	if err != nil {
		return nil, err
	}
	if password == "" {
		return nil, fmt.Errorf("no irodsPassword specified in irods env file")
	}
	if ticket != "" {
		env["IRODS_TICKET"] = ticket
	}

	if _, err := os.Stat(env["IRODS_AUTHENTICATION_FILE"]); err == nil {
		return env, nil // already logged in
	}

	if _, err := h.runCalloutStdin(ctx, []string{"iinit"}, env, password+"\n"); err != nil {
		return nil, err
	}
	if _, err := creds.EnsureFSPermissions(env["IRODS_AUTHENTICATION_FILE"]); err != nil {
		h.logger().Warn("irods auth file permission check failed", "error", err)
	}
	return env, nil
}

// readIrodsCreds parses "key: value"-shaped lines (irods 4.x JSON-like env
// files use the same "key": "value" shape), matching _irods_login's
// line.split(":", 2) parser exactly, including its quote/whitespace strip.
func readIrodsCreds(path string) (password, ticket string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, ":", 3)
		if len(parts) < 2 {
			continue
		}
		key := strings.ToLower(strings.Trim(parts[0], " \t'\"\r\n"))
		value := strings.Trim(parts[1], " \t'\",\r\n")
		switch key {
		case "irodspassword", "irods_password":
			password = value
		case "irodsticket", "irods_ticket":
			ticket = value
		}
	}
	return password, ticket, scanner.Err()
}

func (h *IRodsHandler) DoMkdirs(ctx context.Context, mkdirs []*model.Mkdir) Result {
	if _, ok := findTool("iget"); !ok {
		h.logger().Error("unable to do irods transfers: iget not found")
		return Result{Failed: entriesOf(mkdirs)}
	}
	env, err := h.irodsLogin(ctx, mkdirs[0].SiteLabel())
	if err != nil {
		h.logger().Error("irods login failed", "error", err)
		return Result{Failed: entriesOf(mkdirs)}
	}

	var res Result
	for _, m := range mkdirs {
		argv := []string{"imkdir"}
		if ticket, ok := env["IRODS_TICKET"]; ok {
			argv = append(argv, "-t", ticket)
		}
		argv = append(argv, "-p", m.Path())
		if _, err := h.runCallout(ctx, argv, env); err != nil {
			h.logger().Error("irods mkdir failed", "path", m.Path(), "error", err)
			res.Failed = append(res.Failed, m)
			continue
		}
		res.Succeeded = append(res.Succeeded, m)
	}
	return res
}

func (h *IRodsHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	if _, ok := findTool("iget"); !ok {
		h.logger().Error("unable to do irods transfers: iget not found")
		return Result{Failed: entriesOfTransfers(transfers)}
	}

	var res Result
	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()

		siteLabel := t.SrcSiteLabel()
		if t.DstProto() == "irods" {
			siteLabel = t.DstSiteLabel()
		}
		env, err := h.irodsLogin(ctx, siteLabel)
		if err != nil {
			h.logger().Error("irods login failed", "error", err)
			return Result{Failed: entriesOfTransfers(transfers)}
		}

		if err := h.transferOne(ctx, t, env); err != nil {
			h.logger().Error("irods transfer failed", "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}
		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

func (h *IRodsHandler) transferOne(ctx context.Context, t *model.Transfer, env map[string]string) error {
	if t.DstProto() == "file" {
		if err := PrepareLocalDir(filepath.Dir(t.DstPath())); err != nil {
			return err
		}
		argv := []string{"iget", "-v", "-f", "-T", "-K", "-N", "4"}
		if t.SrcHost() != "" && t.Attempts <= 1 {
			argv = append(argv, "-R", t.SrcHost())
		}
		if ticket, ok := env["IRODS_TICKET"]; ok {
			argv = append(argv, "-t", ticket)
		}
		argv = append(argv, t.SrcPath(), t.DstPath())
		_, err := h.runCallout(ctx, argv, env)
		return err
	}

	if !VerifyLocalFile(t.SrcPath()) {
		return fmt.Errorf("source file does not exist or is not readable: %s", t.SrcPath())
	}
	// best-effort remote mkdir, matching the original's swallowed errors
	_, _ = h.runCallout(ctx, []string{"imkdir", "-p", filepath.Dir(t.DstPath())}, env)

	argv := []string{"iput", "-v", "-f", "-T", "-K", "-N", "4"}
	if t.DstHost() != "" && t.Attempts <= 1 {
		argv = append(argv, "-R", t.DstHost())
	}
	if ticket, ok := env["IRODS_TICKET"]; ok {
		argv = append(argv, "-t", ticket)
	}
	argv = append(argv, t.SrcPath(), t.DstPath())
	_, err := h.runCallout(ctx, argv, env)
	return err
}

func (h *IRodsHandler) DoRemoves(ctx context.Context, removes []*model.Remove) Result {
	if _, ok := findTool("iget"); !ok {
		h.logger().Error("unable to do irods transfers: iget not found")
		return Result{Failed: entriesOf(removes)}
	}
	env, err := h.irodsLogin(ctx, removes[0].SiteLabel())
	if err != nil {
		h.logger().Error("irods login failed", "error", err)
		return Result{Failed: entriesOf(removes)}
	}

	var res Result
	for _, r := range removes {
		argv := []string{"irm", "-f"}
		if r.Recursive {
			argv = append(argv, "-r")
		}
		if ticket, ok := env["IRODS_TICKET"]; ok {
			argv = append(argv, "-t", ticket)
		}
		argv = append(argv, r.Path())
		if result, err := h.runCallout(ctx, argv, env); err != nil {
			if result != nil && result.ExitCode == 3 {
				// matches transfer.py: exit code 3 means "does not exist",
				// treated as success.
				res.Succeeded = append(res.Succeeded, r)
				continue
			}
			h.logger().Error("irods remove failed", "path", r.Path(), "error", err)
			res.Failed = append(res.Failed, r)
			continue
		}
		res.Succeeded = append(res.Succeeded, r)
	}
	return res
}
