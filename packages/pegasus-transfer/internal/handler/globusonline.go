package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/creds"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// GlobusOnlineHandler transfers go->go URLs by shelling out to
// pegasus-globus-online, exactly as transfer.py's GlobusOnlineHandler did —
// per the project's decision record, Globus Online integration was
// deliberately kept as a callout to the unchanged Python tool rather than
// internalized (unlike file/http(s)/webdav/S3). The JSON spec file format
// and CLI flags below must stay byte-for-byte compatible with that tool.
type GlobusOnlineHandler struct {
	Base
	Hooks
}

func NewGlobusOnlineHandler(hooks Hooks) *GlobusOnlineHandler {
	return &GlobusOnlineHandler{
		Base: Base{
			HandlerName:           "GlobusOnlineHandler",
			ProtocolMap:           []string{"go->go"},
			MkdirCleanupProtocols: []string{"go"},
		},
		Hooks: hooks,
	}
}

type goOAuthCreds struct {
	ClientID      string `json:"client_id"`
	TransferAT    string `json:"transfer_at"`
	TransferRT    string `json:"transfer_rt"`
	TransferATExp int64  `json:"transfer_at_exp"`
}

// loadGlobusCreds mirrors GlobusOnlineHandler._creds: parse
// ~/.pegasus/globus.conf's [oauth] section.
func loadGlobusCreds() (goOAuthCreds, error) {
	home, _ := os.UserHomeDir()
	path := home + "/.pegasus/globus.conf"
	data, err := os.ReadFile(path)
	if err != nil {
		return goOAuthCreds{}, fmt.Errorf("unable to locate globus config file %s", path)
	}
	ini, err := creds.ParseINI(data)
	if err != nil {
		return goOAuthCreds{}, err
	}
	clientID, ok := ini.Get("oauth", "client_id")
	if !ok {
		return goOAuthCreds{}, fmt.Errorf("no client_id was supplied for Globus App")
	}
	c := goOAuthCreds{ClientID: clientID}
	c.TransferAT, _ = ini.Get("oauth", "transfer_at")
	c.TransferRT, _ = ini.Get("oauth", "transfer_rt")
	if exp, ok := ini.Get("oauth", "transfer_at_exp"); ok {
		fmt.Sscanf(exp, "%d", &c.TransferATExp)
	}
	if c.TransferRT == "" {
		if c.TransferAT == "" || c.TransferATExp < time.Now().Unix()-3600 {
			return goOAuthCreds{}, fmt.Errorf("Globus transfer_access_token is missing or expiring soon")
		}
	}
	return c, nil
}

func (h *GlobusOnlineHandler) runSpec(ctx context.Context, mode string, spec any) error {
	path, ok := findTool("pegasus-globus-online")
	if !ok {
		return fmt.Errorf("unable to locate pegasus-globus-online in $PATH")
	}
	if _, err := loadGlobusCreds(); err != nil {
		return err
	}

	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp("", "pegasus-transfer-*.json")
	if err != nil {
		return fmt.Errorf("unable to create tmp file for pegasus-globus-online: %w", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	tmp.Close()

	_, err = h.runCallout(ctx, []string{path, "--" + mode, "--file", tmp.Name()}, nil)
	return err
}

func (h *GlobusOnlineHandler) DoMkdirs(ctx context.Context, mkdirs []*model.Mkdir) Result {
	cred, err := loadGlobusCreds()
	if err != nil {
		h.logger().Error("globus online mkdir failed", "error", err)
		return Result{Failed: entriesOf(mkdirs)}
	}
	files := make([]string, len(mkdirs))
	for i, m := range mkdirs {
		files[i] = m.Path()
	}
	spec := map[string]any{
		"endpoint": mkdirs[0].Host(), "client_id": cred.ClientID,
		"transfer_at": cred.TransferAT, "transfer_rt": cred.TransferRT,
		"transfer_at_exp": cred.TransferATExp, "files": files,
	}
	if err := h.runSpec(ctx, "mkdir", spec); err != nil {
		h.logger().Error("globus online mkdir failed", "error", err)
		return Result{Failed: entriesOf(mkdirs)}
	}
	return Result{Succeeded: entriesOf(mkdirs)}
}

func (h *GlobusOnlineHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	for _, t := range transfers {
		h.PreTransferAttempt(t)
	}
	start := time.Now()

	cred, err := loadGlobusCreds()
	if err != nil {
		h.logger().Error("globus online transfer failed", "error", err)
		return Result{Failed: entriesOfTransfers(transfers)}
	}
	files := make([]map[string]string, len(transfers))
	for i, t := range transfers {
		files[i] = map[string]string{"src": t.SrcPath(), "dst": t.DstPath()}
	}
	spec := map[string]any{
		"src_endpoint": transfers[0].SrcHost(), "dst_endpoint": transfers[0].DstHost(),
		"client_id": cred.ClientID, "transfer_at": cred.TransferAT, "transfer_rt": cred.TransferRT,
		"transfer_at_exp": cred.TransferATExp, "files": files,
	}
	if err := h.runSpec(ctx, "transfer", spec); err != nil {
		h.logger().Error("globus online transfer failed", "error", err)
		for _, t := range transfers {
			h.PostTransferAttempt(t, false, start)
		}
		return Result{Failed: entriesOfTransfers(transfers)}
	}
	for _, t := range transfers {
		h.PostTransferAttempt(t, true, start)
	}
	return Result{Succeeded: entriesOfTransfers(transfers)}
}

func (h *GlobusOnlineHandler) DoRemoves(ctx context.Context, removes []*model.Remove) Result {
	cred, err := loadGlobusCreds()
	if err != nil {
		h.logger().Error("globus online remove failed", "error", err)
		return Result{Failed: entriesOf(removes)}
	}
	files := make([]string, len(removes))
	for i, r := range removes {
		files[i] = r.Path()
	}
	spec := map[string]any{
		"endpoint": removes[0].Host(), "client_id": cred.ClientID,
		"transfer_at": cred.TransferAT, "transfer_rt": cred.TransferRT,
		"transfer_at_exp": cred.TransferATExp, "recursive": removes[0].Recursive, "files": files,
	}
	if err := h.runSpec(ctx, "remove", spec); err != nil {
		h.logger().Error("globus online remove failed", "error", err)
		return Result{Failed: entriesOf(removes)}
	}
	return Result{Succeeded: entriesOf(removes)}
}
