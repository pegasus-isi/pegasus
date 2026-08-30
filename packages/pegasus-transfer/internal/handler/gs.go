package handler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/creds"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// GSHandler transfers file<->gs (Google Cloud Storage) via the `gcloud
// storage` CLI, mirroring transfer.py's GSHandler.
type GSHandler struct {
	Base
	Hooks
}

func NewGSHandler(hooks Hooks) *GSHandler {
	return &GSHandler{
		Base: Base{
			HandlerName:           "GSHandler",
			ProtocolMap:           []string{"file->gs", "gs->file", "gs->gs"},
			MkdirCleanupProtocols: []string{"gs"},
		},
		Hooks: hooks,
	}
}

var reGSBucket = regexp.MustCompile(`(gs://[\w-]+/)[/\w-]*`)

// gcloudEnv mirrors GSHandler._gcloud_env: resolve BOTO_CONFIG[_site] and
// GOOGLE_PKCS12[_site], then rewrite a copy of the boto config so its
// Credentials.gs_service_key_file points at the resolved PKCS12 key.
func gcloudEnv(siteLabel string) (map[string]string, error) {
	boto, ok := creds.SiteEnv("BOTO_CONFIG", siteLabel)
	if !ok {
		return nil, fmt.Errorf("at least one of BOTO_CONFIG_%s or BOTO_CONFIG must be set", siteLabel)
	}
	pkcs12, ok := creds.SiteEnv("GOOGLE_PKCS12", siteLabel)
	if !ok {
		return nil, fmt.Errorf("at least one of GOOGLE_PKCS12_%s or GOOGLE_PKCS12 must be set", siteLabel)
	}
	if _, err := creds.EnsureFSPermissions(boto); err != nil {
		return nil, err
	}
	if _, err := creds.EnsureFSPermissions(pkcs12); err != nil {
		return nil, err
	}

	data, err := os.ReadFile(boto)
	if err != nil {
		return nil, err
	}
	ini, err := creds.ParseINI(data)
	if err != nil {
		return nil, err
	}
	ini.Set("Credentials", "gs_service_key_file", pkcs12)

	tmp, err := os.CreateTemp("", "pegasus-transfer-*.boto")
	if err != nil {
		return nil, fmt.Errorf("unable to create tmp file for gs boto config: %w", err)
	}
	if _, err := tmp.WriteString(ini.Dump()); err != nil {
		tmp.Close()
		return nil, err
	}
	tmp.Close()
	if err := os.Chmod(tmp.Name(), 0o600); err != nil {
		return nil, err
	}

	return map[string]string{
		"PYTHONPATH":    "",
		"BOTO_CONFIG":   tmp.Name(),
		"GOOGLE_PKCS12": pkcs12,
	}, nil
}

func (h *GSHandler) DoMkdirs(ctx context.Context, mkdirs []*model.Mkdir) Result {
	if len(mkdirs) == 0 {
		return Result{}
	}
	env, err := gcloudEnv(mkdirs[0].SiteLabel())
	if err != nil {
		h.logger().Error("gs mkdir: credential setup failed", "error", err)
		return Result{Failed: entriesOf(mkdirs)}
	}
	defer cleanupBotoConfig(env)

	var res Result
	for _, m := range mkdirs {
		bucket := m.URL()
		if bm := reGSBucket.FindStringSubmatch(m.URL()); bm != nil {
			bucket = bm[1]
		}
		result, err := h.runCallout(ctx, []string{"gcloud", "storage", "buckets", "create", bucket}, env)
		if err != nil && (result == nil || !strings.Contains(result.Output, "previous request to create the named bucket succeeded")) {
			h.logger().Error("gs mkdir failed", "bucket", bucket, "error", err)
			res.Failed = append(res.Failed, m)
			continue
		}
		res.Succeeded = append(res.Succeeded, m)
	}
	return res
}

func (h *GSHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	if len(transfers) == 0 {
		return Result{}
	}
	siteLabel := transfers[0].DstSiteLabel()
	if transfers[0].SrcProto() == "gs" {
		siteLabel = transfers[0].SrcSiteLabel()
	}
	env, err := gcloudEnv(siteLabel)
	if err != nil {
		h.logger().Error("gs transfer: credential setup failed", "error", err)
		return Result{Failed: entriesOfTransfers(transfers)}
	}
	defer cleanupBotoConfig(env)

	var res Result
	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()

		var src, dst string
		switch {
		case t.SrcProto() == "gs" && t.DstProto() == "gs":
			src, dst = t.SrcURL(), t.DstURL()
		case t.DstProto() == "file":
			if err := PrepareLocalDir(filepath.Dir(t.DstPath())); err != nil {
				h.logger().Error("prepare local dir failed", "error", err)
			}
			src, dst = t.SrcURL(), t.DstPath()
		default:
			if !VerifyLocalFile(t.SrcPath()) {
				h.logger().Error("source file does not exist or is not readable", "src", t.SrcPath())
				h.PostTransferAttempt(t, false, start)
				res.Failed = append(res.Failed, t)
				continue
			}
			src, dst = t.SrcPath(), t.DstURL()
		}

		if _, err := h.runCallout(ctx, []string{"gcloud", "-q", "storage", "cp", src, dst}, env); err != nil {
			h.logger().Error("gs transfer failed", "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}
		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

func (h *GSHandler) DoRemoves(ctx context.Context, removes []*model.Remove) Result {
	if len(removes) == 0 {
		return Result{}
	}
	env, err := gcloudEnv(removes[0].SiteLabel())
	if err != nil {
		h.logger().Error("gs remove: credential setup failed", "error", err)
		return Result{Failed: entriesOf(removes)}
	}
	defer cleanupBotoConfig(env)

	var res Result
	for _, r := range removes {
		argv := []string{"gcloud", "storage", "rm"}
		if r.Recursive {
			argv = append(argv, "-r")
		}
		argv = append(argv, r.URL())
		result, err := h.runCallout(ctx, argv, env)
		if err != nil && (result == nil || !strings.Contains(result.Output, "following URLs matched no objects or files")) {
			h.logger().Error("gs remove failed", "url", r.URL(), "error", err)
			res.Failed = append(res.Failed, r)
			continue
		}
		res.Succeeded = append(res.Succeeded, r)
	}
	return res
}

func cleanupBotoConfig(env map[string]string) {
	if path, ok := env["BOTO_CONFIG"]; ok {
		_ = os.Remove(path)
	}
}
