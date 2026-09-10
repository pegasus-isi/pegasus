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
// storage` CLI, authenticating with a Google service account key referenced
// by GOOGLE_APPLICATION_CREDENTIALS (JSON key files, the replacement for the
// deprecated PKCS12/boto-based auth transfer.py's GSHandler used to use).
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

// gcloudEnv resolves GOOGLE_APPLICATION_CREDENTIALS[_site] (a Google service
// account JSON key file) and activates it as the active gcloud identity in
// an ephemeral, per-call gcloud config directory (via CLOUDSDK_CONFIG), so
// concurrent pegasus-transfer invocations never share - or race on - a
// single ~/.config/gcloud. The returned env also carries
// GOOGLE_APPLICATION_CREDENTIALS itself, since the `gcloud storage` client
// libraries fall back to it directly as Application Default Credentials.
func gcloudEnv(ctx context.Context, h *GSHandler, siteLabel string) (map[string]string, error) {
	keyFile, ok := creds.SiteEnv("GOOGLE_APPLICATION_CREDENTIALS", siteLabel)
	if !ok {
		return nil, fmt.Errorf(
			"at least one of GOOGLE_APPLICATION_CREDENTIALS_%s or GOOGLE_APPLICATION_CREDENTIALS must be set",
			siteLabel)
	}
	if _, err := creds.EnsureFSPermissions(keyFile); err != nil {
		return nil, err
	}

	configDir, err := os.MkdirTemp("", "pegasus-transfer-gcloud-*")
	if err != nil {
		return nil, fmt.Errorf("unable to create tmp gcloud config dir: %w", err)
	}

	env := map[string]string{
		"GOOGLE_APPLICATION_CREDENTIALS": keyFile,
		"CLOUDSDK_CONFIG":                configDir,
	}

	if _, err := h.runCallout(
		ctx,
		[]string{"gcloud", "auth", "activate-service-account", "--key-file=" + keyFile},
		env,
	); err != nil {
		os.RemoveAll(configDir)
		return nil, fmt.Errorf("gcloud auth activate-service-account failed: %w", err)
	}

	return env, nil
}

func cleanupGcloudConfig(env map[string]string) {
	if path, ok := env["CLOUDSDK_CONFIG"]; ok {
		_ = os.RemoveAll(path)
	}
}

func (h *GSHandler) DoMkdirs(ctx context.Context, mkdirs []*model.Mkdir) Result {
	if len(mkdirs) == 0 {
		return Result{}
	}
	env, err := gcloudEnv(ctx, h, mkdirs[0].SiteLabel())
	if err != nil {
		h.logger().Error("gs mkdir: credential setup failed", "error", err)
		return Result{Failed: entriesOf(mkdirs)}
	}
	defer cleanupGcloudConfig(env)

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
	env, err := gcloudEnv(ctx, h, siteLabel)
	if err != nil {
		h.logger().Error("gs transfer: credential setup failed", "error", err)
		return Result{Failed: entriesOfTransfers(transfers)}
	}
	defer cleanupGcloudConfig(env)

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
	env, err := gcloudEnv(ctx, h, removes[0].SiteLabel())
	if err != nil {
		h.logger().Error("gs remove: credential setup failed", "error", err)
		return Result{Failed: entriesOf(removes)}
	}
	defer cleanupGcloudConfig(env)

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
