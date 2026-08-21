package handler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// DockerHandler pulls a Docker image and saves it as a tarball via
// `docker pull && docker save`, mirroring transfer.py's DockerHandler.
type DockerHandler struct {
	Base
	Hooks
}

func NewDockerHandler(hooks Hooks) *DockerHandler {
	return &DockerHandler{
		Base:  Base{HandlerName: "DockerHandler", ProtocolMap: []string{"docker->file", "docker->file::docker"}},
		Hooks: hooks,
	}
}

func (h *DockerHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	docker, ok := findTool("docker")
	if !ok {
		h.logger().Error("unable to pull Docker images: docker not found")
		return Result{Failed: entriesOfTransfers(transfers)}
	}

	var res Result
	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()

		image := strings.TrimPrefix(t.SrcURL(), "docker://")
		image = strings.TrimLeft(image, "/")

		if err := PrepareLocalDir(filepath.Dir(t.DstPath())); err != nil {
			h.logger().Error("prepare local dir failed", "error", err)
		}

		if _, err := h.runCallout(ctx, []string{docker, "pull", image}, nil); err != nil {
			h.logger().Error("docker pull failed", "image", image, "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}
		if _, err := h.runCallout(ctx, []string{docker, "save", "-o", t.DstPath(), image}, nil); err != nil {
			h.logger().Error("docker save failed", "image", image, "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}
		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

// SingularityHandler pulls Docker/Singularity Hub/Library images via
// `apptainer pull` (preferred) or `singularity pull`, mirroring
// transfer.py's SingularityHandler.
type SingularityHandler struct {
	Base
	Hooks
}

func NewSingularityHandler(hooks Hooks) *SingularityHandler {
	return &SingularityHandler{
		Base: Base{
			HandlerName: "SingularityHandler",
			ProtocolMap: []string{
				"shub->file", "shub->file::singularity",
				"library->file", "library->file::singularity",
				"docker->file::singularity",
			},
		},
		Hooks: hooks,
	}
}

func (h *SingularityHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	exe, ok := findTool("apptainer")
	if !ok {
		exe, ok = findTool("singularity")
	}
	if !ok {
		h.logger().Error("unable to pull Singularity images: apptainer/singularity not found")
		return Result{Failed: entriesOfTransfers(transfers)}
	}

	var res Result
	for _, t := range transfers {
		h.PreTransferAttempt(t)
		start := time.Now()

		if err := PrepareLocalDir(filepath.Dir(t.DstPath())); err != nil {
			h.logger().Error("prepare local dir failed", "error", err)
		}

		// singularity pull only accepts a bare filename, not a full path,
		// so pull under a hashed temp name then move it into place.
		sum := sha256.Sum224([]byte(t.DstPath()))
		targetName := hex.EncodeToString(sum[:])

		if _, err := h.runCallout(ctx, []string{exe, "pull", "--allow-unauthenticated", targetName, t.SrcURL()}, nil); err != nil {
			h.logger().Error("singularity pull failed", "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}
		if err := movePulledImage(targetName, t.DstPath()); err != nil {
			h.logger().Error("moving pulled image failed", "error", err)
			h.PostTransferAttempt(t, false, start)
			res.Failed = append(res.Failed, t)
			continue
		}
		h.PostTransferAttempt(t, true, start)
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

// movePulledImage mirrors `mv <targetName>* dst`: singularity pull may
// append a suffix (e.g. .sif) to the requested filename.
func movePulledImage(targetName, dst string) error {
	matches, err := filepath.Glob(targetName + "*")
	if err != nil {
		return err
	}
	if len(matches) == 0 {
		return fmt.Errorf("no pulled image found matching %s*", targetName)
	}
	return renameOrCopy(matches[0], dst)
}
