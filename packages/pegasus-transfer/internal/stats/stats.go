// Package stats accumulates per-transfer statistics matching transfer.py's
// Stats class: one YAML record per attempted transfer, optionally flushed to
// $PEGASUS_MULTIPART_DIR for the monitoring daemon to pick up.
//
// The real-time Panorama POST (KICKSTART_MON_ENDPOINT_URL) and the
// PEGASUS_TRANSFER_ERROR_RATE fault injector that lived alongside this in
// transfer.py were both dropped in the Go rewrite (see the project's
// decision record) and are not reimplemented here.
package stats

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// Collector accumulates transfer outcomes for the run.
type Collector struct {
	mu            sync.Mutex
	totalCount    int
	totalBytes    int64
	sitePairCount map[string]int
	sitePairBytes map[string]int64
	yaml          strings.Builder
}

func NewCollector() *Collector {
	return &Collector{
		sitePairCount: map[string]int{},
		sitePairBytes: map[string]int64{},
	}
}

// Add records the outcome of one transfer attempt, matching Stats.add_stats.
func (c *Collector) Add(t *model.Transfer, success bool, start, end time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := t.SrcSiteLabel() + "->" + t.DstSiteLabel()
	c.sitePairCount[key]++
	c.totalCount++

	var localPath string
	if t.SrcProto() == "file" {
		localPath = t.SrcPath()
	} else if t.DstProto() == "file" {
		localPath = t.DstPath()
	}

	var size int64
	if localPath != "" {
		if info, err := os.Stat(localPath); err == nil {
			size = info.Size()
			c.totalBytes += size
			c.sitePairBytes[key] += size
		}
	}

	fmt.Fprintf(&c.yaml,
		"  - src_url: %q\n    src_label: %q\n    dst_url: %q\n    dst_label: %q\n    success: %s\n    start: %.0f\n    duration: %.1f\n",
		t.SrcURL(), t.SrcSiteLabel(), t.DstURL(), t.DstSiteLabel(),
		pythonBool(success), float64(start.Unix()), end.Sub(start).Seconds())
	if t.LFN != "" {
		fmt.Fprintf(&c.yaml, "    lfn: %q\n", t.LFN)
	}
	if size > 0 {
		fmt.Fprintf(&c.yaml, "    bytes: %d\n", size)
	}
}

func pythonBool(b bool) string {
	if b {
		return "True"
	}
	return "False"
}

// Flush writes the accumulated YAML to $PEGASUS_MULTIPART_DIR, matching
// Stats.stats_summary's multipart-dir side effect. A no-op if the env var is
// unset or nothing was recorded.
func (c *Collector) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	dir, ok := os.LookupEnv("PEGASUS_MULTIPART_DIR")
	if !ok || c.yaml.Len() == 0 {
		return nil
	}
	path := fmt.Sprintf("%s/%d-transfer", dir, time.Now().Unix())
	content := "- transfer_attempts:\n" + c.yaml.String()
	return os.WriteFile(path, []byte(content), 0o644)
}

// TotalCount and TotalBytes support a short human-readable summary log line.
func (c *Collector) TotalCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.totalCount
}

func (c *Collector) TotalBytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.totalBytes
}
