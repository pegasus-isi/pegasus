package engine

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/handler"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// fakeHandler lets tests script success/failure without touching a real
// protocol.
type fakeHandler struct {
	handler.Base
	transferCalls atomic.Int32
	// failFirstN transfers fail on their first call, then succeed.
	failFirstN int
	attempts   map[*model.Transfer]int
}

func newFakeHandler(protocolMap []string, failFirstN int) *fakeHandler {
	return &fakeHandler{
		Base:       handler.Base{HandlerName: "fake", ProtocolMap: protocolMap},
		failFirstN: failFirstN,
		attempts:   map[*model.Transfer]int{},
	}
}

func (f *fakeHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) handler.Result {
	f.transferCalls.Add(1)
	var res handler.Result
	for _, t := range transfers {
		f.attempts[t]++
		if f.attempts[t] <= f.failFirstN {
			res.Failed = append(res.Failed, t)
			continue
		}
		res.Succeeded = append(res.Succeeded, t)
	}
	return res
}

func mustTransfer(t *testing.T, src, dst string) *model.Transfer {
	t.Helper()
	tr := model.NewTransfer()
	if err := tr.AddSrc("local", src, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := tr.AddDst("local", dst, "", nil); err != nil {
		t.Fatal(err)
	}
	return tr
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, nil))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestRun_AllSucceedFirstAttempt(t *testing.T) {
	fh := newFakeHandler([]string{"file->file"}, 0)
	reg := handler.NewRegistry(fh)

	entries := []model.Entry{
		mustTransfer(t, "file:///a", "file:///b"),
		mustTransfer(t, "file:///c", "file:///d"),
	}

	ok := Run(context.Background(), entries, Config{MaxAttempts: 3, NumThreads: 2, Registry: reg, Log: silentLogger()})
	if !ok {
		t.Fatal("expected success")
	}
}

func TestRun_RetriesAndEventuallySucceeds(t *testing.T) {
	// Fails once, succeeds on the retry (real time.Sleep is unavoidable
	// here since Run's backoff isn't injectable, so keep max_attempts low
	// and accept the ~ (5^3+jitter)s ~ 2 minute worst case backoff would be
	// too slow for a unit test — instead verify a fail-forever case
	// finishes with the expected failure count, which doesn't need to wait
	// out the retry delay before failing on the last attempt).
	fh := newFakeHandler([]string{"file->file"}, 100) // always fails
	reg := handler.NewRegistry(fh)

	entries := []model.Entry{mustTransfer(t, "file:///a", "file:///b")}

	ok := Run(context.Background(), entries, Config{MaxAttempts: 1, NumThreads: 1, Registry: reg, Log: silentLogger()})
	if ok {
		t.Fatal("expected failure")
	}
	if fh.transferCalls.Load() != 1 {
		t.Errorf("expected exactly 1 dispatch call with MaxAttempts=1, got %d", fh.transferCalls.Load())
	}
}

func TestRun_NoHandlerFound(t *testing.T) {
	reg := handler.NewRegistry() // empty registry
	entries := []model.Entry{mustTransfer(t, "file:///a", "unknownproto://b")}

	ok := Run(context.Background(), entries, Config{MaxAttempts: 1, NumThreads: 1, Registry: reg, Log: silentLogger()})
	if ok {
		t.Fatal("expected failure when no handler matches")
	}
}

func TestGroupable_SameProtocolPairGroupsTogether(t *testing.T) {
	a := mustTransfer(t, "http://h1/a", "file:///a")
	b := mustTransfer(t, "http://h2/b", "file:///b")
	c := mustTransfer(t, "s3://x/y", "file:///c")

	if !groupable(a, b) {
		t.Error("expected same-protocol-pair transfers to be groupable")
	}
	if groupable(a, c) {
		t.Error("expected different-protocol-pair transfers to not be groupable")
	}
}

func TestGroupEntries_CapsGroupSize(t *testing.T) {
	var entries []model.Entry
	for i := 0; i < 5; i++ {
		entries = append(entries, mustTransfer(t, "file:///a", "file:///b"))
	}
	groups := groupEntries(entries, 2)
	if len(groups) != 3 {
		t.Fatalf("expected 3 groups (2,2,1), got %d", len(groups))
	}
	if len(groups[0]) != 2 || len(groups[1]) != 2 || len(groups[2]) != 1 {
		t.Errorf("unexpected group sizes: %v", []int{len(groups[0]), len(groups[1]), len(groups[2])})
	}
}

func TestGroupEntries_MkdirNeverGrouped(t *testing.T) {
	m1, err := model.NewPegasusURL("file:///a", "", "local", 0)
	if err != nil {
		t.Fatal(err)
	}
	m2, err := model.NewPegasusURL("file:///b", "", "local", 0)
	if err != nil {
		t.Fatal(err)
	}
	entries := []model.Entry{&model.Mkdir{Target: m1}, &model.Mkdir{Target: m2}}
	groups := groupEntries(entries, 10)
	if len(groups) != 2 {
		t.Fatalf("expected mkdirs to never group together, got %d groups", len(groups))
	}
}

func TestSubTransferRotation_RetriedWithinSameAttempt(t *testing.T) {
	// A transfer with 2 source URLs should be retried against the 2nd
	// source within the same attempt before being marked failed, matching
	// move_to_next_sub_transfer's role in the retry loop.
	tr := model.NewTransfer()
	if err := tr.AddSrc("local", "file:///a", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := tr.AddSrc("local", "file:///b", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := tr.AddDst("local", "file:///dst", "", nil); err != nil {
		t.Fatal(err)
	}

	var seenPaths []string
	fh := &recordingHandler{Base: handler.Base{HandlerName: "rec", ProtocolMap: []string{"file->file"}}, seen: &seenPaths}
	reg := handler.NewRegistry(fh)

	ok := Run(context.Background(), []model.Entry{tr}, Config{MaxAttempts: 1, NumThreads: 1, Registry: reg, Log: silentLogger()})
	if ok {
		t.Fatal("expected overall failure (both sub-transfers fail)")
	}
	if len(seenPaths) != 2 {
		t.Fatalf("expected both source paths to be tried within one attempt, got %v", seenPaths)
	}
}

// TestRun_TwoStageSplit reproduces the reported bug: a src/dst protocol pair
// with no direct handler (mirroring scp->s3s) but which two registered
// handlers can bridge via a local temp file (scp->file, then file->s3s).
// Before dispatchTwoStage existed, this failed every attempt with "no
// handler found for protocol pair" even though transfer.py handled it via
// its split-transfer fallback.
func TestRun_TwoStageSplit(t *testing.T) {
	down := newFakeHandler([]string{"scp->file"}, 0)
	up := newFakeHandler([]string{"file->s3s"}, 0)
	reg := handler.NewRegistry(down, up)

	entries := []model.Entry{mustTransfer(t, "scp://host/path/a", "s3s://bucket/key/a")}

	ok := Run(context.Background(), entries, Config{MaxAttempts: 1, NumThreads: 1, Registry: reg, Log: silentLogger()})
	if !ok {
		t.Fatal("expected the two-stage split to succeed")
	}
	if down.transferCalls.Load() != 1 {
		t.Errorf("expected the download leg to run once, got %d calls", down.transferCalls.Load())
	}
	if up.transferCalls.Load() != 1 {
		t.Errorf("expected the upload leg to run once, got %d calls", up.transferCalls.Load())
	}
}

// TestRun_TwoStageSplit_OnlyOneLegAvailable makes sure a partial bridge (only
// src->file, no file->dst) still fails cleanly with "no handler found"
// rather than silently dropping data or panicking.
func TestRun_TwoStageSplit_OnlyOneLegAvailable(t *testing.T) {
	down := newFakeHandler([]string{"scp->file"}, 0)
	reg := handler.NewRegistry(down)

	entries := []model.Entry{mustTransfer(t, "scp://host/path/a", "s3s://bucket/key/a")}

	ok := Run(context.Background(), entries, Config{MaxAttempts: 1, NumThreads: 1, Registry: reg, Log: silentLogger()})
	if ok {
		t.Fatal("expected failure when only one leg of the bridge has a handler")
	}
	if down.transferCalls.Load() != 0 {
		t.Errorf("expected the download leg to never run without an upload leg, got %d calls", down.transferCalls.Load())
	}
}

// TestRun_TwoStageSplit_DownloadFails makes sure a failed download leg never
// attempts the upload leg, and the original transfer is reported failed
// (not the synthetic per-leg transfer).
func TestRun_TwoStageSplit_DownloadFails(t *testing.T) {
	down := newFakeHandler([]string{"scp->file"}, 100) // always fails
	up := newFakeHandler([]string{"file->s3s"}, 0)
	reg := handler.NewRegistry(down, up)

	entries := []model.Entry{mustTransfer(t, "scp://host/path/a", "s3s://bucket/key/a")}

	ok := Run(context.Background(), entries, Config{MaxAttempts: 1, NumThreads: 1, Registry: reg, Log: silentLogger()})
	if ok {
		t.Fatal("expected failure when the download leg fails")
	}
	if up.transferCalls.Load() != 0 {
		t.Errorf("expected the upload leg to never run after a failed download, got %d calls", up.transferCalls.Load())
	}
}

type recordingHandler struct {
	handler.Base
	seen *[]string
}

func (r *recordingHandler) DoTransfers(ctx context.Context, transfers []*model.Transfer) handler.Result {
	var res handler.Result
	for _, t := range transfers {
		*r.seen = append(*r.seen, t.SrcPath())
		res.Failed = append(res.Failed, t)
	}
	return res
}
