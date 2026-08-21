// Package engine implements pegasus-transfer's grouping/dispatch/retry loop,
// mirroring transfer.py's main(): entries are grouped by protocol pair,
// handed to handlers in parallel worker goroutines (one "attempt" at a
// time), and failures are retried with backoff and eventually
// single-file/single-worker degradation — exactly matching the frozen
// retry-semantics half of the input/behavior contract.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/handler"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// Config controls a Run.
type Config struct {
	MaxAttempts int // default 3, matching the CLI's -m/--max-attempts
	NumThreads  int // default 8, matching PEGASUS_TRANSFER_THREADS/-n
	Registry    *handler.Registry
	Log         *slog.Logger
}

// Run executes every entry to completion or exhaustion of MaxAttempts,
// matching transfer.py's main() retry loop (transfer.py:5201-5330). It
// returns true if everything succeeded, matching the Python function's bool
// return ("All transfers completed successfully." vs "Some transfers
// failed!").
func Run(ctx context.Context, entries []model.Entry, cfg Config) bool {
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	numThreads := cfg.NumThreads
	if numThreads <= 0 {
		numThreads = 8
	}

	ready := append([]model.Entry(nil), entries...)
	sortEntries(ready)

	total := len(ready)
	log.Info(fmt.Sprintf("%d transfers loaded", total))

	approxPerGroup := 100.0
	if numThreads > 0 && total > 0 {
		v := float64(total) / float64(numThreads)
		if v < approxPerGroup {
			approxPerGroup = v
		}
	}

	var failed []model.Entry
	attempt := 0
	tooManyFailures := false

	for {
		attempt++
		log.Info(fmt.Sprintf("Starting transfers - attempt %d", attempt))

		for len(ready) > 0 {
			groups := groupEntries(ready, int(approxPerGroup))
			ready = nil

			threadsToStart := numThreads
			if threadsToStart > len(groups) {
				threadsToStart = len(groups)
			}
			if attempt > 2 {
				threadsToStart = 1
			}
			log.Debug(fmt.Sprintf("Using %d threads for this set of transfers", threadsToStart))

			var wg sync.WaitGroup
			groupCh := make(chan []model.Entry, len(groups))
			for _, g := range groups {
				groupCh <- g
			}
			close(groupCh)

			var mu sync.Mutex
			var roundFailed []model.Entry
			shortCircuited := false

			for i := 0; i < threadsToStart; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for g := range groupCh {
						mu.Lock()
						sc := shortCircuited
						mu.Unlock()
						if sc {
							// A sibling worker saw excessive failures;
							// matches transfer.py leaving remaining queued
							// SimilarWorkSets untouched so the outer loop
							// detects "too many failures" and aborts early.
							mu.Lock()
							roundFailed = append(roundFailed, g...)
							mu.Unlock()
							continue
						}
						succeeded, gFailed := dispatch(ctx, cfg.Registry, g, log)
						_ = succeeded
						if len(gFailed) > 0 {
							mu.Lock()
							roundFailed = append(roundFailed, gFailed...)
							if excessiveFailures(len(gFailed), len(g)) {
								shortCircuited = true
							}
							mu.Unlock()
						}
					}
				}()
			}
			wg.Wait()

			// Rotate sub-transfers and requeue within this same attempt,
			// matching transfer.py's failed_q reprocessing.
			var stillFailed []model.Entry
			for _, e := range roundFailed {
				e.MoveToNextSubTransfer()
				if e.SubTransferIndex() > 0 {
					ready = append(ready, e)
				} else {
					stillFailed = append(stillFailed, e)
				}
			}
			failed = stillFailed

			if len(ready) > 0 && len(failed) > 0 {
				log.Error("Too many failures to continue trying - exiting early")
				tooManyFailures = true
				break
			}
		}

		log.Debug(fmt.Sprintf("%d items in failed queue", len(failed)))

		if attempt == maxAttempts || len(failed) == 0 || tooManyFailures {
			break
		}

		delay := time.Duration(min64(intPow(5, attempt+2)+rand.Intn(20)+1, 300)) * time.Second
		log.Debug(fmt.Sprintf("Sleeping for %s before the next attempt", delay))
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return false
		}

		for _, e := range failed {
			if attempt >= 2 {
				if t, ok := e.(*model.Transfer); ok {
					t.AllowGrouping = false
				}
			}
			ready = append(ready, e)
		}
		failed = nil
	}

	if len(failed) > 0 {
		log.Error("Some transfers failed! See above, and possibly stderr.")
		return false
	}
	log.Info("All transfers completed successfully.")
	return true
}

func intPow(base, exp int) int {
	r := 1
	for i := 0; i < exp; i++ {
		r *= base
	}
	return r
}

func min64(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// excessiveFailures mirrors SimilarWorkSet.excessive_failures's role: if an
// entire group failed, treat it as a signal to short-circuit remaining
// queued work this round rather than keep burning through it.
func excessiveFailures(failedCount, groupSize int) bool {
	return groupSize > 0 && failedCount == groupSize
}
