// Package handler implements the per-protocol transfer handlers dispatched
// by the engine, mirroring transfer.py's TransferHandlerBase subclasses.
package handler

import (
	"context"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// Result splits a batch into what succeeded and what failed, matching every
// do_transfers/do_mkdirs/do_removes return in transfer.py (a [successful,
// failed] pair).
type Result struct {
	Succeeded []model.Entry
	Failed    []model.Entry
}

// Handler is the interface every protocol implementation satisfies,
// mirroring TransferHandlerBase. A handler need only implement the
// operations it supports; the others may return a Result with everything
// failed (matching the base class's "not implemented" RuntimeError, but as
// a soft failure so one bad request doesn't crash the whole run).
type Handler interface {
	// Name identifies the handler for logging.
	Name() string
	// Accepts reports whether this handler handles the given src->dst
	// protocol pair, matching TransferHandlerBase.protocol_check. For
	// mkdir/remove, src is "".
	Accepts(src, dst string) bool

	DoTransfers(ctx context.Context, transfers []*model.Transfer) Result
	DoMkdirs(ctx context.Context, mkdirs []*model.Mkdir) Result
	DoRemoves(ctx context.Context, removes []*model.Remove) Result
}

// Base provides Accepts() and no-op fallbacks for handlers that only
// implement a subset of operations (e.g. MovetoHandler has no mkdir/remove),
// matching TransferHandlerBase's protocol_check/_protocol_map pattern.
type Base struct {
	HandlerName           string
	ProtocolMap           []string // "src->dst" pairs this handler accepts for transfers
	MkdirCleanupProtocols []string // bare protocols this handler accepts for mkdir/remove
}

func (b Base) Name() string { return b.HandlerName }

func (b Base) Accepts(src, dst string) bool {
	if src == "" && dst != "" {
		for _, p := range b.MkdirCleanupProtocols {
			if p == dst {
				return true
			}
		}
		return false
	}
	item := src + "->" + dst
	for _, p := range b.ProtocolMap {
		if p == item {
			return true
		}
	}
	return false
}

func (b Base) DoTransfers(ctx context.Context, transfers []*model.Transfer) Result {
	failed := make([]model.Entry, len(transfers))
	for i, t := range transfers {
		failed[i] = t
	}
	return Result{Failed: failed}
}

func (b Base) DoMkdirs(ctx context.Context, mkdirs []*model.Mkdir) Result {
	failed := make([]model.Entry, len(mkdirs))
	for i, m := range mkdirs {
		failed[i] = m
	}
	return Result{Failed: failed}
}

func (b Base) DoRemoves(ctx context.Context, removes []*model.Remove) Result {
	failed := make([]model.Entry, len(removes))
	for i, r := range removes {
		failed[i] = r
	}
	return Result{Failed: failed}
}

// Registry holds handlers in registration-order priority, matching
// transfer.py's _available_handlers list: first match wins.
type Registry struct {
	handlers []Handler
}

func NewRegistry(handlers ...Handler) *Registry {
	return &Registry{handlers: handlers}
}

// Find returns the first handler that accepts the given protocol pair, or
// nil if none does.
func (r *Registry) Find(src, dst string) Handler {
	for _, h := range r.handlers {
		if h.Accepts(src, dst) {
			return h
		}
	}
	return nil
}
