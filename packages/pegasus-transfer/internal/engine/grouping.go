package engine

import (
	"context"
	"log/slog"
	"sort"

	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/handler"
	"github.com/pegasus-isi/pegasus/packages/pegasus-transfer/internal/model"
)

// sortEntries mirrors transfer.py's `inputs_l.sort()`: entries are ordered
// by (src proto, dst proto, src host, dst host, src path, dst path) for
// Transfers, and by (proto, host, path) for Removes, so protocol-compatible
// entries land next to each other before grouping. In practice a single
// pegasus-transfer input file is homogeneous (all transfers, or all mkdirs,
// or all removes — see StageIn/RemoveDirectory/Cleanup in the planner),
// which sidesteps the cross-type comparisons transfer.py's Mkdir class
// doesn't even define a __lt__ for; mixed-type input here is left in its
// original relative order rather than guessing an ordering Python itself
// couldn't produce.
func sortEntries(entries []model.Entry) {
	sort.SliceStable(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		switch av := a.(type) {
		case *model.Transfer:
			if bv, ok := b.(*model.Transfer); ok {
				return model.LessTransfer(av, bv)
			}
		case *model.Remove:
			if bv, ok := b.(*model.Remove); ok {
				return model.LessRemove(av, bv)
			}
		}
		return false
	})
}

// protoPair returns the (src, dst) protocol pair used for handler dispatch,
// matching TransferHandlerBase.protocol_check's src=="" convention for
// mkdir/remove.
func protoPair(e model.Entry) (src, dst string) {
	switch v := e.(type) {
	case *model.Transfer:
		return v.SrcProto(), v.DstProto()
	case *model.Mkdir:
		return "", v.Proto()
	case *model.Remove:
		return "", v.Proto()
	}
	return "", ""
}

// groupable mirrors transfer.py's transfers_groupable(): same concrete
// type, never Mkdir, matching proto for Remove, and matching
// groupable()+src/dst proto for Transfer.
func groupable(a, b model.Entry) bool {
	switch av := a.(type) {
	case *model.Mkdir:
		return false
	case *model.Remove:
		bv, ok := b.(*model.Remove)
		return ok && av.Proto() == bv.Proto()
	case *model.Transfer:
		bv, ok := b.(*model.Transfer)
		if !ok || !av.Groupable() || !bv.Groupable() {
			return false
		}
		return av.SrcProto() == bv.SrcProto() && av.DstProto() == bv.DstProto()
	}
	return false
}

// groupEntries chunks a (sorted) entry list into same-protocol runs capped
// at maxPerGroup, approximating transfer.py's SimilarWorkSet packing (see
// sortEntries' doc comment for why an exact port of that queue-shuffling
// algorithm isn't needed: group size here only affects retry granularity
// and how many entries one handler call receives, not per-item outcomes,
// since every handler in this rewrite processes entries one at a time).
func groupEntries(entries []model.Entry, maxPerGroup int) [][]model.Entry {
	if maxPerGroup < 1 {
		maxPerGroup = 1
	}
	var groups [][]model.Entry
	var current []model.Entry
	for _, e := range entries {
		if len(current) > 0 && (len(current) >= maxPerGroup || !groupable(current[0], e)) {
			groups = append(groups, current)
			current = nil
		}
		current = append(current, e)
	}
	if len(current) > 0 {
		groups = append(groups, current)
	}
	return groups
}

// dispatch finds the handler for a group's protocol pair and runs the
// appropriate Do* method, matching main()'s per-SimilarWorkSet handling.
func dispatch(ctx context.Context, registry *handler.Registry, group []model.Entry, log *slog.Logger) (succeeded, failed []model.Entry) {
	if len(group) == 0 {
		return nil, nil
	}
	src, dst := protoPair(group[0])
	h := registry.Find(src, dst)
	if h == nil {
		log.Error("no handler found for protocol pair", "src", src, "dst", dst)
		return nil, group
	}

	switch group[0].(type) {
	case *model.Transfer:
		transfers := make([]*model.Transfer, len(group))
		for i, e := range group {
			transfers[i] = e.(*model.Transfer)
		}
		res := h.DoTransfers(ctx, transfers)
		return res.Succeeded, res.Failed
	case *model.Mkdir:
		mkdirs := make([]*model.Mkdir, len(group))
		for i, e := range group {
			mkdirs[i] = e.(*model.Mkdir)
		}
		res := h.DoMkdirs(ctx, mkdirs)
		return res.Succeeded, res.Failed
	case *model.Remove:
		removes := make([]*model.Remove, len(group))
		for i, e := range group {
			removes[i] = e.(*model.Remove)
		}
		res := h.DoRemoves(ctx, removes)
		return res.Succeeded, res.Failed
	}
	return nil, group
}
