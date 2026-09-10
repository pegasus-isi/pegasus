package engine

import (
	"context"
	"fmt"
	"log/slog"
	"os"
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
// appropriate Do* method, matching main()'s per-SimilarWorkSet handling. When
// no single handler bridges an incompatible src/dst pair directly, it falls
// back to a two-stage transfer via a local temp file (see dispatchTwoStage),
// matching transfer.py's TransferHandlerHandle split-transfer fallback.
func dispatch(ctx context.Context, registry *handler.Registry, group []model.Entry, log *slog.Logger) (succeeded, failed []model.Entry) {
	if len(group) == 0 {
		return nil, nil
	}
	src, dst := protoPair(group[0])
	h := registry.Find(src, dst)
	if h == nil {
		if _, ok := group[0].(*model.Transfer); ok {
			if down, up := findTwoStageHandlers(registry, src, dst); down != nil && up != nil {
				log.Debug("no direct handler for protocol pair, splitting via local temp file",
					"src", src, "dst", dst, "downHandler", down.Name(), "upHandler", up.Name())
				return dispatchTwoStage(ctx, down, up, group, log)
			}
		}
		log.Error("no handler found for protocol pair", "src", src, "dst", dst)
		return nil, group
	}

	switch group[0].(type) {
	case *model.Transfer:
		transfers := make([]*model.Transfer, len(group))
		for i, e := range group {
			t := e.(*model.Transfer)
			transfers[i] = t
			log.Info("transferring", "src", t.SrcURL(), "dst", t.DstURL())
		}
		res := h.DoTransfers(ctx, transfers)
		return res.Succeeded, res.Failed
	case *model.Mkdir:
		mkdirs := make([]*model.Mkdir, len(group))
		for i, e := range group {
			m := e.(*model.Mkdir)
			mkdirs[i] = m
			log.Info("creating directory", "target", m.URL())
		}
		res := h.DoMkdirs(ctx, mkdirs)
		return res.Succeeded, res.Failed
	case *model.Remove:
		removes := make([]*model.Remove, len(group))
		for i, e := range group {
			r := e.(*model.Remove)
			removes[i] = r
			log.Info("removing", "target", r.URL())
		}
		res := h.DoRemoves(ctx, removes)
		return res.Succeeded, res.Failed
	}
	return nil, group
}

// findTwoStageHandlers looks for a pair of handlers that can bridge an
// incompatible src->dst pair via a local temporary file: one that accepts
// src->file (the "down" leg) and one that accepts file->dst (the "up" leg),
// matching transfer.py's TransferHandlerHandle.__init__ split-transfer
// fallback (the search it falls into once no single handler answers
// protocol_check(src_proto, dst_proto) directly). "symlink" as a destination
// is looked up as "file" instead, matching transfer.py's dst_proto override
// right before that secondary-handler search -- the actual transfer built in
// dispatchTwoStage still targets the real symlink:// destination.
func findTwoStageHandlers(registry *handler.Registry, srcProto, dstProto string) (down, up handler.Handler) {
	lookupDst := dstProto
	if lookupDst == "symlink" {
		lookupDst = "file"
	}
	down = registry.Find(srcProto, "file")
	up = registry.Find("file", lookupDst)
	if down == nil || up == nil {
		return nil, nil
	}
	return down, up
}

// dispatchTwoStage runs every transfer in group as two legs -- src to a local
// temp file, then that temp file to dst -- matching transfer.py's
// TransferHandlerHandle.do_transfers "self._secondary_handler is not None"
// branch.
//
// SIMPLIFICATION: transfer.py allocates a single temp file per
// TransferHandlerHandle (i.e. per SimilarWorkSet) and reuses it across every
// transfer in the set sequentially -- safe there only because a WorkThread
// processes its set one transfer at a time. This uses one temp file per
// transfer instead: simpler, and no less correct, since two-stage entries are
// already handled one at a time here.
func dispatchTwoStage(ctx context.Context, down, up handler.Handler, group []model.Entry, log *slog.Logger) (succeeded, failed []model.Entry) {
	for _, e := range group {
		t := e.(*model.Transfer)
		log.Info("transferring", "src", t.SrcURL(), "dst", t.DstURL(), "via", "local temp file")
		ok, err := runTwoStageTransfer(ctx, down, up, t)
		if err != nil {
			log.Error("two-stage transfer setup failed", "lfn", t.LFN, "error", err)
		}
		if ok {
			succeeded = append(succeeded, t)
		} else {
			failed = append(failed, t)
		}
	}
	return succeeded, failed
}

// runTwoStageTransfer downloads t's source to a local temp file via down,
// then uploads that temp file to t's destination via up, matching
// transfer.py's t_one/t_two split (transfer.py's
// TransferHandlerHandle.do_transfers, "we have a two stage transfer" branch).
// generate_checksum is carried onto the download leg only, so a successful
// download checksums the now-local copy -- exactly like transfer.py only
// setting t_one.generate_checksum.
func runTwoStageTransfer(ctx context.Context, down, up handler.Handler, t *model.Transfer) (bool, error) {
	tmp, err := os.CreateTemp("", "pegasus-transfer-*.data")
	if err != nil {
		return false, fmt.Errorf("creating temp file for two-stage transfer: %w", err)
	}
	tmpName := tmp.Name()
	tmp.Close()
	defer os.Remove(tmpName)

	downLeg := model.NewTransfer()
	downLeg.LFN = t.LFN
	downLeg.GenerateChecksum = t.GenerateChecksum
	if err := downLeg.AddSrc(t.SrcSiteLabel(), t.SrcURL(), t.SrcType(), nil); err != nil {
		return false, err
	}
	if err := downLeg.AddDst("local", "file://"+tmpName, "", nil); err != nil {
		return false, err
	}

	downRes := down.DoTransfers(ctx, []*model.Transfer{downLeg})
	if len(downRes.Succeeded) != 1 {
		return false, nil
	}

	// Open the permissions up to make sure the upload leg (potentially
	// running under different assumptions about inherited permissions) gets
	// sane access, matching transfer.py's os.chmod(tmp_name, 0o0644) between
	// legs.
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return false, fmt.Errorf("chmod temp file for two-stage transfer: %w", err)
	}

	upLeg := model.NewTransfer()
	upLeg.LFN = t.LFN
	if err := upLeg.AddSrc("local", "file://"+tmpName, "", nil); err != nil {
		return false, err
	}
	if err := upLeg.AddDst(t.DstSiteLabel(), t.DstURL(), t.DstType(), nil); err != nil {
		return false, err
	}

	upRes := up.DoTransfers(ctx, []*model.Transfer{upLeg})
	return len(upRes.Succeeded) == 1, nil
}
