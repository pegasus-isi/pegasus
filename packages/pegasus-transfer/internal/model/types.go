package model

import (
	"math/rand"
	"sort"
)

// Entry is anything the engine can queue and hand to a handler: a Transfer,
// Mkdir, or Remove. It mirrors transfer.py's TransferBase interface.
type Entry interface {
	// SubTransferIndex returns how many times MoveToNextSubTransfer has
	// cycled since the entry was last reset (0 initially).
	SubTransferIndex() int
	// MoveToNextSubTransfer rotates to the next source/destination pair,
	// if any. A no-op for entries with a single pair (Mkdir, Remove).
	MoveToNextSubTransfer()
}

// Mkdir represents a single mkdir request (transfer.py's Mkdir).
type Mkdir struct {
	Target *PegasusURL
}

func (m *Mkdir) SubTransferIndex() int  { return 0 }
func (m *Mkdir) MoveToNextSubTransfer() {}
func (m *Mkdir) URL() string            { return m.Target.GetURL() }
func (m *Mkdir) SiteLabel() string      { return m.Target.SiteLabel }
func (m *Mkdir) Proto() string          { return m.Target.Proto }
func (m *Mkdir) Host() string           { return m.Target.Host }
func (m *Mkdir) Path() string           { return m.Target.Path }

// Remove represents a single remove request (transfer.py's Remove).
type Remove struct {
	Target    *PegasusURL
	Recursive bool
}

func (r *Remove) SubTransferIndex() int  { return 0 }
func (r *Remove) MoveToNextSubTransfer() {}
func (r *Remove) URL() string            { return r.Target.GetURL() }
func (r *Remove) SiteLabel() string      { return r.Target.SiteLabel }
func (r *Remove) Proto() string          { return r.Target.Proto }
func (r *Remove) Host() string           { return r.Target.Host }
func (r *Remove) Path() string           { return r.Target.Path }

// Less orders two Removes the way transfer.py's Remove.__lt__ does: by
// proto, then host, then path (Python's chained "or" comparison, which is
// NOT a proper lexicographic tuple compare — see LessRemove for the exact
// semantics preserved here).
func LessRemove(a, b *Remove) bool {
	return a.Target.Proto < b.Target.Proto ||
		a.Target.Host < b.Target.Host ||
		a.Target.Path < b.Target.Path
}

// Transfer represents a single transfer request with one or more candidate
// source/destination URL pairs, mirroring transfer.py's Transfer class.
type Transfer struct {
	LFN                  string
	Linkage              string // "unknown", "input", or "output"
	VerifySymlinkSource  bool
	GenerateChecksum     bool
	VerifyChecksumRemote bool

	SrcURLs []*PegasusURL
	DstURLs []*PegasusURL

	Attempts      int
	AllowGrouping bool

	subTransferIndex int
	subTransferCount int
}

// NewTransfer returns a Transfer with the same defaults as transfer.py's
// Transfer.__init__.
func NewTransfer() *Transfer {
	return &Transfer{
		Linkage:             "unknown",
		VerifySymlinkSource: true,
		AllowGrouping:       true,
	}
}

// AddSrc appends a source URL, matching Transfer.add_src: a nil priority is
// randomized 1-100, and the list is kept sorted by priority descending.
func (t *Transfer) AddSrc(siteLabel, url, fileType string, priority *int) error {
	u, err := newPriorityURL(url, fileType, siteLabel, priority)
	if err != nil {
		return err
	}
	t.SrcURLs = append(t.SrcURLs, u)
	sortByPriorityDesc(t.SrcURLs)
	t.updateSubTransferCount()
	return nil
}

// AddDst appends a destination URL, matching Transfer.add_dst.
func (t *Transfer) AddDst(siteLabel, url, fileType string, priority *int) error {
	u, err := newPriorityURL(url, fileType, siteLabel, priority)
	if err != nil {
		return err
	}
	t.DstURLs = append(t.DstURLs, u)
	sortByPriorityDesc(t.DstURLs)
	t.updateSubTransferCount()
	return nil
}

func newPriorityURL(url, fileType, siteLabel string, priority *int) (*PegasusURL, error) {
	p := 0
	if priority != nil {
		p = *priority
	} else {
		p = rand.Intn(100) + 1 // matches Python's random.randint(1, 100)
	}
	return NewPegasusURL(url, fileType, siteLabel, p)
}

func sortByPriorityDesc(urls []*PegasusURL) {
	sort.SliceStable(urls, func(i, j int) bool {
		return urls[i].Priority > urls[j].Priority
	})
}

func (t *Transfer) updateSubTransferCount() {
	t.subTransferCount = len(t.SrcURLs) * len(t.DstURLs)
}

func (t *Transfer) SrcURL() string        { return t.SrcURLs[0].GetURL() }
func (t *Transfer) SrcURLEncoded() string { return t.SrcURLs[0].GetURLEncoded() }
func (t *Transfer) DstURL() string        { return t.DstURLs[0].GetURL() }
func (t *Transfer) DstURLEncoded() string { return t.DstURLs[0].GetURLEncoded() }
func (t *Transfer) DstURLDirname() string { return t.DstURLs[0].GetURLDirname() }

func (t *Transfer) SrcSiteLabel() string { return t.SrcURLs[0].SiteLabel }
func (t *Transfer) DstSiteLabel() string { return t.DstURLs[0].SiteLabel }
func (t *Transfer) SrcProto() string     { return t.SrcURLs[0].Proto }
func (t *Transfer) DstProto() string     { return t.DstURLs[0].Proto }
func (t *Transfer) SrcType() string      { return t.SrcURLs[0].FileType }
func (t *Transfer) DstType() string      { return t.DstURLs[0].FileType }
func (t *Transfer) SrcHost() string      { return t.SrcURLs[0].Host }
func (t *Transfer) DstHost() string      { return t.DstURLs[0].Host }
func (t *Transfer) SrcPath() string      { return t.SrcURLs[0].Path }
func (t *Transfer) DstPath() string      { return t.DstURLs[0].Path }

func (t *Transfer) SubTransferIndex() int { return t.subTransferIndex }

// MoveToNextSubTransfer cycles the source URL list, matching
// Transfer.move_to_next_sub_transfer: rotate src_urls[0] to the back, and
// wrap subTransferIndex back to 0 once every pair has been tried.
func (t *Transfer) MoveToNextSubTransfer() {
	if len(t.SrcURLs) == 0 {
		return
	}
	first := t.SrcURLs[0]
	t.SrcURLs = append(t.SrcURLs[1:], first)
	t.subTransferIndex++
	if t.subTransferIndex == t.subTransferCount {
		t.subTransferIndex = 0
	}
}

func (t *Transfer) Groupable() bool { return t.AllowGrouping }

// SortKey mirrors Transfer.__lt__'s tuple comparison, used to order the
// initial transfer list (transfer.py sorts src/dst proto, host, path).
type SortKey struct {
	SrcProto, DstProto, SrcHost, DstHost, SrcPath, DstPath string
}

func (t *Transfer) sortKey() SortKey {
	return SortKey{t.SrcProto(), t.DstProto(), t.SrcHost(), t.DstHost(), t.SrcPath(), t.DstPath()}
}

// LessTransfer orders two transfers the way transfer.py's Transfer.__lt__
// does (a real tuple comparison, unlike Remove.__lt__'s chained-or).
func LessTransfer(a, b *Transfer) bool {
	ak, bk := a.sortKey(), b.sortKey()
	if ak.SrcProto != bk.SrcProto {
		return ak.SrcProto < bk.SrcProto
	}
	if ak.DstProto != bk.DstProto {
		return ak.DstProto < bk.DstProto
	}
	if ak.SrcHost != bk.SrcHost {
		return ak.SrcHost < bk.SrcHost
	}
	if ak.DstHost != bk.DstHost {
		return ak.DstHost < bk.DstHost
	}
	if ak.SrcPath != bk.SrcPath {
		return ak.SrcPath < bk.SrcPath
	}
	return ak.DstPath < bk.DstPath
}
