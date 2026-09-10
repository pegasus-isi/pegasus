package model

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ParseJSON decodes the current (JSON array) pegasus-transfer input format
// into a list of Entry values, matching transfer.py's read_json_format /
// json_object_decoder. The v1 line-based format is not supported — the
// planner has emitted JSON exclusively for years, so it is deliberately
// dropped rather than ported.
//
// Extra keys the planner writes but transfer.py never reads (e.g. "id",
// "attributes") are ignored, matching json_object_decoder's silent-drop
// behavior — encoding/json ignores unknown JSON object keys by default.
func ParseJSON(data []byte) ([]Entry, error) {
	var raw []rawEntry
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("error parsing the transfer specification JSON: %w", err)
	}

	entries := make([]Entry, 0, len(raw))
	for _, r := range raw {
		e, err := r.toEntry()
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

type rawURLSpec struct {
	SiteLabel string          `json:"site_label"`
	URL       string          `json:"url"`
	Type      string          `json:"type,omitempty"`
	Priority  json.RawMessage `json:"priority,omitempty"`
}

func (u rawURLSpec) priority() (*int, error) {
	if len(u.Priority) == 0 {
		return nil, nil
	}
	// Mirrors Python's int(surl["priority"]): accept a JSON number or a
	// numeric string.
	var n int
	if err := json.Unmarshal(u.Priority, &n); err == nil {
		return &n, nil
	}
	var s string
	if err := json.Unmarshal(u.Priority, &s); err == nil {
		v, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			return nil, fmt.Errorf("invalid priority %q: %w", s, err)
		}
		return &v, nil
	}
	return nil, fmt.Errorf("invalid priority value: %s", u.Priority)
}

type rawTarget struct {
	SiteLabel string          `json:"site_label"`
	URL       string          `json:"url"`
	Recursive json.RawMessage `json:"recursive,omitempty"`
}

type rawEntry struct {
	Type string `json:"type"`

	// transfer
	LFN                  string          `json:"lfn,omitempty"`
	Linkage              string          `json:"linkage,omitempty"`
	VerifySymlinkSource  json.RawMessage `json:"verify_symlink_source,omitempty"`
	GenerateChecksum     json.RawMessage `json:"generate_checksum,omitempty"`
	VerifyChecksumRemote json.RawMessage `json:"verify_checksum_remote,omitempty"`
	SrcURLs              []rawURLSpec    `json:"src_urls,omitempty"`
	DestURLs             []rawURLSpec    `json:"dest_urls,omitempty"`

	// mkdir / remove
	Target *rawTarget `json:"target,omitempty"`
}

func (r rawEntry) toEntry() (Entry, error) {
	switch r.Type {
	case "transfer":
		return r.toTransfer()
	case "mkdir":
		if r.Target == nil {
			return nil, fmt.Errorf("mkdir entry missing target")
		}
		u, err := NewPegasusURL(r.Target.URL, "", r.Target.SiteLabel, 0)
		if err != nil {
			return nil, err
		}
		return &Mkdir{Target: u}, nil
	case "remove":
		if r.Target == nil {
			return nil, fmt.Errorf("remove entry missing target")
		}
		u, err := NewPegasusURL(r.Target.URL, "", r.Target.SiteLabel, 0)
		if err != nil {
			return nil, err
		}
		recursive := false
		if len(r.Target.Recursive) > 0 {
			recursive = parsePythonBool(r.Target.Recursive, false)
		}
		return &Remove{Target: u, Recursive: recursive}, nil
	default:
		return nil, fmt.Errorf("unknown JSON entry type: %q", r.Type)
	}
}

func (r rawEntry) toTransfer() (*Transfer, error) {
	t := NewTransfer()
	if r.LFN != "" {
		t.LFN = r.LFN
	}
	if r.Linkage != "" {
		t.Linkage = r.Linkage
	}
	if len(r.VerifySymlinkSource) > 0 {
		t.VerifySymlinkSource = parsePythonBool(r.VerifySymlinkSource, true)
	}
	if len(r.GenerateChecksum) > 0 {
		t.GenerateChecksum = parsePythonBool(r.GenerateChecksum, false)
	}
	if len(r.VerifyChecksumRemote) > 0 {
		t.VerifyChecksumRemote = parsePythonBool(r.VerifyChecksumRemote, false)
	}
	for _, s := range r.SrcURLs {
		p, err := s.priority()
		if err != nil {
			return nil, err
		}
		if err := t.AddSrc(s.SiteLabel, s.URL, s.Type, p); err != nil {
			return nil, err
		}
	}
	for _, d := range r.DestURLs {
		p, err := d.priority()
		if err != nil {
			return nil, err
		}
		if err := t.AddDst(d.SiteLabel, d.URL, d.Type, p); err != nil {
			return nil, err
		}
	}
	return t, nil
}

// parsePythonBool interprets a raw JSON value the way transfer.py's
// Remove.set_recursive (and the direct-assignment booleans in
// json_object_decoder) end up being treated: a real JSON bool is used
// as-is; a string is matched case-insensitively against
// yes/true/t/1 (true) or no/false/f/0 (false); anything else falls back to
// the given default rather than erroring, since the frozen input contract
// only ever emits bools or "True"/"False" strings (see RemoveDirectory.java).
func parsePythonBool(raw json.RawMessage, def bool) bool {
	var b bool
	if err := json.Unmarshal(raw, &b); err == nil {
		return b
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		switch strings.ToLower(s) {
		case "yes", "true", "t", "1":
			return true
		case "no", "false", "f", "0":
			return false
		}
	}
	return def
}
