package main

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
)

// plainHandler is a slog.Handler that writes only the log message, plus any
// attached key=value attributes, one line per record -- no timestamp, no
// level. pegasus-transfer's output is meant to read as plain progress/status
// lines (matching transfer.py's bare logger.info/error text), not structured
// log records, which is what slog's built-in handlers always prefix with
// "time=... level=...".
type plainHandler struct {
	mu     *sync.Mutex
	w      io.Writer
	level  slog.Leveler
	attrs  []slog.Attr
	groups []string
}

func newPlainHandler(w io.Writer, level slog.Leveler) *plainHandler {
	return &plainHandler{mu: &sync.Mutex{}, w: w, level: level}
}

func (h *plainHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

func (h *plainHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Message)
	for _, a := range h.attrs {
		writePlainAttr(&b, h.groups, a)
	}
	r.Attrs(func(a slog.Attr) bool {
		writePlainAttr(&b, h.groups, a)
		return true
	})
	b.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := io.WriteString(h.w, b.String())
	return err
}

func (h *plainHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	n := *h
	n.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &n
}

func (h *plainHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	n := *h
	n.groups = append(append([]string{}, h.groups...), name)
	return &n
}

// writePlainAttr appends " key=value" (group-prefixed, quoting the value if
// it needs it) to b, matching the logfmt-ish style slog.TextHandler uses for
// attributes -- just without the time/level attrs that handler always adds.
func writePlainAttr(b *strings.Builder, groups []string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}
	b.WriteByte(' ')
	for _, g := range groups {
		b.WriteString(g)
		b.WriteByte('.')
	}
	b.WriteString(a.Key)
	b.WriteByte('=')
	b.WriteString(formatPlainValue(a.Value))
}

func formatPlainValue(v slog.Value) string {
	s := v.String()
	if s == "" || strings.ContainsAny(s, " \t\"=") {
		return strconv.Quote(s)
	}
	return s
}
