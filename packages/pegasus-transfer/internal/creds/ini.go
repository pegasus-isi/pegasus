// Package creds loads pegasus-transfer's credential and S3 config files,
// preserving the exact resolution order, permission checks, and INI dialect
// used by transfer.py's load_credentials() and s3.py's get_config().
package creds

import (
	"bufio"
	"fmt"
	"strings"
)

// INI is a minimal Python configparser-compatible reader: sections, a
// DEFAULT section whose keys are inherited by every other section,
// "key = value" / "key: value" lines, and '#'/';' full-line or
// trailing comments. Option names are case-folded to lowercase, matching
// configparser's default optionxform.
type INI struct {
	sections map[string]map[string]string
	defaults map[string]string
}

// ParseINI parses INI-formatted data.
func ParseINI(data []byte) (*INI, error) {
	f := &INI{sections: map[string]map[string]string{}, defaults: map[string]string{}}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	current := ""
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, ";") {
			continue
		}
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			name := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
			if strings.EqualFold(name, "DEFAULT") {
				current = ""
			} else {
				current = name
				if _, ok := f.sections[current]; !ok {
					f.sections[current] = map[string]string{}
				}
			}
			continue
		}
		key, value, ok := splitKV(trimmed)
		if !ok {
			return nil, fmt.Errorf("ini: line %d: cannot parse %q", lineNo, line)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		value = strings.TrimSpace(value)
		if current == "" {
			f.defaults[key] = value
		} else {
			f.sections[current][key] = value
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return f, nil
}

func splitKV(line string) (key, value string, ok bool) {
	if i := strings.Index(line, "="); i >= 0 {
		return line[:i], line[i+1:], true
	}
	if i := strings.Index(line, ":"); i >= 0 {
		return line[:i], line[i+1:], true
	}
	return "", "", false
}

// HasSection reports whether the given section exists (DEFAULT-only keys do
// not count, matching configparser.has_section semantics).
func (f *INI) HasSection(section string) bool {
	_, ok := f.sections[section]
	return ok
}

// Get returns a key's value, falling back to the DEFAULT section, matching
// configparser.get(section, key).
func (f *INI) Get(section, key string) (string, bool) {
	key = strings.ToLower(key)
	if s, ok := f.sections[section]; ok {
		if v, ok := s[key]; ok {
			return v, true
		}
	}
	if v, ok := f.defaults[key]; ok {
		return v, true
	}
	return "", false
}

// GetDefault returns a key's value or def if absent.
func (f *INI) GetDefault(section, key, def string) string {
	if v, ok := f.Get(section, key); ok {
		return v
	}
	return def
}

// Items returns all key/value pairs visible in a section (its own keys plus
// inherited DEFAULT keys), matching configparser.items(section).
func (f *INI) Items(section string) map[string]string {
	out := map[string]string{}
	for k, v := range f.defaults {
		out[k] = v
	}
	if s, ok := f.sections[section]; ok {
		for k, v := range s {
			out[k] = v
		}
	}
	return out
}

// Set writes a key into a section (creating it if needed), matching
// configparser.set(). Used to patch a single value before Dump()ing a
// config back out (e.g. GSHandler rewriting gs_service_key_file).
func (f *INI) Set(section, key, value string) {
	if _, ok := f.sections[section]; !ok {
		f.sections[section] = map[string]string{}
	}
	f.sections[section][strings.ToLower(key)] = value
}

// Dump serializes the config back to INI text, matching configparser.write().
// Section/key order is not preserved (Go maps have none), which is fine for
// configs consumed by other configparser-compatible readers.
func (f *INI) Dump() string {
	var b strings.Builder
	if len(f.defaults) > 0 {
		b.WriteString("[DEFAULT]\n")
		for k, v := range f.defaults {
			fmt.Fprintf(&b, "%s = %s\n", k, v)
		}
		b.WriteString("\n")
	}
	for section, kv := range f.sections {
		fmt.Fprintf(&b, "[%s]\n", section)
		for k, v := range kv {
			fmt.Fprintf(&b, "%s = %s\n", k, v)
		}
		b.WriteString("\n")
	}
	return b.String()
}
