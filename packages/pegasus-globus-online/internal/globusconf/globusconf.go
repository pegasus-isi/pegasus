// Package globusconf reads and writes ~/.pegasus/globus.conf: the [oauth]
// section INI file pegasus-globus-online-init.py writes and
// pegasus-globus-online.py (GlobusOnlineHandler._creds, ported into
// packages/pegasus-transfer/internal/handler/globusonline.go) reads.
//
// This is a standalone INI reader, duplicating (in miniature)
// packages/pegasus-transfer/internal/creds's ParseINI rather than importing
// it — per the project's decision record, each worker tool is its own
// fully separate Go module. Only what globus.conf actually needs is
// implemented: a single unnamed [oauth] section, "key = value" lines,
// '#'/';' comments — no DEFAULT-section inheritance, since globus.conf never
// uses one.
package globusconf

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Config is the [oauth] section of globus.conf.
type Config struct {
	ClientID      string
	TransferAT    string // access token
	TransferRT    string // refresh token (empty if not requested with --permanent)
	TransferATExp int64  // access token expiration, unix seconds
}

// Path returns ~/.pegasus/globus.conf, matching both tools' hardcoded
// config_file / GlobusOnlineHandler._creds path.
func Path() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".pegasus", "globus.conf"), nil
}

// Load reads and parses globus.conf's [oauth] section.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("unable to locate globus config file %s: %w", path, err)
	}

	values := map[string]string{}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	inOAuth := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			inOAuth = strings.EqualFold(strings.TrimSpace(line[1:len(line)-1]), "oauth")
			continue
		}
		if !inOAuth {
			continue
		}
		key, val, ok := splitKV(line)
		if !ok {
			continue
		}
		values[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(val)
	}
	if err := scanner.Err(); err != nil {
		return Config{}, err
	}

	cfg := Config{
		ClientID:   values["client_id"],
		TransferAT: values["transfer_at"],
		TransferRT: values["transfer_rt"],
	}
	if cfg.ClientID == "" {
		return Config{}, fmt.Errorf("no client_id was supplied for Globus App")
	}
	if exp, ok := values["transfer_at_exp"]; ok {
		cfg.TransferATExp, _ = strconv.ParseInt(exp, 10, 64)
	}
	return cfg, nil
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

// Save writes globus.conf, matching pegasus-globus-online-init.py's
// ConfigParser.write() output shape (an [oauth] section, "key = value"
// lines). Creates the parent directory (~/.pegasus) if needed.
func Save(path string, cfg Config) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString("[oauth]\n")
	fmt.Fprintf(&b, "client_id = %s\n", cfg.ClientID)
	fmt.Fprintf(&b, "transfer_at = %s\n", cfg.TransferAT)
	fmt.Fprintf(&b, "transfer_rt = %s\n", cfg.TransferRT)
	fmt.Fprintf(&b, "transfer_at_exp = %d\n", cfg.TransferATExp)
	return os.WriteFile(path, []byte(b.String()), 0o600)
}
