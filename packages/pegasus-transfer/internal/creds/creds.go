package creds

import (
	"fmt"
	"os"
	"path/filepath"
)

// EnsureFSPermissions mirrors transfer.py's check_cred_fs_permissions: if a
// credential file is more open than 0600, it is silently chmod'd back down
// (with the caller expected to log a warning) rather than rejected. Used for
// per-protocol credential files (X509 proxies, SSH keys, S3 config).
func EnsureFSPermissions(path string) (fixed bool, err error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("credential file %s does not exist", path)
	}
	mode := info.Mode().Perm()
	if mode != 0o600 && mode&0o077 != 0 {
		if err := os.Chmod(path, 0o600); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// checkStrictPermissions mirrors the stricter, fail-rather-than-fix checks
// used by load_credentials() and s3.py's get_config(): group/other access of
// any kind is a hard error, not an auto-repair.
func checkStrictPermissions(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("permissions of file %s are too liberal", path)
	}
	return nil
}

// SiteEnv resolves an env var using pegasus-transfer's per-site override
// convention: "<name>_<siteLabel>" takes precedence over the bare "<name>".
func SiteEnv(name, siteLabel string) (string, bool) {
	if v, ok := os.LookupEnv(name + "_" + siteLabel); ok {
		return v, true
	}
	return os.LookupEnv(name)
}

// LoadCredentials mirrors transfer.py's load_credentials(): if
// PEGASUS_CREDENTIALS is set, the file must exist and must not be
// group/other accessible, or this returns an error. If the env var is
// unset, it returns (nil, nil) — no credentials configured, matching
// Python's implicit None return.
func LoadCredentials() (*INI, error) {
	path, ok := os.LookupEnv("PEGASUS_CREDENTIALS")
	if !ok {
		return nil, nil
	}
	if info, err := os.Stat(path); err != nil || info.IsDir() {
		return nil, fmt.Errorf("credentials file does not exist: %s", path)
	}
	if err := checkStrictPermissions(path); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("unable to load credentials: %w", err)
	}
	return ParseINI(data)
}

// S3CredEnv mirrors S3Handler._s3_cred_env: resolves the PEGASUS_CREDENTIALS
// path to hand to the S3 client for a given destination/source site,
// preferring PEGASUS_CREDENTIALS_<site> over the bare PEGASUS_CREDENTIALS,
// and enforcing the (auto-fixing) permission check on whichever file wins.
func S3CredEnvPath(siteLabel string) (string, error) {
	path, ok := SiteEnv("PEGASUS_CREDENTIALS", siteLabel)
	if !ok || path == "" {
		return "", fmt.Errorf(
			"at least one of the PEGASUS_CREDENTIALS_%s or PEGASUS_CREDENTIALS environment variables has to be set",
			siteLabel)
	}
	if _, err := EnsureFSPermissions(path); err != nil {
		return "", err
	}
	return path, nil
}

// DefaultCredentialPath and OldDefaultCredentialPath mirror s3.py's
// DEFAULT_CREDENTIAL_PATH / OLD_DEFAULT_CREDENTIAL_PATH fallbacks.
const (
	DefaultCredentialPath    = "~/.pegasus/credentials.conf"
	OldDefaultCredentialPath = "~/.pegasus/s3cfg"
)

// LoadS3Config mirrors s3.py's get_config(): resolution order is an
// explicit path (the CLI --conf flag, pass "" if unset) → PEGASUS_CREDENTIALS
// → S3CFG → ~/.pegasus/credentials.conf → ~/.pegasus/s3cfg. The winning file
// must not be group/other accessible.
func LoadS3Config(explicitPath string) (*INI, string, error) {
	path := explicitPath
	if path == "" {
		if v, ok := os.LookupEnv("PEGASUS_CREDENTIALS"); ok {
			path = v
		} else if v, ok := os.LookupEnv("S3CFG"); ok {
			path = v
		} else {
			home, _ := os.UserHomeDir()
			newDefault := filepath.Join(home, ".pegasus", "credentials.conf")
			if _, err := os.Stat(newDefault); err == nil {
				path = newDefault
			} else {
				path = filepath.Join(home, ".pegasus", "s3cfg")
			}
		}
	}
	if _, err := os.Stat(path); err != nil {
		return nil, "", fmt.Errorf("config file not found")
	}
	if err := checkStrictPermissions(path); err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	ini, err := ParseINI(data)
	if err != nil {
		return nil, "", err
	}
	// s3.py's DEFAULT_CONFIG.
	if _, ok := ini.defaults["batch_delete"]; !ok {
		ini.defaults["batch_delete"] = "True"
	}
	if _, ok := ini.defaults["batch_delete_size"]; !ok {
		ini.defaults["batch_delete_size"] = "1000"
	}
	return ini, path, nil
}
