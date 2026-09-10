package integrity

import (
	"encoding/json"
	"fmt"
	"os"
)

// ReadMetaData reads one .meta file's entries, matching read_meta_data():
// files too small to be real JSON (<=2 bytes, e.g. empty or "[]") are
// silently treated as having no entries rather than erroring.
func ReadMetaData(path string) ([]MetaEntry, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("Unable to open metadata file: %w", err)
	}
	if info.Size() <= 2 {
		return nil, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Error parsing the meta data: %w", err)
	}
	var entries []MetaEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("Error parsing the meta data: %w", err)
	}
	return entries, nil
}
